/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *  MemcacheQ - Simple Queue Service over Memcache
 *
 *      http://memcacheq.googlecode.com
 *
 *  The source code of MemcacheQ is most based on MemcachDB:
 *
 *      http://memcachedb.googlecode.com
 *
 *  Copyright 2008 Steve Chu.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Steve Chu <stvchu@gmail.com>
 *
 */
 
#include "memcacheq.h"
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <errno.h>
#include <malloc.h>
#include <string.h>

#ifdef USE_THREADS

#include <pthread.h>

#define ITEMS_PER_ALLOC 64

/* An item in the connection queue. */
enum conn_queue_item_modes {
    queue_new_conn,   /* brand new connection. */
    queue_pause,      /* pause thread */
    queue_timeout,    /* socket sfd timed out */
    queue_redispatch, /* return conn from side thread */
    queue_stop,       /* exit thread */
    queue_return_io,  /* returning a pending IO object immediately */
};

/* An item in the connection queue. */
typedef struct conn_queue_item CQ_ITEM;
struct conn_queue_item {
    int     sfd;
    int     init_state;
    int     event_flags;
    int     read_buffer_size;
    int     is_udp;
    enum conn_queue_item_modes mode;
    CQ_ITEM *next;
};

/* A connection queue. */
typedef struct conn_queue CQ;
struct conn_queue {
    CQ_ITEM *head;
    CQ_ITEM *tail;
    pthread_mutex_t lock;
    pthread_cond_t  cond;
};

/* Lock for connection freelist */
static pthread_mutex_t conn_lock;

/* Lock for global stats */
static pthread_mutex_t stats_lock;

/* Free list of CQ_ITEM structs */
static CQ_ITEM *cqi_freelist;
static pthread_mutex_t cqi_freelist_lock;

static LIBEVENT_THREAD *threads;

/*
 * Number of threads that have finished setting themselves up.
 */
static int init_count = 0;
static pthread_mutex_t init_lock;
static pthread_cond_t init_cond;


static void thread_libevent_process(int fd, short which, void *arg);

/*
 * Initializes a connection queue.
 */
static void cq_init(CQ *cq) {
    pthread_mutex_init(&cq->lock, NULL);
    pthread_cond_init(&cq->cond, NULL);
    cq->head = NULL;
    cq->tail = NULL;
}

/*
 * Waits for work on a connection queue.
 */
static CQ_ITEM *cq_pop(CQ *cq) {
    CQ_ITEM *item;

    pthread_mutex_lock(&cq->lock);
    while (NULL == cq->head)
        pthread_cond_wait(&cq->cond, &cq->lock);
    item = cq->head;
    cq->head = item->next;
    if (NULL == cq->head)
        cq->tail = NULL;
    pthread_mutex_unlock(&cq->lock);

    return item;
}

/*
 * Looks for an item on a connection queue, but doesn't block if there isn't
 * one.
 * Returns the item, or NULL if no item is available
 */
static CQ_ITEM *cq_peek(CQ *cq) {
    CQ_ITEM *item;

    pthread_mutex_lock(&cq->lock);
    item = cq->head;
    if (NULL != item) {
        cq->head = item->next;
        if (NULL == cq->head)
            cq->tail = NULL;
    }
    pthread_mutex_unlock(&cq->lock);

    return item;
}

/*
 * Adds an item to a connection queue.
 */
static void cq_push(CQ *cq, CQ_ITEM *item) {
    item->next = NULL;

    pthread_mutex_lock(&cq->lock);
    if (NULL == cq->tail)
        cq->head = item;
    else
        cq->tail->next = item;
    cq->tail = item;
    pthread_cond_signal(&cq->cond);
    pthread_mutex_unlock(&cq->lock);
}

/*
 * Returns a fresh connection queue item.
 */
static CQ_ITEM *cqi_new() {
    CQ_ITEM *item = NULL;
    pthread_mutex_lock(&cqi_freelist_lock);
    if (cqi_freelist) {
        item = cqi_freelist;
        cqi_freelist = item->next;
    }
    pthread_mutex_unlock(&cqi_freelist_lock);

    if (NULL == item) {
        int i;

        /* Allocate a bunch of items at once to reduce fragmentation */
        item = malloc(sizeof(CQ_ITEM) * ITEMS_PER_ALLOC);
        if (NULL == item)
            return NULL;

        /*
         * Link together all the new items except the first one
         * (which we'll return to the caller) for placement on
         * the freelist.
         */
        for (i = 2; i < ITEMS_PER_ALLOC; i++)
            item[i - 1].next = &item[i];

        pthread_mutex_lock(&cqi_freelist_lock);
        item[ITEMS_PER_ALLOC - 1].next = cqi_freelist;
        cqi_freelist = &item[1];
        pthread_mutex_unlock(&cqi_freelist_lock);
    }

    return item;
}


/*
 * Frees a connection queue item (adds it to the freelist.)
 */
static void cqi_free(CQ_ITEM *item) {
    pthread_mutex_lock(&cqi_freelist_lock);
    item->next = cqi_freelist;
    cqi_freelist = item;
    pthread_mutex_unlock(&cqi_freelist_lock);
}


/*
 * Creates a worker thread.
 */
static void create_worker(void *(*func)(void *), void *arg) {
    pthread_t       thread;
    pthread_attr_t  attr;
    int             ret;

    pthread_attr_init(&attr);

    if ((ret = pthread_create(&thread, &attr, func, arg)) != 0) {
        fprintf(stderr, "Can't create thread: %s\n",
                strerror(ret));
        exit(1);
    }
}


/****************************** LIBEVENT THREADS *****************************/

/*
 * Set up a thread's information.
 */
static void setup_thread(LIBEVENT_THREAD *me) {
    if (! me->base) {
        me->base = event_init();
        if (! me->base) {
            fprintf(stderr, "Can't allocate event base\n");
            exit(1);
        }
    }

    /* Listen for notifications from other threads */
    event_set(&me->notify_event, me->notify_receive_fd,
              EV_READ | EV_PERSIST, thread_libevent_process, me);
    event_base_set(me->base, &me->notify_event);

    if (event_add(&me->notify_event, 0) == -1) {
        fprintf(stderr, "Can't monitor libevent notify pipe\n");
        exit(1);
    }

    me->ev_queue = malloc(sizeof(struct conn_queue));
    if (me->ev_queue == NULL) {
        perror("Failed to allocate memory for connection queue");
        exit(EXIT_FAILURE);
    }
    cq_init(me->ev_queue);
}


/*
 * Worker thread: main event loop
 */
static void *worker_libevent(void *arg) {
    LIBEVENT_THREAD *me = arg;

    /* Any per-thread setup can happen here; thread_init() will block until
     * all threads have finished initializing.
     */

    pthread_mutex_lock(&init_lock);
    init_count++;
    pthread_cond_signal(&init_cond);
    pthread_mutex_unlock(&init_lock);

    event_base_loop(me->base, 0);
    return NULL;
}


/*
 * Processes an incoming "handle a new connection" item. This is called when
 * input arrives on the libevent wakeup pipe.
 */
static void thread_libevent_process(int fd, short which, void *arg) {
    LIBEVENT_THREAD *me = arg;
    CQ_ITEM *item;
    conn *c;
    char buf[1];

    if (read(fd, buf, 1) != 1)
        if (settings.verbose > 0)
            fprintf(stderr, "Can't read from libevent pipe\n");

    item = cq_peek(me->ev_queue);
    if (item == NULL) {
        return;
    }
    switch (item->mode) {
    case queue_new_conn:
        c = conn_new(item->sfd, item->init_state, item->event_flags,
                     item->read_buffer_size, item->is_udp, me->base);
        if (c == NULL) {
            if (item->is_udp) {
                fprintf(stderr, "Can't listen for events on UDP socket\n");
                exit(1);
            } else {
                if (settings.verbose > 0) {
                    fprintf(stderr, "Can't listen for events on fd %d\n",
                            item->sfd);
                }
                close(item->sfd);
            }
        }
        c->thread = me;
        break;
    case queue_redispatch:
        /* a side thread redispatched a client connection */
        conn_worker_readd(conns[item->sfd]);
        break;
    }
    cqi_free(item);
}

/* Which thread we assigned a connection to most recently. */
static int last_thread = -1;

/*
 * Dispatches a new connection to another thread. This is only ever called
 * from the main thread, either during initialization (for UDP) or because
 * of an incoming connection.
 */
void dispatch_conn_new(int sfd, int init_state, int event_flags,
                       int read_buffer_size, int is_udp) {
    CQ_ITEM *item = cqi_new();
    int thread = (last_thread + 1) % settings.num_threads;

    last_thread = thread;

    item->sfd = sfd;
    item->init_state = init_state;
    item->event_flags = event_flags;
    item->read_buffer_size = read_buffer_size;
    item->is_udp = is_udp;
    item->mode = queue_new_conn;

    cq_push(threads[thread].ev_queue, item);
    if (write(threads[thread].notify_send_fd, "", 1) != 1) {
        perror("Writing to thread notify pipe");
    }
}

void sidethread_conn_close(conn *c) {
    if (settings.verbose > 1)
        fprintf(stderr, "<%d connection closing from side thread.\n", c->sfd);

    c->state = conn_closing;
    // redispatch will see closing flag and properly close connection.
    CQ_ITEM *item = cqi_new();

    item->sfd = c->sfd;
    item->is_udp = 0;
    item->mode = queue_redispatch;

    cq_push(c->thread->ev_queue, item);
    if (write(c->thread->notify_send_fd, "", 1) != 1) {
        perror("Writing to thread notify pipe");
    }

    return;
}

/*
 * Returns true if this is the thread that listens for new TCP connections.
 */
int mt_is_listen_thread() {
    return pthread_self() == threads[0].thread_id;
}

/******************************* GLOBAL STATS ******************************/

void mt_stats_lock() {
    pthread_mutex_lock(&stats_lock);
}

void mt_stats_unlock() {
    pthread_mutex_unlock(&stats_lock);
}

/*
 * Initializes the thread subsystem, creating various worker threads.
 *
 * nthreads  Number of event handler threads to spawn
 * main_base Event base for main thread
 */
void thread_init(int nthreads, struct event_base *main_base) {
    int         i;

    /* init pq manager */
    pqm_init(nthreads);

    pthread_mutex_init(&conn_lock, NULL);
    pthread_mutex_init(&stats_lock, NULL);

    pthread_mutex_init(&init_lock, NULL);
    pthread_cond_init(&init_cond, NULL);

    pthread_mutex_init(&cqi_freelist_lock, NULL);
    cqi_freelist = NULL;

    threads = malloc(sizeof(LIBEVENT_THREAD) * nthreads);
    if (! threads) {
        perror("Can't allocate thread descriptors");
        exit(1);
    }

    threads[0].base = main_base;
    threads[0].thread_id = pthread_self();

    for (i = 0; i < nthreads; i++) {
        int fds[2];
        if (pipe(fds)) {
            perror("Can't create notify pipe");
            exit(1);
        }

        threads[i].l = logger_create();

        threads[i].notify_receive_fd = fds[0];
        threads[i].notify_send_fd = fds[1];

        setup_thread(&threads[i]);
    }

    /* Create threads after we've done all the libevent setup. */
    for (i = 1; i < nthreads; i++) {
        create_worker(worker_libevent, &threads[i]);
    }

    /* Wait for all the threads to set themselves up before returning. */
    pthread_mutex_lock(&init_lock);
    init_count++; /* main thread */
    while (init_count < nthreads) {
        pthread_cond_wait(&init_cond, &init_lock);
    }
    pthread_mutex_unlock(&init_lock);
}

#endif
