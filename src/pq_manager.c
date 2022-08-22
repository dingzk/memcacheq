//
// Created by zhenkai on 2022/6/17.
//

#include "pq_manager.h"
#include <unistd.h>
#include <sys/time.h>

#define hashsize(n) ((unsigned long int)1 << (n))
#define hashmask(n) (hashsize(n) - 1)

static volatile int init_count = 0;
static pthread_mutex_t init_lock;
static pthread_cond_t init_cond;

/* free item thread bits */
static pthread_t free_item_tid;
static volatile int do_run_free_item_thread;
static pthread_cond_t free_item_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t free_item_lock = PTHREAD_MUTEX_INITIALIZER;

#define SUB_QUEUE_IDENT '+'

PQM g_pqm;

#define FREE_PER_SLEEP_MS 50
#define FREE_PER_SLICE 10000
#define QUEUE_MIN_LENGTH 1

static void wait_for_thread_registration(int nthreads) {
    while (init_count < nthreads) {
        pthread_cond_wait(&init_cond, &init_lock);
    }
}

static void register_thread_initialized(void) {
    pthread_mutex_lock(&init_lock);
    init_count++;
    pthread_mutex_unlock(&init_lock);
    pthread_cond_signal(&init_cond);
}

static void *free_item_thread(void *arg) {
    item *it, *next_it;
    int8_t refcount;
    uint64_t subq_min_id;
    int free_count = 0;
    static PQ *pq;
    struct timeval now;
    struct timespec to_sleep;
    long nsec;
    unsigned int h_count;
    pq_mutex_lock(&free_item_lock);
    register_thread_initialized();
    while (do_run_free_item_thread) {
        free_count = 0;
        pqm_mutex_rd_lock(&g_pqm.hash_rw_lock);
        h_count = hashtable_count(g_pqm.h);
        if (!pq) {
            pq = STAILQ_FIRST(&g_pqm.q_head);
        } else {
            pq = STAILQ_NEXT(pq, q_next);
        }
        pqm_mutex_rw_unlock(&g_pqm.hash_rw_lock);
        if (h_count == 0) {
            sleep(5);
            goto SLEEP;
        }
        if (pq == NULL) {
            goto SLEEP;
        }
        if (pq_mutex_try_lock(&pq->lock) != 0) {
            Q_DEBUG("try lock queue %s failed, try next queue\n", pq->name);
            continue;
        }
        // delete unreferenced items
        subq_min_id = pq_sub_min_id_for_free(pq);
        while ((it = STAILQ_FIRST(&pq->i_head)) != NULL) {
            get_refcount(it, refcount);
            if (refcount > 0) {
                Q_DEBUG("compaction queue %s, status %d, refcount %d, sleep "
                        "for next processing\n",
                        pq->name, pq->q_stat, refcount);
                break;
            }
            if (pq->q_stat == DELETED || pq->min_id < subq_min_id ||
                (pq->f_length > QUEUE_MIN_LENGTH &&
                 pq->f_length < pq->max_id - pq->min_id + 1)) {
                next_it = STAILQ_NEXT(it, i_next);
                Q_DEBUG("compaction queue %s, free offset %lu\n", pq->name,
                        ITEM_offset(it));
                pq_stats_op(pq, it, STATS_DEL);
                STAILQ_REMOVE_HEAD(&pq->i_head, i_next);
                pqi_free(it);
                if (next_it != NULL) {
                    pq->min_id = ITEM_offset(next_it);
                } else {
                    assert(pq->q_stat == DELETED);
                }
                if (pq->q_stat == DELETED) {
                    continue;
                }
                if (free_count++ >= FREE_PER_SLICE - 1) {
                    break;
                }
            } else {
                break;
            }
        }
        if (free_count > 0) {
            Q_DEBUG("compaction queue %s end, free %d items\n", pq->name,
                    free_count);
        }
        pq_mutex_unlock(&pq->lock);

    SLEEP:
//        usleep(FREE_PER_SLEEP_MS * 1000);
        gettimeofday(&now, NULL);
        nsec = now.tv_usec * 1000 + FREE_PER_SLEEP_MS * 1000000;
        to_sleep.tv_sec = now.tv_sec + nsec / 1000000000;
        to_sleep.tv_nsec = nsec % 1000000000;
        pthread_cond_timedwait(&free_item_cond, &free_item_lock, &to_sleep);
    }
    pq_mutex_unlock(&free_item_lock);
}

static int start_free_item_thread() {
    int ret;
    do_run_free_item_thread = 1;
    if ((ret = pthread_create(&free_item_tid, NULL, free_item_thread, NULL)) !=
        0) {
        Q_DEBUG("Can't create idle connection timeout thread: %s\n",
                strerror(ret));
        return -1;
    }

    return 0;
}

int stop_free_item_thread(void) {
    if (!do_run_free_item_thread)
        return -1;
    pq_mutex_lock(&free_item_lock);
    do_run_free_item_thread = 0;
    pq_mutex_unlock(&free_item_lock);
    pthread_cond_signal(&free_item_cond);
    pthread_join(free_item_tid, NULL);
    return 0;
}

void pqm_init(int nthreads) {
    pthread_rwlock_init(&g_pqm.hash_rw_lock, NULL);
    g_pqm.h = create_hashtable(16, hash_from_key_fn, keys_equal_fn);
    STAILQ_INIT(&g_pqm.q_head);

    pthread_mutex_init(&init_lock, NULL);
    pthread_cond_init(&init_cond, NULL);

    // TODO create healper threads
    start_free_item_thread();
    pthread_mutex_lock(&init_lock);
    wait_for_thread_registration(1);
    pthread_mutex_unlock(&init_lock);
}

static char *queue_sub_suffix(const char *queue_name) {
    char *suffix;
    if ((suffix = memchr(queue_name, SUB_QUEUE_IDENT, strlen(queue_name))) !=
        NULL) {
        return suffix + 1;
    }
    return NULL;
}

static char *get_main_queue_name(const char *queue_name) {
    char *split;
    if (*queue_name == SUB_QUEUE_IDENT) {
        return NULL;
    }
    if ((split = queue_sub_suffix(queue_name)) != NULL) { // sub queue
        return strndup(queue_name, split - queue_name - 1);
    }

    return strdup(queue_name);
}

static PQ *pqm_create(const char *queue_name) {
    pqm_mutex_rw_unlock(&g_pqm.hash_rw_lock);
    pqm_mutex_wr_lock(&g_pqm.hash_rw_lock);
    // double Check
    PQ *pq = (PQ *)hashtable_search(g_pqm.h, (void *)queue_name);
    if (pq != NULL) {
        return pq;
    }
    pq = pq_create(queue_name, strlen(queue_name));
    if (pq == NULL) {
        Q_DEBUG("create pqueue failed\n");
        return NULL;
    }
    // insert into hashtable and queue lists
    if (!hashtable_insert(g_pqm.h, pq->name, pq)) {
        Q_DEBUG("insert hashtable Failed\n");
        pq_destroy(pq);
        return NULL;
    }
    STAILQ_INSERT_TAIL(&g_pqm.q_head, pq, q_next);

    return pq;
}

int pqm_push_item(const char *queue_name, item *it) {
    if (queue_name == NULL || it == NULL) {
        return 0;
    }
    if (queue_sub_suffix(queue_name) != NULL) {
        Q_DEBUG("can't push item to sub queue %s since inviled char %c \n",
                queue_name, SUB_QUEUE_IDENT);
        return 0;
    }
    pqm_mutex_rd_lock(&g_pqm.hash_rw_lock);
    PQ *pq = (PQ *)hashtable_search(g_pqm.h, (void *)queue_name);
    if (pq == NULL && (pq = pqm_create(queue_name)) == NULL) {
        Q_DEBUG("queue %s not exist create Failed\n", queue_name);
        pqm_mutex_rw_unlock(&g_pqm.hash_rw_lock);
        return 0;
    }
    pqm_mutex_rw_unlock(&g_pqm.hash_rw_lock);

    // push item to the parent queue
    // reuse deleted queue
    pq_mutex_lock(&pq->lock);
    if (pq->q_stat == DELETED && pq_reuse_init(pq) == 0) {
        Q_DEBUG("reuse queue %s failed\n", pq->name);
        pq_mutex_unlock(&pq->lock);
        return 0;
    }
    pq_push(pq, it);
    pq_mutex_unlock(&pq->lock);

    return 1;
}

item *pqm_get_item(const char *queue_name) {
    if (queue_name == NULL) {
        return NULL;
    }
    item *it = NULL;
    char *main_queue = get_main_queue_name(queue_name);
    if (main_queue == NULL) {
        Q_DEBUG("you'd better get exist main queue\n");
        return NULL;
    }
    pqm_mutex_rd_lock(&g_pqm.hash_rw_lock);
    PQ *pq = (PQ *)hashtable_search(g_pqm.h, main_queue);
    pqm_mutex_rw_unlock(&g_pqm.hash_rw_lock);
    free(main_queue);
    if (pq == NULL) {
        Q_DEBUG("main queue of %s not exist\n", queue_name);
        return NULL;
    }

    pq_mutex_lock(&pq->lock);
    if (pq->q_stat == DELETED) {
        Q_DEBUG("queue %s is deleted\n", pq->name);
        pq_mutex_unlock(&pq->lock);
        return NULL;
    }
    SUBPQ *spq = (SUBPQ *)hashtable_search(pq->h, (void *)queue_name);
    if (spq == NULL &&
        (spq = pq_sub_create(pq, queue_name, strlen(queue_name))) == NULL) {
        Q_DEBUG("create sub queue %s failed\n", queue_name);
        pq_mutex_unlock(&pq->lock);
        return NULL;
    }
    it = pq_pop(pq, spq);
    pq_mutex_unlock(&pq->lock);

    return it;
}

int pqm_delete(const char *queue_name) {
    if (queue_name == NULL) {
        return 0;
    }
    char *main_queue = get_main_queue_name(queue_name);
    if (main_queue == NULL) {
        Q_DEBUG("illegal queue name\n");
        return 0;
    }
    pqm_mutex_rd_lock(&g_pqm.hash_rw_lock);
    PQ *pq = (PQ *)hashtable_search(g_pqm.h, main_queue);
    pqm_mutex_rw_unlock(&g_pqm.hash_rw_lock);
    int is_main = strcmp(main_queue, queue_name) == 0 ? 1 : 0;
    free(main_queue);
    if (pq == NULL) {
        Q_DEBUG("delete main queue of %s not exist\n", queue_name);
        return 0;
    }

    pq_mutex_lock(&pq->lock);
    if (is_main) {
        pq_delete(pq);
    } else if (pq->h != NULL) {
        SUBPQ *spq = (SUBPQ *)hashtable_search(pq->h, (void *)queue_name);
        pq_sub_destroy(pq, spq);
    }
    pq_mutex_unlock(&pq->lock);

    return 1;
}

void pqm_dump(const char *queue_name, int parent) {
    if (queue_name == NULL) {
        return;
    }
    char *main_queue = get_main_queue_name(queue_name);
    if (main_queue == NULL) {
        Q_DEBUG("illegal queue name\n");
        return;
    }
    pqm_mutex_rd_lock(&g_pqm.hash_rw_lock);
    PQ *pq = (PQ *)hashtable_search(g_pqm.h, main_queue);
    pqm_mutex_rw_unlock(&g_pqm.hash_rw_lock);
    free(main_queue);
    if (pq == NULL) {
        Q_DEBUG("dump queue %s not exist\n", queue_name);
        return;
    }

    pq_mutex_lock(&pq->lock);
    if (parent) {
        pq_dump(pq);
    } else {
        SUBPQ *spq = (SUBPQ *)hashtable_search(pq->h, (void *)queue_name);
        pq_sub_dump(pq, spq);
    }
    pq_mutex_unlock(&pq->lock);
}
