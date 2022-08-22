//
// Created by zhenkai on 2022/5/31.
//

#ifndef MEMCACHEQ_PQUEUE_H
#define MEMCACHEQ_PQUEUE_H

#include "atomicvar.h"
#include "cache.h"
#include "hash.h"
#include "hashtable.h"
#include "queue.h"

#include <jemalloc/jemalloc.h>
#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

//#define PQ_DEBUG

#ifdef PQ_DEBUG
#define Q_DEBUG(...) \
    do { \
        fprintf(stderr, __VA_ARGS__); \
    } while (0)
#else
#define Q_DEBUG(...)
#endif

/* 队列acl限制 */
struct acl_hosts {
    struct sockaddr_storage addr;
    SLIST_ENTRY(acl_hosts) a_next;
};

struct pqueue_acl {
    SLIST_HEAD(set, acl_hosts) head_set_acl;
    SLIST_HEAD(get, acl_hosts) head_get_acl;
};

/* 队列统计信息 */
typedef struct pqueue_stats PQSTATS;

struct pqueue_stats {
    size_t nitems;
    size_t nbytes;
    time_t current_age;
};

typedef enum { STATS_ADD = 0, STATS_DEL } STATS_OP;

typedef struct sub_pqueue_stats SUBPQSTATS;

struct sub_pqueue_stats {
    size_t nitems;
    size_t nbytes;
    time_t current_age;
};

/* 队列配置 */
struct pqueue_cfg {
    size_t max_in_mem_items;
    size_t max_in_mem_bytes;
    size_t max_item_size;
    size_t max_age;
    size_t max_wal_size;
};

extern struct pqueue_cfg pqueue_cfg;

/* 队列wal日志 */
struct queue_wal {
    char *file;
    FILE *reader;
    FILE *writer;
    FILE *playback;
};

typedef struct _stritem {
    STAILQ_ENTRY(_stritem) i_next; /* the next item */
    uint64_t offset;               /* the msg offset */
    time_t time;                   /* push time */
    int nbytes;                    /* size of data */
    uint8_t nsuffix;               /* length of flags-and-length string */
    redisAtomic int8_t refcount;   /* atomic reference */
    uint8_t slabs_clsid;           /* which slab class we're in */
    void *end[];
    /* then " flags length\r\n" (no terminating null) */
    /* then data with terminating \r\n (no terminating null; it's binary!) */
} item;

/* warning: don't use these macros with a function, as it evals its arg twice */
#define ITEM_suffix(item) ((char*) &((item)->end[0]))
#define ITEM_data(item) ((char*) &((item)->end[0]) + (item)->nsuffix)
#define ITEM_ntotal(item) (sizeof(struct _stritem) + (item)->nsuffix + (item)->nbytes)
#define ITEM_offset(item) ((item)->offset)
#define ITEM_clsid(item) ((item)->slabs_clsid & ~(3<<6))

/* persistent queue definitions */

typedef struct pqueue PQ;

typedef struct sub_pqueue SUBPQ;

struct sub_pqueue {
    char *name;
    SUBPQSTATS *stats;
    item **head_item;
    uint64_t curr_id;
    STAILQ_ENTRY(sub_pqueue) subq_next; /* the next sub pqueue */
};

typedef enum { COMMON = 0, DELETED } Q_STATS;

struct pqueue {
    char *name;
    PQSTATS *stats;
    STAILQ_HEAD(pq_item_head, _stritem) i_head;     /* protected by lock */
    STAILQ_HEAD(sub_pq_head, sub_pqueue) subq_head; /* the head sub pqueue */
    STAILQ_ENTRY(pqueue) q_next; /* next queue protected by gpm.hash_rw_lock */
    struct hashtable *h;
    pthread_mutex_t lock; /* protected for subq_head and i_head */
    uint64_t max_id;
    uint64_t min_id;
    uint64_t f_length; /* fixed length */
    Q_STATS q_stat;    /* status for delete/common */
};

#define NOT_IN_QUEUE 0
#define START_ON_QUEUE 1

#define inc_refcount(item) (atomicIncr((item)->refcount, 1))
#define dec_refcount(item) (atomicDecr((item)->refcount, 1))
#define get_refcount(item, var) atomicGet(((item)->refcount), var)

#define incr_counter(id)                                                       \
    do {                                                                       \
        if (++(id) == NOT_IN_QUEUE) {                                          \
            (id) = START_ON_QUEUE;                                             \
        }                                                                      \
    } while (0)


PQ *pq_create(const char *name, size_t name_len);

int pq_reuse_init(PQ *pq);

SUBPQ *pq_sub_create(PQ *pq, const char *name, size_t name_len);

void pq_set_cfg();

void pq_dump(PQ* pq);

void pq_sub_dump(PQ *pq, SUBPQ *spq);

void pq_destroy(PQ* pq);

void pq_delete(PQ* pq);

void pq_sub_destroy(PQ *pq, SUBPQ *spq);

void pq_push(PQ *pq, item *it);

item *pq_pop(PQ *pq, SUBPQ *spq);

item *pqi_alloc(const int flags, const int nbytes);

void pqi_free(item *it);

void pqi_release(item *it);

uint64_t pq_sub_min_id_for_free(PQ* pq);

void pq_stats_op(PQ *pq, item *it, STATS_OP operate);

#endif //MEMCACHEQ_PQUEUE_H
