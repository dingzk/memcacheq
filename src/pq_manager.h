//
// Created by zhenkai on 2022/6/17.
//

#ifndef MEMCACHEQ_PQ_MANAGER_H
#define MEMCACHEQ_PQ_MANAGER_H

#include "cache.h"
#include "queue.h"
#include "pqueue.h"
#include "hash.h"
#include "hashtable.h"

#include <jemalloc/jemalloc.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define pq_mutex_lock(x) pthread_mutex_lock(x)
#define pq_mutex_try_lock(x) pthread_mutex_trylock(x)
#define pq_mutex_unlock(x) pthread_mutex_unlock(x)

#define pqm_mutex_wr_lock(x) pthread_rwlock_wrlock(x)
#define pqm_mutex_rd_lock(x) pthread_rwlock_rdlock(x)
#define pqm_mutex_rw_unlock(x) pthread_rwlock_unlock(x)

typedef struct pqueue_manager PQM;

struct pqueue_manager {
    pthread_rwlock_t hash_rw_lock;
    struct hashtable *h;
    STAILQ_HEAD(pq_head, pqueue) q_head; /* the head of pqueue */
};

void pqm_init(int nthreads);

int pqm_push_item(const char *queue_name, item *it);

item *pqm_get_item(const char *queue_name);

int pqm_delete(const char *queue_name);

void pqm_dump(const char *queue_name, int parent);



#endif // MEMCACHEQ_PQ_MANAGER_H
