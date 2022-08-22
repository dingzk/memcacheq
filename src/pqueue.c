//
// Created by zhenkai on 2022/5/31.
//

#include "pqueue.h"

/** exported globals **/
struct pqueue_cfg pqueue_cfg;
// extern struct stats stats;

// extern void STATS_LOCK();
// extern void STATS_UNLOCK();

static int subq_in_queue(PQ *pq, SUBPQ *spq) {
    if (spq->curr_id == NOT_IN_QUEUE) {
        return 0;
    }
    if (pq->max_id >= pq->min_id && spq->curr_id <= pq->max_id + 1 &&
        spq->curr_id > pq->min_id) {
        return 1;
    }
    // just in case uint64_t overflow
    if (pq->max_id < pq->min_id) {
        if (spq->curr_id >= pq->min_id || spq->curr_id <= pq->max_id + 1) {
            return 1;
        }
    }

    return 0;
}

static item *pq_sub_head_item(PQ *pq, SUBPQ *spq) {
    if (subq_in_queue(pq, spq)) {
        return *spq->head_item;
    } else {
        return STAILQ_FIRST(&pq->i_head);
    }
}

static void item_dump(item *it) {
    char *tmp = (char *)malloc(it->nbytes + 1);
    int8_t refcount;
    get_refcount(it, refcount);
    snprintf(tmp, it->nbytes + 1, "%s", ITEM_data(it));
    printf("dump item offset %lu, refcount %d, %s\n", ITEM_offset(it), refcount,
           tmp);
    free(tmp);
    setbuf(stdout, NULL);
    fflush(stdout);
}

PQ *pq_create(const char *name, size_t name_len) {
    if (name == NULL) {
        Q_DEBUG("Lost queue name\n");
        return NULL;
    }
    PQ *pq = (PQ *)calloc(1, sizeof(PQ));
    char *nm = strndup(name, name_len);
    PQSTATS *pq_stats = (PQSTATS *)calloc(1, sizeof(PQSTATS));
    if (pq == NULL || nm == NULL || pq_stats == NULL) {
        goto FAILED;
    }
    pq->name = nm;
    pq->stats = pq_stats;
    STAILQ_INIT(&pq->i_head);
    STAILQ_INIT(&pq->subq_head);
    pq->h = create_hashtable(16, hash_from_key_fn, keys_equal_fn);
    if (pq->h == NULL) {
        goto FAILED;
    }
    pq->max_id = pq->min_id = NOT_IN_QUEUE;
    pthread_mutex_init(&pq->lock, NULL);
    // create first main sub queue
    if (pq_sub_create(pq, name, name_len) == NULL) {
        Q_DEBUG("create first main sub queue failed\n");
        goto FAILED;
    }
    Q_DEBUG("create queue %s successfully\n", name);

    return pq;

FAILED:
    Q_DEBUG("Failed to allocate queue object\n");
    free(pq);
    free(nm);
    free(pq_stats);

    return NULL;
}

int pq_reuse_init(PQ *pq) {
    if (pq->q_stat == DELETED && STAILQ_EMPTY(&pq->i_head) &&
        STAILQ_EMPTY(&pq->subq_head)) {
        memset(pq->stats, 0, sizeof(PQSTATS));
        pq->h = create_hashtable(16, hash_from_key_fn, keys_equal_fn);
        if (pq->h == NULL) {
            return 0;
        }
        pq->max_id = pq->min_id = NOT_IN_QUEUE;
        // create first main sub queue
        if (pq_sub_create(pq, pq->name, strlen(pq->name)) == NULL) {
            Q_DEBUG("queue for reuse creating first main sub queue failed\n");
            return 0;
        }
        pq->q_stat = COMMON;
        Q_DEBUG("queue %s deleted and reused \n", pq->name);
        return 1;
    }

    return 0;
}

SUBPQ *pq_sub_create(PQ *pq, const char *name, size_t name_len) {
    if (pq == NULL || name == NULL) {
        Q_DEBUG("Lost sub queue name\n");
        return NULL;
    }
    SUBPQ *spq = (SUBPQ *)calloc(1, sizeof(SUBPQ));
    char *nm = strndup(name, name_len);
    SUBPQSTATS *spq_stats = (SUBPQSTATS *)calloc(1, sizeof(SUBPQSTATS));
    if (spq == NULL || nm == NULL || spq_stats == NULL) {
        goto FAILED;
    }
    spq->name = nm;
    spq->stats = spq_stats;
    spq->head_item = NULL;
    spq->curr_id = NOT_IN_QUEUE;
    // hashtable insert and link
    if (!hashtable_insert(pq->h, nm, spq)) {
        Q_DEBUG("Failed to insert into hashtable for sub queue\n");
        goto FAILED;
    }
    STAILQ_INSERT_TAIL(&pq->subq_head, spq, subq_next);

    Q_DEBUG("create sub queue %s successfully\n", name);

    return spq;

FAILED:
    Q_DEBUG("Failed to allocate sub queue %s\n", spq->name);
    free(spq);
    free(nm);
    free(spq_stats);

    return NULL;
}

void pq_dump(PQ *pq) {
    if (pq == NULL) {
        return;
    }
    Q_DEBUG("dump pqueue %s\n", pq->name);
    item *it;
    STAILQ_FOREACH(it, &pq->i_head, i_next) { item_dump(it); }
}

void pq_sub_dump(PQ *pq, SUBPQ *spq) {
    if (pq == NULL || spq == NULL) {
        return;
    }
    Q_DEBUG("dump sub_pqueue pq %s\n", spq->name);
    item *it = pq_sub_head_item(pq, spq);
    for (; it != NULL; it = STAILQ_NEXT(it, i_next)) {
        item_dump(it);
    }
}

void pq_sub_destroy(PQ *pq, SUBPQ *spq) {
    if (pq == NULL || spq == NULL) {
        return;
    }
    // remove hash bucket and unlink for queue
    SUBPQ *found;
    if ((found = hashtable_remove(pq->h, spq->name)) == NULL) {
        return;
    }
    assert(found == spq);
    STAILQ_REMOVE(&pq->subq_head, spq, sub_pqueue, subq_next);
    // free name ? hashtable_remove/hashtable_destroy will free key
    free(spq->stats);
    free(spq);
}

void pq_delete(PQ *pq) {
    if (pq == NULL || pq->q_stat == DELETED) {
        return;
    }
    pq->q_stat = DELETED; // logic delete
    // free sub_pqueue
    while (!STAILQ_EMPTY(&pq->subq_head)) {
        SUBPQ *subq = STAILQ_FIRST(&pq->subq_head);
        pq_sub_destroy(pq, subq);
    }
    hashtable_destroy(pq->h, 0);
    pq->h = NULL;
}

void pq_destroy(PQ *pq) {
    if (pq == NULL) {
        return;
    }
    // free sub_pqueue
    while (!STAILQ_EMPTY(&pq->subq_head)) {
        SUBPQ *subq = STAILQ_FIRST(&pq->subq_head);
        pq_sub_destroy(pq, subq);
    }
    // free items
    item *it;
    STAILQ_FOREACH(it, &pq->i_head, i_next) { pqi_free(it); }
    // free struct
    hashtable_destroy(pq->h, 0);
    free(pq->name);
    free(pq->stats);
    pthread_mutex_destroy(&pq->lock);
    free(pq);
}

void pq_push(PQ *pq, item *it) {
    if (it == NULL) {
        return;
    }
    incr_counter(pq->max_id);
    if (STAILQ_EMPTY(&pq->i_head)) {
        assert(pq->min_id == NOT_IN_QUEUE);
        pq->min_id = pq->max_id;
    }
    ITEM_offset(it) = pq->max_id;
    STAILQ_INSERT_TAIL(&pq->i_head, it, i_next);

    pq_stats_op(pq, it, STATS_ADD);
}

item *pq_pop(PQ *pq, SUBPQ *spq) {
    item *it;
    if (pq == NULL || spq == NULL) {
        return NULL;
    }
    it = pq_sub_head_item(pq, spq);
    if (it == NULL) {
        assert(spq->curr_id > pq->max_id);
        return NULL;
    }
    if (ITEM_offset(it) == pq->min_id) {
        spq->curr_id = pq->min_id;
    }
    // TODO there is two refrence one for it and other for &it->i_next
    // add refcount for socket send
    inc_refcount(it);
    // adjust for next pop
    incr_counter(spq->curr_id);
    spq->head_item = &STAILQ_NEXT(it, i_next);

    return it;
}

static item *pqi_new(size_t ntotal) {
    item *it = (item *)malloc(ntotal); // jemalloc
    if (it == NULL) {
        //        STATS_LOCK();
        //        stats.malloc_fails++;
        //        STATS_UNLOCK();
    } else {
        memset(it, 0, ntotal);
    }

    return it;
}

void pqi_free(item *it) {
    if (it == NULL) {
        return;
    }
    free(it);
}

void pqi_release(item *it) {
    if (it == NULL) {
        return;
    }
    dec_refcount(it);
}

/**
 * Generates the variable-sized part of the header for an object.
 *
 * flags   - key flags
 * nbytes  - Number of bytes to hold value and addition CRLF terminator
 * suffix  - Buffer for the "VALUE" line suffix (flags, size).
 * nsuffix - The length of the suffix is stored here.
 *
 * Returns the total size of the header.
 */
static size_t item_make_header(const int flags, const int nbytes, char *suffix,
                               uint8_t *nsuffix) {
    /* suffix is defined at 40 chars elsewhere.. */
    *nsuffix = (uint8_t)snprintf(suffix, 40, " %d %d\r\n", flags, nbytes - 2);
    return sizeof(item) + *nsuffix + nbytes;
}

/*
 * alloc a item buffer, and init it.
 */
item *pqi_alloc(const int flags, const int nbytes) {
    uint8_t nsuffix;
    item *it;
    char suffix[40];
    size_t ntotal = item_make_header(flags, nbytes, suffix, &nsuffix);

    it = pqi_new(ntotal);
    if (it == NULL) {
        return NULL;
    }

    it->nbytes = nbytes;
    memcpy(ITEM_suffix(it), suffix, (size_t)nsuffix);
    it->nsuffix = nsuffix;
    it->refcount = 0;
    it->slabs_clsid = 0;
    it->time = time(NULL);

    return it;
}

uint64_t pq_sub_min_id_for_free(PQ *pq) {
    SUBPQ *subq;
    uint64_t min_id;
    if (STAILQ_EMPTY(&pq->subq_head)) {
        return NOT_IN_QUEUE;
    }
    subq = STAILQ_FIRST(&pq->subq_head);
    min_id = subq->curr_id;
    while ((subq = STAILQ_NEXT(subq, subq_next)) != NULL) {
        if (subq->curr_id < min_id) {
            min_id = subq->curr_id;
        }
    }
    if (min_id > pq->max_id) {
        min_id = pq->max_id;
    }
    // TODO fixbug : pq_pop refrence
    if (min_id >= START_ON_QUEUE) {
        min_id--;
    }

    return min_id;
}

void pq_stats_op(PQ *pq, item *it, STATS_OP operate) {
    PQSTATS *stats = pq->stats;
    if (operate == STATS_ADD) {
        stats->nitems++;
        stats->nbytes += ITEM_ntotal(it);
        stats->current_age = it->time;
    } else {
        stats->nitems--;
        stats->nbytes -= ITEM_ntotal(it);
    }
}