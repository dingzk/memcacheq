//
// Created by zhenkai on 2022/6/5.
//

#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>

#include "pq_manager.h"

#define NITEMS 500

#define QUEUE_NAME "test_q_main"
#define QUEUE_SUB_NAME "test_q_main+a"

#define QUEUE_SUB_B_NAME "test_q_main+b"
#define QUEUE_B_NAME "test_q_b_main"

static item *gen_queue_item() {
    static redisAtomic uint64_t i = 1;
    char data[60] = {0};
    unsigned long oldvalue;
    atomicGetIncr(i, oldvalue, 1);
    snprintf(data, 60, "%s %lu", "hello memcacheq offset ", oldvalue);
    item *it = pqi_alloc(0, strlen(data));
    memcpy(ITEM_data(it), data, strlen(data));

    return it;
}

static void *test_pq_manager_set(void *arg) {
    int i = NITEMS;
    while (i-- > 0) {
        item *it = gen_queue_item();
        int ret = pqm_push_item(QUEUE_NAME, it);
        struct timeval tv;
        gettimeofday(&tv, NULL);
        fprintf(stderr, "push item %s ret %d time %ld pid %ld\n", ITEM_data(it),
                ret, tv.tv_usec, (long)arg);
        //        usleep(10000);
    }
}

static void *test_pq_manager_dump(void *arg) {
    pqm_dump(QUEUE_NAME, 1);
    pqm_dump(QUEUE_NAME, 0);
    pqm_dump(QUEUE_SUB_NAME, 0);
}

struct thr_args {
    pthread_t pid;
    char * q_name;
};

static void *test_pq_manager_get(void *arg) {
    struct thr_args *thr_args = (struct thr_args *) arg;

    int i = NITEMS;
    while (1) {
        item *it = pqm_get_item(thr_args->q_name);
        if (it != NULL) {
            struct timeval tv;
            gettimeofday(&tv, NULL);
            fprintf(stderr, "queue %s get item %s time %ld pid %ld\n", thr_args->q_name, ITEM_data(it), tv.tv_usec, (pthread_t)thr_args->pid);
            // decr refcount for socket send
            pqi_release(it);
        } else {
            fprintf(stderr, "queue %s get item null\n", thr_args->q_name);
        }
        //        usleep(10000);
    }


    return NULL;
}

static void *test_multi_thread_pq_manager_dump(int threads) {
    while (threads-- > 0) {
        pthread_t pid;
        pthread_create(&pid, NULL, test_pq_manager_dump, (void *)pid);
        pthread_detach(pid);
    }
}

void test_multi_thread_pq_manager_set(int threads) {
    while (threads-- > 0) {
        pthread_t pid;
        pthread_create(&pid, NULL, test_pq_manager_set, (void *)pid);
        pthread_detach(pid);
    }
}

void test_multi_thread_pq_manager_get(int threads) {
    struct thr_args *thr_args;

    while (threads-- > 0) {
        thr_args = calloc(1, sizeof(struct thr_args));
        thr_args->q_name = QUEUE_NAME;
        pthread_create(&thr_args->pid, NULL, test_pq_manager_get, (void *)thr_args);
        pthread_detach(thr_args->pid);

        sleep(5);
        thr_args = calloc(1, sizeof(struct thr_args));
        thr_args->q_name = QUEUE_SUB_NAME;
        pthread_create(&thr_args->pid, NULL, test_pq_manager_get, (void *)thr_args);
        pthread_detach(thr_args->pid);

        sleep(5);
        thr_args = calloc(1, sizeof(struct thr_args));
        thr_args->q_name = QUEUE_SUB_B_NAME;
        pthread_create(&thr_args->pid, NULL, test_pq_manager_get, (void *)thr_args);
        pthread_detach(thr_args->pid);

    }
}

static void *test_pq_manager_delete(void *arg) {
    pqm_delete(QUEUE_NAME);
    pqm_delete(QUEUE_SUB_NAME);
    pqm_delete(QUEUE_SUB_B_NAME);
}

void test_multi_thread_pq_manager_delete(int threads) {
    while (threads-- > 0) {
        pthread_t pid;
        pthread_create(&pid, NULL, test_pq_manager_delete, (void *)pid);
        pthread_detach(pid);
    }
}

int main(void) {
    pqm_init(4);

    setvbuf(stdout, NULL, _IONBF, 0);
    setbuf(stdout, NULL);

    setvbuf(stderr, NULL, _IONBF, 0);
    setbuf(stderr, NULL);

    test_multi_thread_pq_manager_get(1);

    sleep(2);
    test_multi_thread_pq_manager_set(2);

//    sleep(2);
//    test_multi_thread_pq_manager_dump(1);

    sleep(2);
    test_multi_thread_pq_manager_delete(2);

    sleep(30);
    test_multi_thread_pq_manager_set(2);



    while (1)
        sleep(1);
}
