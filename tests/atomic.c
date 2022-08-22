#include "atomicvar.h"
#include <stdio.h>

redisAtomic int acnt;
int cnt;

void * f(void *thr_data) {
    for (int n = 0; n < 1000000; ++n) {
        ++cnt;
        int a;
        atomicGetIncr(acnt, a, 1);
    }
    return NULL;
}

int main(void) {
    pthread_t thr[10];
    for (int n = 0; n < 10; ++n)
        pthread_create(&thr[n], NULL, f, NULL);
    for (int n = 0; n < 10; ++n)
        pthread_join(thr[n], NULL);

    printf("The atomic counter is %u\n", acnt);
    printf("The non-atomic counter is %u\n", cnt);
}