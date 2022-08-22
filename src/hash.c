/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "hash.h"
#include "murmur3_hash.h"
#include <string.h>

typedef uint32_t (*hash_func)(const void *key, size_t length);

static hash_func hash = MurmurHash3_x86_32;

//static uint32_t XXH3_hash(const void *key, size_t length) {
//    return (uint32_t)XXH3_64bits(key, length);
//}

int hash_init(enum hashfunc_type type) {
    switch (type) {
//    case JENKINS_HASH:
//        hash = jenkins_hash;
//        break;
    case MURMUR3_HASH:
        hash = MurmurHash3_x86_32;
        break;
//    case XXH3_HASH:
//        hash = XXH3_hash;
//        break;
    default:
        return -1;
    }
    return 0;
}

uint32_t hash_from_key_fn(void *k) { return hash(k, strlen(k)); }

int keys_equal_fn(void *key1, void *key2) {
    return !strcmp(&(*(const char *)key1), &(*(const char *)key2));
}
