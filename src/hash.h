#ifndef HASH_H
#define HASH_H

#include <stdint.h>

enum hashfunc_type { JENKINS_HASH = 0, MURMUR3_HASH, XXH3_HASH };

int hash_init(enum hashfunc_type type);

uint32_t hash_from_key_fn(void *k);

int keys_equal_fn(void *key1, void *key2);

#endif /* HASH_H */
