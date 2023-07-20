#ifndef RHM_WRAPPER_H
#define RHM_WRAPPER_H

#include "rhm.h"

RHM g_robinhood_hashmap;

extern inline int g_rhm_init(uint64_t nslots, uint64_t key_size, uint64_t value_size)
{
  // log_2(nslots) will be used as quotient bits of key_size.
	rhm_malloc(&g_robinhood_hashmap, nslots, key_size, value_size, QF_HASH_NONE, 0);
	return 0;
}

extern inline int g_rhm_insert(uint64_t key, uint64_t val)
{
	rhm_insert(&g_robinhood_hashmap, key, val, QF_NO_LOCK);
	return 0;
}

extern inline int g_rhm_lookup(uint64_t key, uint64_t *val)
{
	return rhm_lookup(&g_robinhood_hashmap, key, val, QF_HASH_NONE);
}

extern inline int g_rhm_remove(uint64_t key)
{
	return rhm_remove(&g_robinhood_hashmap, key, QF_HASH_NONE);
}

extern inline int g_rhm_destroy()
{
	rhm_free(&g_robinhood_hashmap);
	return 0;
}

#endif
