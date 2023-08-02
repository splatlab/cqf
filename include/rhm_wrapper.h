#ifndef RHM_WRAPPER_H
#define RHM_WRAPPER_H

#include "rhm.h"

RHM g_robinhood_hashmap;

extern inline int g_rhm_init(uint64_t nslots, uint64_t key_size, uint64_t value_size)
{
 	// log_2(nslots) will be used as quotient bits of key_size.
	return rhm_malloc(&g_robinhood_hashmap, nslots, key_size, value_size, QF_HASH_NONE, 0);
}

extern inline int g_rhm_insert(uint64_t key, uint64_t val)
{
	#if DEBUG
	printf("Insert key:%lx %lx\n", key, val);
	#endif
	return rhm_insert(&g_robinhood_hashmap, key, val, QF_NO_LOCK | QF_KEY_IS_HASH);
}

extern inline int g_rhm_lookup(uint64_t key, uint64_t *val)
{
	#if DEBUG
	printf("Lookup key:%lx\n", key);
	#endif
	return rhm_lookup(&g_robinhood_hashmap, key, val, QF_NO_LOCK | QF_KEY_IS_HASH);
}

extern inline int g_rhm_remove(uint64_t key)
{
	#if DEBUG
	printf("Remove key:%lx\n", key);
	#endif
	return rhm_remove(&g_robinhood_hashmap, key, QF_NO_LOCK | QF_KEY_IS_HASH);
}

extern inline int g_rhm_destroy()
{
	return rhm_free(&g_robinhood_hashmap);
}

extern inline int g_rhm_rebuild() {
	// DO NOTHING.
	return 0;
}

#endif
