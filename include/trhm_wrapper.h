#ifndef TRHM_WRAPPER_H
#define TRHM_WRAPPER_H

#include "trhm.h"

TRHM g_trobinhood_hashmap;

extern inline int g_trhm_init(uint64_t nslots, uint64_t key_size, uint64_t value_size)
{
 	// log_2(nslots) will be used as quotient bits of key_size.
	return trhm_malloc(&g_trobinhood_hashmap, nslots, key_size, value_size, QF_HASH_NONE, 0);
}

extern inline int g_trhm_insert(uint64_t key, uint64_t val)
{
	return trhm_insert(&g_trobinhood_hashmap, key, val, QF_NO_LOCK | QF_KEY_IS_HASH);
}

extern inline int g_trhm_lookup(uint64_t key, uint64_t *val)
{
	return trhm_lookup(&g_trobinhood_hashmap, key, val, QF_NO_LOCK | QF_KEY_IS_HASH);
}

extern inline int g_trhm_remove(uint64_t key)
{
	return trhm_remove(&g_trobinhood_hashmap, key, QF_NO_LOCK | QF_KEY_IS_HASH);
}

extern inline int g_trhm_rebuild()
{
	return trhm_clear_tombstones(&g_trobinhood_hashmap, QF_NO_LOCK);
}

extern inline int g_trhm_destroy()
{
	return trhm_free(&g_trobinhood_hashmap);
}


#endif
