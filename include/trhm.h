#ifndef _TRHM_H_
#define _TRHM_H_

#include "gqf.h"
#include "gqf_int.h"
#include <inttypes.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct quotient_filter tombstone_robinhood_hashmap;
typedef tombstone_robinhood_hashmap TRHM;

uint64_t trhm_init(TRHM *trhm, uint64_t nslots, uint64_t key_bits,
                  uint64_t value_bits, enum qf_hashmode hash, uint32_t seed,
                  void *buffer, uint64_t buffer_len);

bool trhm_malloc(TRHM *trhm, uint64_t nslots, uint64_t key_bits,
                uint64_t value_bits, enum qf_hashmode hash, uint32_t seed);

void trhm_destroy(TRHM *rhm);

bool trhm_free(QF *qf);

int trhm_insert(TRHM *trhm, uint64_t key, uint64_t value, uint8_t flags);

int trhm_remove(TRHM *trhm, uint64_t key, uint8_t flags);

int trhm_lookup(const QF *qf, uint64_t key, uint64_t *value, uint8_t flags);

int trhm_clear_tombstones(QF *qf, uint8_t flags);

#ifdef __cplusplus
}
#endif

#endif /* _TRHM_H_ */
