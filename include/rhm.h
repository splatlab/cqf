#ifndef _RHM_H_
#define _RHM_H_

#include "gqf.h"
#include "gqf_int.h"
#include <inttypes.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct quotient_filter robinhood_hashmap;
typedef robinhood_hashmap RHM;

uint64_t rhm_init(RHM *rhm, uint64_t nslots, uint64_t key_bits,
                  uint64_t value_bits, enum qf_hashmode hash, uint32_t seed,
                  void *buffer, uint64_t buffer_len);

bool rhm_malloc(RHM *rhm, uint64_t nslots, uint64_t key_bits,
                uint64_t value_bits, enum qf_hashmode hash, uint32_t seed);

void rhm_destroy(RHM *rhm);

bool rhm_free(QF *qf);

int rhm_insert(RHM *rhm, uint64_t key, uint64_t value, uint8_t flags);

int rhm_remove(RHM *rhm, uint64_t key, uint8_t flags);

int rhm_lookup(const QF *qf, uint64_t key, uint64_t *value, uint8_t flags);

#ifdef __cplusplus
}
#endif

#endif /* _RHM_H_ */
