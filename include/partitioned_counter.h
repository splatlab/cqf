/*
 * ============================================================================
 *
 *         Author:  Prashant Pandey (), ppandey@cs.stonybrook.edu
 *   Organization:  Stony Brook University
 *
 * ============================================================================
 */

#ifndef _PARTITIONED_COUNTER_H_
#define _PARTITIONED_COUNTER_H_

#include <inttypes.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_NUM_COUNTERS 32

typedef struct partitioned_counter {
	uint64_t *local_counters;
} partitioned_counter;

int pc_init(uint64_t *global_counter, uint32_t num_counters, uint32_t
						 thresholds);

int pc_add(int64_t count);

int pc_sync(void);

#ifdef __cplusplus
}
#endif

#endif /* _GQF_INT_H_ */
