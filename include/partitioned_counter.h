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
	int64_t *local_counters;
	int64_t *global_counter;
	uint32_t num_counters;
	uint32_t threshold;
} partitioned_counter;

typedef struct partitioned_counter pc_t;

int pc_init(pc_t *pc, int64_t *global_counter, uint32_t num_counters,
						uint32_t threshold);

void pc_add(pc_t *pc, int64_t count);

void pc_sync(pc_t *pc);

#ifdef __cplusplus
}
#endif

#endif /* _GQF_INT_H_ */
