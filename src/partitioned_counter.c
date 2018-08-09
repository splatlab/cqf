/*
 * ============================================================================
 *
 *         Author:  Prashant Pandey (), ppandey@cs.stonybrook.edu
 *   Organization:  Stony Brook University
 *
 * ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <sched.h>

#include "partitioned_counter.h"

int pc_init(pc_t *pc, uint64_t *global_counter, uint32_t num_counters,
						uint32_t threshold) {
	if (num_counters > MAX_NUM_COUNTERS)
		return -1;
	pc->local_counters = (uint64_t *)calloc(num_counters,
																					sizeof(pc->local_counters));
	if (pc->local_counters == NULL) {
		perror("Couldn't allocate memory for local counters.");
		exit(EXIT_FAILURE);
	}
	pc->global_counter = global_counter;
	pc->num_counters = num_counters;
	pc->threshold = threshold;

	return 0;
}

void pc_add(pc_t *pc, int64_t count) {
	int cpuid = sched_getcpu();
	uint32_t counter_id = (cpuid * 0x5bd1e995) % pc->num_counters;
	int64_t cur_count = __sync_add_and_fetch(&pc->local_counters[counter_id],
																					 count);
	if (cur_count > threshold) {
		__sync_fetch_and_add(pc->global_counter, cur_count);
		__sync_fetch_and_add(&pc->local_counter[counter_id], -cur_count);
	}
}

int pc_sync(pc_t *pc) {
	return 0;
}



