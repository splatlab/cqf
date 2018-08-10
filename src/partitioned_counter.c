/*
 * ============================================================================
 *
 *         Author:  Prashant Pandey (), ppandey@cs.stonybrook.edu
 *   Organization:  Stony Brook University
 *
 * ============================================================================
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sched.h>
#include <sys/sysinfo.h>
#include <linux/unistd.h>
#include <sys/syscall.h>
#include <errno.h>

#include "partitioned_counter.h"

#define min(a,b) ((a) < (b) ? (a) : (b))

int pc_init(pc_t *pc, int64_t *global_counter, uint32_t num_counters,
						int32_t threshold) {
	pc->local_counters = (int64_t *)calloc(num_counters,
																				 sizeof(*pc->local_counters));
	if (pc->local_counters == NULL) {
		perror("Couldn't allocate memory for local counters.");
		return PC_ERROR;
	}
	pc->global_counter = global_counter;
	pc->threshold = threshold;
	int num_cpus = (int)sysconf( _SC_NPROCESSORS_ONLN );
	if (num_cpus < 0) {
		perror( "sysconf" );
		return PC_ERROR;
	}
	pc->num_counters = min(num_cpus, num_counters);

	return 0;
}

void pc_add(pc_t *pc, int64_t count) {
	int cpuid = sched_getcpu();
	uint32_t counter_id = cpuid % pc->num_counters;
	int64_t cur_count = __atomic_add_fetch(&pc->local_counters[counter_id],
																				 count, __ATOMIC_SEQ_CST);
	if (cur_count > pc->threshold || cur_count < -pc->threshold) {
		int64_t new_count = __atomic_exchange_n(&pc->local_counters[counter_id],
																						0, __ATOMIC_SEQ_CST);
		__atomic_fetch_add(pc->global_counter, new_count, __ATOMIC_SEQ_CST);
	}
}

void pc_sync(pc_t *pc) {
	for (uint32_t i = 0; i < pc->num_counters; i++) {
		__atomic_fetch_add(pc->global_counter, pc->local_counters[i],
											 __ATOMIC_SEQ_CST);
		__atomic_exchange_n(&pc->local_counters[i], 0, __ATOMIC_SEQ_CST);
	}
}



