/*
 * ============================================================================
 *
 *       Filename:  main_release.c
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2017-02-04 03:40:58 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Prashant Pandey (ppandey@cs.stonybrook.edu)
 *                  Rob Johnson (rob@cs.stonybrook.edu)
 *   Organization:  Stony Brook University
 *
 * ============================================================================
 */

#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <openssl/rand.h>

#include "include/gqf.h"

int main(int argc, char **argv)
{
	uint64_t qbits = atoi(argv[1]);
	uint64_t nhashbits = qbits + 8;
	uint64_t nslots = (1ULL << qbits);
	uint64_t nvals = 250*nslots/1000;
	uint64_t *vals;

	/* Initialise the CQF */
	QF *qf = qf_malloc(nslots, nhashbits, 0, LOCKS_FORBIDDEN, NONE, 0);

	/* Generate random values */
	vals = (uint64_t*)malloc(nvals*sizeof(vals[0]));
	RAND_pseudo_bytes((unsigned char *)vals, sizeof(*vals) * nvals);
	for (uint64_t i = 0; i < nvals; i++) {
		vals[i] = (1 * vals[i]) % qf->metadata->range;
	}

	/* Insert vals in the CQF */
	for (uint64_t i = 0; i < nvals; i++) {
		qf_insert(qf, vals[i], 0, 50);
	}
	for (uint64_t i = 0; i < nvals; i++) {
		uint64_t count = qf_count_key_value(qf, vals[i], 0);
		if (count < 50) {
			fprintf(stderr, "failed lookup after insertion for %lx %ld.\n", vals[i],
							count);
			abort();
		}
	}

	QFi qfi;
	/* Initialize an iterator */
	qf_iterator(qf, &qfi, 0);
	do {
		uint64_t key, value, count;
		qfi_get(&qfi, &key, &value, &count);
		if (qf_count_key_value(qf, key, 0) < 50) {
			fprintf(stderr, "Failed lookup from A for: %ld. Returned count: %ld\n",
							key, qf_count_key_value(qf, key, 0));
			abort();
		}
	} while(!qfi_next(&qfi));

	fprintf(stdout, "Validated the CQF.\n");
}

