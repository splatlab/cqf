/*
 * ============================================================================
 *
 *       Filename:  main.c
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
#include "include/gqf_int.h"
#include "include/gqf_file.h"

int main(int argc, char **argv)
{
	if (argc < 2) {
		fprintf(stderr, "Please specify the log of the number of slots in the CQF.\n");
		exit(1);
	}
	QF qf;
	uint64_t qbits = atoi(argv[1]);
	uint64_t nhashbits = qbits + 8;
	uint64_t nslots = (1ULL << qbits);
	uint64_t nvals = 750*nslots/1000;
	uint64_t key_count = 1000;
	uint64_t *vals;

	/* Initialise the CQF */
	if (!qf_malloc(&qf, nslots, nhashbits, 0, LOCKS_FORBIDDEN, DEFAULT, 0)) {
		fprintf(stderr, "Can't allocate CQF.\n");
		abort();
	}

	qf_set_auto_resize(&qf);

	/* Generate random values */
	vals = (uint64_t*)malloc(nvals*sizeof(vals[0]));
	RAND_bytes((unsigned char *)vals, sizeof(*vals) * nvals);
	for (uint64_t i = 0; i < nvals; i++) {
		vals[i] = (1 * vals[i]) % qf.metadata->range;
	}

	/* Insert keys in the CQF */
	for (uint64_t i = 0; i < nvals; i++) {
		int ret = qf_insert(&qf, vals[i], 0, key_count, NO_LOCK);
		if (ret < 0) {
			fprintf(stderr, "failed insertion for key: %lx %d.\n", vals[i], 50);
			if (ret == -1)
				fprintf(stderr, "CQF is full.\n");
			else if (ret == -2)
				fprintf(stderr, "TRY_ONCE_LOCK failed.\n");
			else if (ret == -3)
				fprintf(stderr, "Runtime lock does not satisfy the init time lock.\n");
			else
				fprintf(stderr, "Does not recognise return value.\n");
			abort();
		}
	}

	/* Lookup inserted keys and counts. */
	for (uint64_t i = 0; i < nvals; i++) {
		uint64_t count = qf_count_key_value(&qf, vals[i], 0);
		if (count < key_count) {
			fprintf(stderr, "failed lookup after insertion for %lx %ld.\n", vals[i],
							count);
			abort();
		}
	}

	/* Write the CQF to disk and read it back. */
	char filename[] = "mycqf.cqf";
	fprintf(stdout, "Serializing the CQF to disk.\n");
	uint64_t total_size = qf_serialize(&qf, filename);
	if (total_size < sizeof(qfmetadata) + qf.metadata->total_size_in_bytes) {
		fprintf(stderr, "CQF serialization failed.\n");
		abort();
	}

	QF file_qf;
	fprintf(stdout, "Reading the CQF from disk.\n");
	if (!qf_usefile(&file_qf, LOCKS_FORBIDDEN, filename)) {
		fprintf(stderr, "Can't initialize the CQF from file: %s.\n", filename);
		abort();
	}
	for (uint64_t i = 0; i < nvals; i++) {
		uint64_t count = qf_count_key_value(&file_qf, vals[i], 0);
		if (count < key_count) {
			fprintf(stderr, "failed lookup in file based CQF for %lx %ld.\n",
							vals[i], count);
			abort();
		}
	}

	fprintf(stdout, "Testing iterator.\n");
	/* Initialize an iterator and validate counts. */
	QFi qfi;
	qf_iterator(&qf, &qfi, 0);
	do {
		uint64_t key, value, count;
		qfi_get(&qfi, &key, &value, &count);
		qfi_next(&qfi);
		if (count < key_count) {
			fprintf(stderr, "Failed lookup during iteration for: %lx. Returned count: %ld\n",
							key, count);
			abort();
		}
	} while(!qfi_end(&qfi));

	/* remove some counts  (or keys) and validate. */
	fprintf(stdout, "Testing remove/delete_key.\n");
	srand(time(NULL));
	for (uint64_t i = 0; i < 100; i++) {
		uint64_t idx = rand()%nvals;
		int ret = qf_delete_key_value(&file_qf, vals[idx], 0, NO_LOCK);
		uint64_t count = qf_count_key_value(&file_qf, vals[idx], 0);
		if (count > 0) {
			if (ret < 0) {
				fprintf(stderr, "failed deletion for %lx %ld ret code: %d.\n",
								vals[idx], count, ret);
				abort();
			}
			uint64_t new_count = qf_count_key_value(&file_qf, vals[idx], 0);
			if (new_count > 0) {
				fprintf(stderr, "delete key failed for %lx %ld new count: %ld.\n",
								vals[idx], count, new_count);
				abort();
			}
		}
	}

	fprintf(stdout, "Validated the CQF.\n");
}

