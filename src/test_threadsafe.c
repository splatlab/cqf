/*
 * ============================================================================
 *
 *        Authors:  Prashant Pandey <ppandey@cs.stonybrook.edu>
 *                  Rob Johnson <robj@vmware.com>   
 *
 * ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <pthread.h>
#include <openssl/rand.h>

#include "include/gqf.h"
#include "include/gqf_int.h"
#include "include/gqf_file.h"

typedef struct insert_args {
	QF *cf;
	uint64_t *vals;
	int freq;
	uint64_t start;
	uint64_t end;
} insert_args;

void *insert_bm(void *arg)
{
	insert_args *a = (insert_args *)arg;
	for (uint32_t i = a->start; i <= a->end; i++) {
		int ret = qf_insert(a->cf, a->vals[i], 0, a->freq, QF_WAIT_FOR_LOCK);
		if (ret < 0) {
			fprintf(stderr, "failed insertion for key: %lx %d.\n", a->vals[i],
							a->freq);
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
	return NULL;
}

void multi_threaded_insertion(insert_args args[], int tcnt)
{
	pthread_t threads[tcnt];

	for (int i = 0; i < tcnt; i++) {
		fprintf(stdout, "Thread %d bounds %ld %ld\n", i, args[i].start, args[i].end);
		if (pthread_create(&threads[i], NULL, &insert_bm, &args[i])) {
			fprintf(stderr, "Error creating thread\n");
			exit(0);
		}
	}

	for (int i = 0; i < tcnt; i++) {
		if (pthread_join(threads[i], NULL)) {
			fprintf(stderr, "Error joining thread\n");
			exit(0);
		}
	}
}

int main(int argc, char **argv)
{
	if (argc < 4) {
		fprintf(stderr, "Please specify three arguments: \n \
            1. log of the number of slots in the CQF.\n \
            2. frequency count of keys.\n \
            3. number of threads.\n");
		exit(1);
	}
	QF cfr;
	uint64_t qbits = atoi(argv[1]);
	uint64_t freq = atoi(argv[2]);
	uint32_t tcnt = atoi(argv[3]);
	uint64_t nhashbits = qbits + 8;
	uint64_t nslots = (1ULL << qbits);
	uint64_t nvals = 750*nslots/1000;
	nvals = nvals/freq;

	uint64_t *vals;
	
	/* Initialise the CQF */
	if (!qf_malloc(&cfr, nslots, nhashbits, 0, QF_HASH_INVERTIBLE, 0)) {
		fprintf(stderr, "Can't allocate CQF.\n");
		abort();
	}

	/* Generate random values */
	vals = (uint64_t*)calloc(nvals, sizeof(vals[0]));
	RAND_bytes((unsigned char *)vals, sizeof(*vals) * nvals);
	for (uint32_t i = 0; i < nvals; i++) {
		vals[i] = (1 * vals[i]) % cfr.metadata->range;
	}
	
	insert_args *args = (insert_args*)malloc(tcnt * sizeof(insert_args));
	for (uint32_t i = 0; i < tcnt; i++) {
		args[i].cf = &cfr;
		args[i].vals = vals;
		args[i].freq = freq;
		args[i].start = (nvals/tcnt) * i;
		args[i].end = (nvals/tcnt) * (i + 1) - 1;
	}
	fprintf(stdout, "Total number of items: %ld\n", args[tcnt-1].end);

	multi_threaded_insertion(args, tcnt);
	
	fprintf(stdout, "Inserted all items: %ld\n", args[tcnt-1].end);

	for (uint64_t i = 0; i < args[tcnt-1].end; i++) {
		uint64_t count = qf_count_key_value(&cfr, vals[i], 0, 0);
		if (count < freq) {
			fprintf(stderr, "failed lookup after insertion for %lx %ld.\n", vals[i],
							count);
			abort();
		}
	}

	QFi cfir;
	/* Initialize an iterator */
	qf_iterator_from_position(&cfr, &cfir, 0);
	do {
		uint64_t key, value, count;
		qfi_get_key(&cfir, &key, &value, &count);
		qfi_next(&cfir);
		if (qf_count_key_value(&cfr, key, 0, 0) < freq) {
			fprintf(stderr, "Failed lookup during iteration for: %lx. Returned count: %ld\n",
							key, count);
			abort();
		}
	} while(!qfi_end(&cfir));

	fprintf(stdout, "Total num of distinct items in the CQF %ld\n",
					cfr.metadata->ndistinct_elts);
	fprintf(stdout, "Verified all items: %ld\n", args[tcnt-1].end);

	return 0;
}
