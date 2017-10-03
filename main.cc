/*
 * =====================================================================================
 *
 *       Filename:  main.cc
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
 * =====================================================================================
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

#include <unordered_set>

#include "gqf.h"

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
		if (!qf_insert(a->cf, a->vals[i], 0, a->freq, true, true)) {
			fprintf(stderr, "Failed insertion for %ld with count %d", a->vals[i],
							a->freq);
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
	QF cfr;
	uint64_t qbits = atoi(argv[1]);
	uint32_t freq = atoi(argv[2]);
	uint32_t tcnt = atoi(argv[3]);
	uint64_t nslots = (1ULL << qbits);
	uint64_t nvals = 950*nslots/1000;
	nvals = nvals/freq;
	uint64_t *vals;
	std::unordered_multiset<uint64_t> multiset;
	std::unordered_set<uint64_t> set;
	
	qf_init(&cfr, nslots, qbits+8, 0, 1, "", 0);

	vals = (uint64_t*)calloc(nvals, sizeof(vals[0]));
	RAND_pseudo_bytes((unsigned char *)vals, sizeof(*vals) * nvals);
	for (uint32_t i = 0; i < nvals; i++) {
		vals[i] = (1 * vals[i]) % cfr.metadata->range;
		for (uint32_t j = 0; j < freq; j++)
			multiset.insert(vals[i]);
	}
	
	insert_args args[32];
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
	
	for (uint32_t i=0; i <= args[tcnt-1].end; i++) {
		set.insert(vals[i]);
		uint64_t cnt = qf_count_key_value(&cfr, vals[i], 0);
		if (cnt != multiset.count(vals[i])) {
			for (uint32_t j = 0; j < args[tcnt-1].end; j++) {
				//fprintf(stderr, "%lx ", vals[j]);
			}
			fprintf(stderr, "\n");
			//qf_dump(&cfr);
			fprintf(stderr, "Failed lookup for %lx at index %d. Expexted: %ld. Returned: %ld\n",
						vals[i], i, multiset.count(vals[i]), cnt);
			abort();
		}
	}

	QFi cfir;
	qf_iterator(&cfr, &cfir, 0);
	do {
		uint64_t key, value, count;
		qfi_get(&cfir, &key, &value, &count);
		if (count != multiset.count(key)) {
			fprintf(stderr, "Failed lookup during iteration for %ld. Expexted: %ld. Returned: %ld\n",
						key, multiset.count(key), count);
			//abort();
		}
	} while(!qfi_next(&cfir));

	if (cfr.metadata->ndistinct_elts > set.size()) {
		fprintf(stdout, "Num of items in the CQF is greater than the set.\n");
	}
	fprintf(stdout, "Total num of distinct items in the CQF %ld\n",
					cfr.metadata->ndistinct_elts);
	fprintf(stdout, "Total num of distinct items in the Set %ld\n",
					set.size());
	fprintf(stdout, "Verified all items: %ld\n", args[tcnt-1].end);

	return 0;
}
