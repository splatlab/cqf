/*
 * ============================================================================
 *
 *        Authors:  Prashant Pandey <ppandey@cs.stonybrook.edu>
 *                  Rob Johnson <robj@vmware.com>   
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
#include <sys/mman.h>
#include <unistd.h>
#include <openssl/rand.h>

#include "include/gqf.h"
#include "include/gqf_int.h"

#define KEY_SIZE_BITS 64
#define VAL_SIZE_BITS 0

void dump_key(uint64_t key) {
	char *k = (char *)(&key) + 7;
	for (int i=8; i>0; i--) {
		uint8_t kc = (uint8_t)(*k);
		printf("%02x ", kc);
		k = k - 1;
	}
	printf("\n");
}

int main(int argc, char **argv)
{
	if (argc < 2) {
		fprintf(stderr, "Please specify the log of the number of slots.\n");
		exit(1);
	}
	QF qf;
	uint64_t qbits = atoi(argv[1]);
	uint64_t rbits = KEY_SIZE_BITS - qbits;
	uint64_t vbits = VAL_SIZE_BITS;
	uint64_t nslots = (1ULL << qbits);
	uint64_t nvals = 95*nslots/100;
	uint64_t key_count = 4;

	// 0 for now, these will be adjusted runtime.
	uint64_t tombstone_space = 0;
	uint64_t nrebuilds = 0;
	uint64_t *keys;
	uint64_t *vals;

	/* Initialise the CQF */
	if (!qf_malloc_advance(&qf, nslots, qbits + rbits, vbits, QF_HASH_NONE, 0, tombstone_space, nrebuilds)) {
		fprintf(stderr, "Can't allocate CQF.\n");
		abort();
	}

	// TODO(chesetti): Enable auto resize once implemented.
	// qf_set_auto_resize(&qf, true);

	/* Generate random values */
	keys = (uint64_t*)malloc(nvals*sizeof(vals[0]));
	vals = (uint64_t*)malloc(nvals*sizeof(keys[0]));
	RAND_bytes((unsigned char *)vals, sizeof(*vals) * nvals);
	RAND_bytes((unsigned char *)keys, sizeof(*keys) * nvals);
	srand(0);

	/* Insert keys in the CQF */
	fprintf(stdout, "Testing inserts.\n");
	for (uint64_t i = 0; i < nvals; i++) {
		int ret = qf_insert(&qf, keys[i], vals[i], QF_NO_LOCK | QF_KEY_IS_HASH);
		if (ret < 0) {
			fprintf(stderr, "failed insertion for key: %lxd.\n", keys[i]);
			if (ret == QF_NO_SPACE)
				fprintf(stderr, "CQF is full.\n");
			else if (ret == QF_COULDNT_LOCK)
				fprintf(stderr, "TRY_ONCE_LOCK failed.\n");
			else
				fprintf(stderr, "Does not recognise return value.\n");
			abort();
		}
	}

	#if 0
	qf_dump(&qf);
	qf_sync_counters(&qf);
	qf_dump_metadata(&qf);
	#endif

	/* Query keys in the CQF */
	fprintf(stdout, "Testing queries .\n");
	for (uint64_t i = 0; i < nvals; i++) {
		uint64_t val;
		int ret = qf_query(&qf, keys[i], &val, QF_NO_LOCK | QF_KEY_IS_HASH);
		if (ret != 1) {
			fprintf(stderr, "failed query for key: %lx\n", keys[i]);
			if (ret == QF_NO_SPACE)
				fprintf(stderr, "CQF is full.\n");
			else if (ret == QF_COULDNT_LOCK)
				fprintf(stderr, "TRY_ONCE_LOCK failed.\n");
			else
				fprintf(stderr, "Does not recognise return value.\n");
			abort();
		}
	}

	/* Initialize an iterator and validate counts. */

	// TODO(chesetti): Write test for iterator.
	/*
	fprintf(stdout, "Testing iterator and unique indexes.\n");
	int64_t last_index = -1;
	int i = 0;
	qf_iterator_from_position(&file_qf, &qfi, 0);
	while(!qfi_end(&qfi)) {
		uint64_t key, value, count;
		qfi_get_key(&qfi, &key, &value, &count);
		if (count < key_count) {
			fprintf(stderr, "Failed lookup during iteration for: %lx. Returned count: %ld\n",
							key, count);
			abort();
		}
		int64_t idx = qf_get_unique_index(&file_qf, key, value, 0);
		if (idx == QF_DOESNT_EXIST) {
			fprintf(stderr, "Failed lookup for unique index for: %lx. index: %ld\n",
							key, idx);
			abort();
		}
		if (idx <= last_index) {
			fprintf(stderr, "Unique indexes not strictly increasing.\n");
			abort();
		}
		last_index = idx;
		if (qf_count_key_value(&unique_idx, key, 0, 0) > 0) {
			fprintf(stderr, "Failed unique index for: %lx. index: %ld\n",
							key, idx);
			abort();
		}
		qf_insert(&unique_idx, key, 0, 1, QF_NO_LOCK);
		int64_t newindex = qf_get_unique_index(&unique_idx, key, 0, 0);
		if (idx < newindex) {
			fprintf(stderr, "Index weirdness: index %dth key %ld was at %ld, is now at %ld\n",
							i, key, idx, newindex);
			//abort();
		}

		i++;
		qfi_next(&qfi);
	}
	*/

	/* remove some counts  (or keys) and validate. */
	fprintf(stdout, "Testing remove/delete_key.\n");
	for (uint64_t i = 0; i < nvals; i++) {
		int ret = qf_remove(&qf, keys[i], QF_NO_LOCK | QF_KEY_IS_HASH);
		if (ret < 0) {
			fprintf(stderr, "failed deletion for ret code: %d.\n", ret);
			dump_key(keys[i]);
			abort();
		}
		uint64_t val;
		/* Query that keys are deleted in the CQF */
		ret = qf_query(&qf, keys[i], &val, QF_NO_LOCK | QF_KEY_IS_HASH);
		if (ret != QF_DOESNT_EXIST) {
			fprintf(stderr, "Did not delete key %ld\n", keys[i]);
			abort();
		}
	}
	fprintf(stdout, "Validated the CQF.\n");
}

