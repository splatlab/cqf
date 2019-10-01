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
#include <assert.h>
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
	uint64_t nhashbits = qbits + 9;
	uint64_t nslots = (1ULL << qbits);
	uint64_t nvals = 75*nslots/100;
	uint64_t key_count = 100000000;
	uint64_t *vals;

	/* Initialise the CQF */
	/*if (!qf_malloc(&qf, nslots, nhashbits, 0, QF_HASH_INVERTIBLE, 0)) {*/
		/*fprintf(stderr, "Can't allocate CQF.\n");*/
		/*abort();*/
	/*}*/
	if (!qf_initfile(&qf, nslots, nhashbits, 0, QF_HASH_INVERTIBLE, 0,
									 "mycqf.file")) {
		fprintf(stderr, "Can't allocate CQF.\n");
		abort();
	}

	qf_set_auto_resize(&qf, true);

	/* Generate random values */
	vals = (uint64_t*)malloc(nvals*sizeof(vals[0]));
	RAND_bytes((unsigned char *)vals, sizeof(*vals) * nvals);
	srand(0);
	for (uint64_t i = 0; i < nvals; i++) {
		vals[i] = (1 * vals[i]) % qf.metadata->range;
		/*vals[i] = rand() % qf.metadata->range;*/
		/*fprintf(stdout, "%lx\n", vals[i]);*/
	}

	/* Insert keys in the CQF */
	for (uint64_t i = 0; i < nvals; i++) {
		int ret = qf_insert(&qf, vals[i], 0, key_count, QF_NO_LOCK);
		if (ret < 0) {
			fprintf(stderr, "failed insertion for key: %lx %d.\n", vals[i], 50);
			if (ret == QF_NO_SPACE)
				fprintf(stderr, "CQF is full.\n");
			else if (ret == QF_COULDNT_LOCK)
				fprintf(stderr, "TRY_ONCE_LOCK failed.\n");
			else
				fprintf(stderr, "Does not recognise return value.\n");
			abort();
		}
	}

	/* Lookup inserted keys and counts. */
	for (uint64_t i = 0; i < nvals; i++) {
		uint64_t count = qf_count_key_value(&qf, vals[i], 0, 0);
		if (count < key_count) {
			fprintf(stderr, "failed lookup after insertion for %lx %ld.\n", vals[i],
							count);
			abort();
		}
	}

#if 0
	for (uint64_t i = 0; i < nvals; i++) {
		uint64_t count = qf_count_key_value(&qf, vals[i], 0, 0);
		if (count < key_count) {
			fprintf(stderr, "failed lookup during deletion for %lx %ld.\n", vals[i],
							count);
			abort();
		}
		if (count > 0) {
			/*fprintf(stdout, "deleting: %lx\n", vals[i]);*/
			qf_delete_key_value(&qf, vals[i], 0, QF_NO_LOCK);
			/*qf_dump(&qf);*/
			uint64_t cnt = qf_count_key_value(&qf, vals[i], 0, 0);
			if (cnt > 0) {
				fprintf(stderr, "failed lookup after deletion for %lx %ld.\n", vals[i],
								cnt);
				abort();
			}
		}
	}
#endif

	/* Write the CQF to disk and read it back. */
	char filename[] = "mycqf_serialized.cqf";
	fprintf(stdout, "Serializing the CQF to disk.\n");
	uint64_t total_size = qf_serialize(&qf, filename);
	if (total_size < sizeof(qfmetadata) + qf.metadata->total_size_in_bytes) {
		fprintf(stderr, "CQF serialization failed.\n");
		abort();
	}
	qf_deletefile(&qf);

	QF file_qf;
	fprintf(stdout, "Reading the CQF from disk.\n");
	if (!qf_usefile(&file_qf, filename, QF_USEFILE_READ_WRITE)) {
		fprintf(stderr, "Can't initialize the CQF from file: %s.\n", filename);
		abort();
	}
	for (uint64_t i = 0; i < nvals; i++) {
		uint64_t count = qf_count_key_value(&file_qf, vals[i], 0, 0);
		if (count < key_count) {
			fprintf(stderr, "failed lookup in file based CQF for %lx %ld.\n",
							vals[i], count);
			abort();
		}
	}

	fprintf(stdout, "Testing iterator and unique indexes.\n");
	/* Initialize an iterator and validate counts. */
	QFi qfi;
	QF unique_idx;
	if (!qf_malloc(&unique_idx, file_qf.metadata->nslots, nhashbits, 0,
								 QF_HASH_INVERTIBLE, 0)) {
		fprintf(stderr, "Can't allocate set.\n");
		abort();
	}
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
  
	/* remove some counts  (or keys) and validate. */
	fprintf(stdout, "Testing remove/delete_key.\n");
	for (uint64_t i = 0; i < nvals; i++) {
		uint64_t count = qf_count_key_value(&file_qf, vals[i], 0, 0);
		/*if (count < key_count) {*/
			/*fprintf(stderr, "failed lookup during deletion for %lx %ld.\n", vals[i],*/
							/*count);*/
			/*abort();*/
		/*}*/
		int ret = qf_delete_key_value(&file_qf, vals[i], 0, QF_NO_LOCK);
		count = qf_count_key_value(&file_qf, vals[i], 0, 0);
		if (count > 0) {
			if (ret < 0) {
				fprintf(stderr, "failed deletion for %lx %ld ret code: %d.\n",
								vals[i], count, ret);
				abort();
			}
			uint64_t new_count = qf_count_key_value(&file_qf, vals[i], 0, 0);
			if (new_count > 0) {
				fprintf(stderr, "delete key failed for %lx %ld new count: %ld.\n",
								vals[i], count, new_count);
				abort();
			}
		}
	}

	/*qf_deletefile(&file_qf);*/

	fprintf(stdout, "Validated the CQF.\n");
}

