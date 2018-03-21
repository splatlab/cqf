#ifndef QF_H
#define QF_H

#include <inttypes.h>
#include <pthread.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

	/* Can be 
		 0 (choose size at run-time), 
		 8, 16, 32, or 64 (for optimized versions),
		 or other integer <= 56 (for compile-time-optimized bit-shifting-based versions)
		 */
#define BITS_PER_SLOT 0

#define BITMASK(nbits) ((nbits) == 64 ? 0xffffffffffffffff : (1ULL << (nbits)) \
												- 1ULL)

	struct __attribute__ ((__packed__)) qfblock;
	typedef struct qfblock qfblock;

	enum hashmode {
		DEFAULT,
		INVERTIBLE,
		NONE
	};

	enum lockingmode {
		LOCKS_FORBIDDEN,
		LOCKS_OPTIONAL,
		LOCKS_REQUIRED
	};

	// The below struct is used to instrument the code.
	// It is not used in normal operations of the CQF.
	typedef struct {
		uint64_t total_time_single;
		uint64_t total_time_spinning;
		uint64_t locks_taken;
		uint64_t locks_acquired_single_attempt;
	} wait_time_data;

	typedef struct quotient_filter_mem {
		int fd;
		enum lockingmode lock_mode;
		volatile int metadata_lock;
		volatile int *locks;
		wait_time_data *wait_times;
	} quotient_filter_mem;

	typedef quotient_filter_mem qfmem;

	typedef struct quotient_filter_metadata {
		char filepath[50];
		enum hashmode hash_mode;
		uint32_t auto_resize;
		uint64_t size;
		uint32_t seed;
		uint64_t nslots;
		uint64_t xnslots;
		uint64_t key_bits;
		uint64_t value_bits;
		uint64_t key_remainder_bits;
		uint64_t bits_per_slot;
		__uint128_t range;
		uint64_t nblocks;
		uint64_t nelts;
		uint64_t ndistinct_elts;
		uint64_t noccupied_slots;
		uint64_t num_locks;
	} quotient_filter_metadata;

	typedef quotient_filter_metadata qfmetadata;

	typedef struct quotient_filter {
		qfmem *mem;
		qfmetadata *metadata;
		qfblock *blocks;
	} quotient_filter;

	typedef quotient_filter QF;

	// The below struct is used to instrument the code.
	// It is not used in normal operations of the CQF.
	typedef struct {
		uint64_t start_index;
		uint16_t length;
	} cluster_data;

	typedef struct quotient_filter_iterator {
		const QF *qf;
		uint64_t run;
		uint64_t current;
		uint64_t cur_start_index;
		uint16_t cur_length;
		uint32_t num_clusters;
		cluster_data *c_info;
	} quotient_filter_iterator;

	typedef quotient_filter_iterator QFi;

	/* Forward declaration for the macro. */
	void qf_dump_metadata(const QF *qf);

#define DEBUG_CQF(fmt, ...) \
	do { if (PRINT_DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)

#define DEBUG_DUMP(qf) \
	do { if (PRINT_DEBUG) qf_dump_metadata(qf); } while (0)

	uint64_t qf_init(QF *qf, uint64_t nslots, uint64_t key_bits, uint64_t
									 value_bits, enum lockingmode lock, enum hashmode hash,
									 uint32_t seed, void* buffer, uint64_t buffer_len);

	uint64_t qf_use(QF* qf, void* buffer, uint64_t buffer_len, enum lockingmode
									lock);

	void *qf_destroy(QF *qf);

	QF *qf_initfile(uint64_t nslots, uint64_t key_bits, uint64_t value_bits,
									enum lockingmode lock, enum hashmode hash, uint32_t seed,
									char* filename);

	uint64_t qf_usefile(QF* qf, enum lockingmode lock, char* filename);

	bool qf_closefile(QF* qf);

	bool qf_deletefile(QF* qf);

	QF *qf_malloc(uint64_t nslots, uint64_t key_bits, uint64_t value_bits, enum
								lockingmode lock, enum hashmode hash, uint32_t seed);

	bool qf_free(QF *qf);

	void qf_reset(QF *qf);

	/* write data structure of to the disk */
	uint64_t qf_serialize(const QF *qf, const char *filename);

	/* read data structure off the disk */
	uint64_t qf_deserialize(QF *qf, const char *filename);

	/* The caller should call qf_init on the dest QF using the same parameters
	 * as the src QF before calling this function. */
	void qf_copy(QF *dest, const QF *src);

	bool qf_resize_malloc(QF *qf, uint64_t nslots);

	uint64_t qf_resize(QF* qf, uint64_t nslots, void* buffer, uint64_t
										 buffer_len);

	/* Increment the counter for this key/value pair by count. */
	bool qf_insert(QF *qf, uint64_t key, uint64_t value, uint64_t count);

	/* Set the counter for this key/value pair to count. */
	bool qf_set_count(QF *qf, uint64_t key, uint64_t value, uint64_t count);

	/* Remove count instances of this key/value combination. */
	bool qf_remove(QF *qf, uint64_t key, uint64_t value, uint64_t count);

	/* Remove all instances of this key/value pair. */
	bool qf_delete_key_value(QF *qf, uint64_t key, uint64_t value);

	/* Remove all instances of this key. */
	void qf_delete_key(QF *qf, uint64_t key);

	/* Replace the association (key, oldvalue, count) with the association
		 (key, newvalue, count). If there is already an association (key,
		 newvalue, count'), then the two associations will be merged and
		 their counters will be summed, resulting in association (key,
		 newvalue, count' + count). */
	void qf_replace(QF *qf, uint64_t key, uint64_t oldvalue, uint64_t newvalue);

	/* Lookup the value associated with key.  Returns the count of that
		 key/value pair in the QF.  If it returns 0, then, the key is not
		 present in the QF. Only returns the first value associated with key
		 in the QF.  If you want to see others, use an iterator. */
	uint64_t qf_query(const QF *qf, uint64_t key, uint64_t *value);

	/* Return the number of times key has been inserted, with any value,
		 into qf. */
	uint64_t qf_count_key(const QF *qf, uint64_t key);

	/* Return the number of times key has been inserted, with the given
		 value, into qf. */
	uint64_t qf_count_key_value(const QF *qf, uint64_t key, uint64_t value);

	/* Initialize an iterator */
	bool qf_iterator(const QF *qf, QFi *qfi, uint64_t position);

	/* Initialize an iterator and position it at the smallest index containing a
	 * hash value greater than or equal to "hash". */
	bool qf_iterator_hash(const QF *qf, QFi *qfi, uint64_t hash);

	/* Returns 0 if the iterator is still valid (i.e. has not reached the
		 end of the QF. */
	int qfi_get(const QFi *qfi, uint64_t *key, uint64_t *value, uint64_t *count);

	/* Advance to next entry.  Returns whether or not another entry is
		 found.  */
	int qfi_next(QFi *qfi);

	/* Check to see if the if the end of the QF */
	int qfi_end(const QFi *qfi); 

	/* For debugging */
	void qf_dump(const QF *);

	/* mmap the QF from disk. */
	void qf_read(QF *qf, const char *path);

	/* merge two QFs into the third one. */
	void qf_merge(QF *qfa, QF *qfb, QF *qfc);

	/* merge multiple QFs into the final QF one. */
	void qf_multi_merge(const QF *qf_arr[], int nqf, QF *qfr);

	/* find cosine similarity between two QFs. */
	uint64_t qf_inner_product(QF *qfa, QF *qfb);

	/* magnitude of a QF. */
	uint64_t qf_magnitude(QF *qf);

#ifdef __cplusplus
}
#endif

#endif /* QF_H */
