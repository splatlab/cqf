/*
 * ============================================================================
 *
 *        Authors:  Prashant Pandey <ppandey@cs.stonybrook.edu>
 *                  Rob Johnson <robj@vmware.com>   
 *
 * ============================================================================
 */

#ifndef _GQF_H_
#define _GQF_H_

#include <inttypes.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

	typedef struct quotient_filter quotient_filter;
	typedef quotient_filter QF;

	/* CQFs support three hashing modes:

		 - DEFAULT uses a hash that may introduce false positives, but
       this can be useful when inserting large keys that need to be
       hashed down to a small fingerprint.  With this type of hash,
       you can iterate over the hash values of all the keys in the
       CQF, but you cannot iterate over the keys themselves.

		 - INVERTIBLE has no false positives, but the size of the hash
       output must be the same as the size of the hash input,
       e.g. 17-bit keys hashed to 17-bit outputs.  So this mode is
       generally only useful when storing small keys in the CQF.  With
       this hashing mode, you can use iterators to enumerate both all
       the hashes in the CQF, or all the keys.

		 - NONE, for when you've done the hashing yourself.  WARNING: the
		   CQF can exhibit very bad performance if you insert a skewed
			 distribution of intputs.
	*/
	
	enum qf_hashmode {
		QF_HASH_INVERTIBLE,
		QF_HASH_NONE
	};

	/* The CQF supports concurrent insertions and queries.  Only the
		 portion of the CQF being examined or modified is locked, so it
		 supports high throughput even with many threads.

		 The CQF operations support 3 locking modes:

		 - NO_LOCK: for single-threaded applications or applications
       that do their own concurrency management.

		 - WAIT_FOR_LOCK: Spin until you get the lock, then do the query
       or update.

		 - TRY_ONCE_LOCK: If you can't grab the lock on the first try,
       return with an error code.
	*/
#define QF_NO_LOCK (0x01)
#define QF_TRY_ONCE_LOCK (0x02)
#define QF_WAIT_FOR_LOCK (0x04)

	/* It is sometimes useful to insert a key that has already been
		 hashed. */
#define QF_KEY_IS_HASH (0x08)

	/******************************************
		 The CQF defines low-level constructor and destructor operations
		 that are designed to enable the application to manage the memory
		 used by the CQF. 
	*******************************************/
	
	/*
	 * Create an empty CQF in "buffer".  If there is not enough space at
	 * buffer then it will return the total size needed in bytes to
	 * initialize the CQF.  This function takes ownership of buffer.
	 */
	uint64_t qf_init(QF *qf, uint64_t nslots, uint64_t key_bits, 
									 uint64_t value_bits, 
									 uint64_t tombstone_space, uint64_t nrebuilds,
									 enum qf_hashmode hash, uint32_t seed, void*
									 buffer, uint64_t buffer_len);

	/* Destroy this CQF.  Returns a pointer to the memory that the CQF was
		 using (i.e. passed into qf_init or qf_use) so that the application
		 can release that memory. */
	void *qf_destroy(QF *qf);

	/***********************************
    The following convenience functions create and destroy CQFs by
		using malloc/free to obtain and release the memory for the CQF. 
	************************************/
	
	/* Initialize the CQF and allocate memory for the CQF. */
	bool qf_malloc(QF *qf, uint64_t nslots, uint64_t key_bits, uint64_t
								 value_bits, enum qf_hashmode hash, uint32_t seed);

	bool qf_free(QF *qf);

	/***********************************
   Functions for modifying the CQF.
	***********************************/

#define QF_NO_SPACE (-1)
#define QF_COULDNT_LOCK (-2)
#define QF_DOESNT_EXIST (-3)
	
	/* Insert this key/value pair. 
	 * Return value:
	 *    >= 0: distance from the home slot to the slot in which the key is
	 *          inserted (or 0 if count == 0).
	 *    == QF_NO_SPACE: the CQF has reached capacity.
	 *    == QF_COULDNT_LOCK: TRY_ONCE_LOCK has failed to acquire the lock.
	 */
	int qf_insert(QF *qf, uint64_t key, uint64_t value, uint8_t flags);

	/* Remove this key.
	 * Return value:
	 *    >= 0: distance from the home slot to the slot in which the key is
	 *          found.
	 *    == QF_DOESNT_EXIST: Specified item did not exist.
	 *    == QF_COULDNT_LOCK: TRY_ONCE_LOCK has failed to acquire the lock.
	 */
	int qf_remove(QF *qf, uint64_t key, uint8_t flags);

	/* Rebuild the next rebuild intervals (c_r * c_p). 
		 May return QF_COULDNT_LOCK if called with QF_TRY_ONCE_LOCK.
	*/
	int qf_rebuild(const QF *qf, uint8_t flags);

	/****************************************
   Query functions
	****************************************/
	
	/* Lookup the key.  Returns if the key is in the QF.
	 * Return value:
	 *    >= 0: distance from the home slot to the slot in which the key is
	 *          found.
	 *    == QF_DOESNT_EXIST: Specified item did not exist.
	 *    == QF_COULDNT_LOCK: TRY_ONCE_LOCK has failed to acquire the lock.
	 */
	int qf_query(const QF *qf, uint64_t key, uint64_t *value, uint8_t flags);

	/* Return the number of times key has been inserted, with any value,
		 into qf. */
	/* NOT IMPLEMENTED YET. */
	//uint64_t qf_count_key(const QF *qf, uint64_t key);

	// What is this?
	/* Returns a unique index corresponding to the key in the CQF.  Note
		 that this can change if further modifications are made to the
		 CQF.

		 If the key is not found then returns QF_DOESNT_EXIST.
		 May return QF_COULDNT_LOCK if called with QF_TRY_ONCE_LOCK.
	 */
	int64_t qf_get_unique_index(const QF *qf, uint64_t key, uint64_t value, 
															uint8_t flags);

	/****************************************
   Metadata accessors.
	****************************************/

	/* Hashing info */
	enum qf_hashmode qf_get_hashmode(const QF *qf);
	uint64_t         qf_get_hash_seed(const QF *qf);
	__uint128_t      qf_get_hash_range(const QF *qf);

	/* Space usage info. */
	uint64_t qf_get_total_size_in_bytes(const QF *qf);
	uint64_t qf_get_nslots(const QF *qf);
	uint64_t qf_get_num_occupied_slots(const QF *qf);

	/* Bit-sizes info. */
	uint64_t qf_get_num_key_bits(const QF *qf);
	uint64_t qf_get_num_value_bits(const QF *qf);
	uint64_t qf_get_num_key_remainder_bits(const QF *qf);
	uint64_t qf_get_bits_per_slot(const QF *qf);

	/* Number of keys. */
	uint64_t qf_get_sum_of_counts(const QF *qf);

	void qf_sync_counters(const QF *qf);

	/****************************************
		Iterators
	*****************************************/
	
	typedef struct quotient_filter_iterator quotient_filter_iterator;
	typedef quotient_filter_iterator QFi;

#define QF_INVALID (-4)
#define QFI_INVALID (-5)
	
	/* Initialize an iterator starting at the given position.
	 * Return value:
	 *  >= 0: iterator is initialized and positioned at the returned slot.
	 *   = QFI_INVALID: iterator has reached end.
	 */
	int64_t qf_iterator_from_position(const QF *qf, QFi *qfi, uint64_t position);

	/* Initialize an iterator and position it at the smallest index
	 * containing a key whose hash is greater than or equal
	 * to the specified key.
	 * Return value:
	 *  >= 0: iterator is initialized and position at the returned slot.
	 *   = QFI_INVALID: iterator has reached end.
	 */
	int64_t qf_iterator_from_key(const QF *qf, QFi *qfi, uint64_t key, uint8_t flags);

	/* Find key and value at the current position of the iterator.
	 * Return value:
	 *   = 0: Iterator is still valid, i.e. the key exists.
	 *   = QFI_INVALID: iterator has reached end, i.e. the key doesn't exist.
	 */
	int qfi_get_key(const QFi *qfi, uint64_t *key, uint64_t *value);

	/* Find hash of the key and the value at the current position of the iterator.
	 * Return value:
	 *   = 0: Iterator is still valid.
	 *   = QFI_INVALID: iterator has reached end.
	 */
	int qfi_get_hash(const QFi *qfi, uint64_t *hash, uint64_t *value);

	/* Advance to next entry.
	 * Return value:
	 *   = 0: Iterator is still valid.
	 *   = QFI_INVALID: iterator has reached end.
	 */
	int qfi_next(QFi *qfi);

	/* Check to see if the if the end of the QF */
	bool qfi_end(const QFi *qfi);

	/************************************
   Miscellaneous convenience functions.
	*************************************/
	
	/* Reset the CQF to an empty filter. */
	void qf_reset(QF *qf);

	/* The caller should call qf_init on the dest QF using the same
	 * parameters as the src QF before calling this function. Note: src
	 * and dest must be exactly the same, including number of slots.  */
	void qf_copy(QF *dest, const QF *src);

	/***********************************
		Debugging functions.
	************************************/

	void qf_dump(const QF *);
	void qf_dump_metadata(const QF *qf);


#ifdef __cplusplus
}
#endif

#endif /* _GQF_H_ */
