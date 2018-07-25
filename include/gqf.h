/*
 * =====================================================================================
 *
 *       Filename:  gqf.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2018-03-21 10:43:39 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Prashant Pandey (), ppandey@cs.stonybrook.edu
 *   Organization:  Stony Brook University
 *
 * =====================================================================================
 */

#ifndef _GQF_H_
#define _GQF_H_

#include <inttypes.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

	enum qf_hashmode {
		DEFAULT,
		INVERTIBLE,
		NONE
	};

	enum qf_runtimelockingmode {
		WAIT_FOR_LOCK,
		TRY_ONCE_LOCK,
		NO_LOCK
	};

	typedef struct quotient_filter_s quotient_filter;
	typedef quotient_filter QF;

  typedef struct quotient_filter_iterator_s quotient_filter_iterator;
	typedef quotient_filter_iterator QFi;

	/* Forward declaration for the macro. */
	void qf_dump_metadata(const QF *qf);

	/* Initialize the CQF at "buffer".
	 * If there is not enough space at buffer then it will return the total size
	 * needed in bytes to initialize the CQF.
	 * */
	uint64_t qf_init(QF *qf, uint64_t nslots, uint64_t key_bits, uint64_t
									 value_bits, enum qf_hashmode hash, uint32_t seed, void*
									 buffer, uint64_t buffer_len);

	/* Read the CQF stored at "buffer". */
	uint64_t qf_use(QF* qf, void* buffer, uint64_t buffer_len);

	void *qf_destroy(QF *qf);

	/* Initialize the CQF and allocate memory for the CQF. */
	bool qf_malloc(QF *qf, uint64_t nslots, uint64_t key_bits, uint64_t
								 value_bits, enum qf_hashmode hash, uint32_t seed);

	bool qf_free(QF *qf);

	void qf_reset(QF *qf);

	/* The caller should call qf_init on the dest QF using the same parameters
	 * as the src QF before calling this function. */
	void qf_copy(QF *dest, const QF *src);

	/* Allocate a new CQF using "nslots" and copy elements from "qf" into it. 
	 * Return value:
	 *    >= 0: number of keys copied during resizing.
	 * */
	int64_t qf_resize_malloc(QF *qf, uint64_t nslots);

	/* Allocate a new CQF using "nslots" at "buffer" and copy elements from "qf"
	 * into it. 
	 * If there is not enough space at buffer then it will return the total size
	 * needed in bytes to initialize the new CQF.
	 * */
	uint64_t qf_resize(QF* qf, uint64_t nslots, void* buffer, uint64_t
										 buffer_len);

	void qf_set_auto_resize(QF* qf);

	/* Increment the counter for this key/value pair by count. 
	 * Return value:
	 *    >= 0: distance from the home slot to the slot in which the key is
	 *          inserted.
	 *    = -1: the CQF has reached capacity.
	 *    = -2: TRY_ONCE_LOCK has failed to acquire the lock.
	 */
	int qf_insert(QF *qf, uint64_t key, uint64_t value, uint64_t count, enum
								qf_runtimelockingmode runtime_lock);

	/* Set the counter for this key/value pair to count. */
	int qf_set_count(QF *qf, uint64_t key, uint64_t value, uint64_t count, enum
										qf_runtimelockingmode runtime_lock);

	/* Remove count instances of this key/value combination. 
	 * Return value:
	 *    >= 0: number of slots freed.
	 *    < -1: deletion error.
	 *    = -2: TRY_ONCE_LOCK has failed to acquire the lock.
	 */
	int qf_remove(QF *qf, uint64_t key, uint64_t value, uint64_t count, enum
								 qf_runtimelockingmode runtime_lock);

	/* Remove all instances of this key/value pair. */
	int qf_delete_key_value(QF *qf, uint64_t key, uint64_t value, enum
													 qf_runtimelockingmode runtime_lock);

	/* Remove all instances of this key. */
	/* NOT IMPLEMENTED YET. */
	//void qf_delete_key(QF *qf, uint64_t key);

	/* Replace the association (key, oldvalue, count) with the association
		 (key, newvalue, count). If there is already an association (key,
		 newvalue, count'), then the two associations will be merged and
		 their counters will be summed, resulting in association (key,
		 newvalue, count' + count). */
	/* NOT IMPLEMENTED YET. */
	//void qf_replace(QF *qf, uint64_t key, uint64_t oldvalue, uint64_t newvalue);

	/* Lookup the value associated with key.  Returns the count of that
		 key/value pair in the QF.  If it returns 0, then, the key is not
		 present in the QF. Only returns the first value associated with key
		 in the QF.  If you want to see others, use an iterator. */
	uint64_t qf_query(const QF *qf, uint64_t key, uint64_t *value);

	/* Return the number of times key has been inserted, with any value,
		 into qf. */
	/* NOT IMPLEMENTED YET. */
	//uint64_t qf_count_key(const QF *qf, uint64_t key);

	/* Return the number of times key has been inserted, with the given
		 value, into qf. */
	uint64_t qf_count_key_value(const QF *qf, uint64_t key, uint64_t value);

	/* Returns a unique index corresponding to the key in the CQF.
	 * If the key is not found then returns -1.
	 */
	int64_t qf_get_unique_index(const QF *qf, uint64_t key, uint64_t value);

	/* Initialize an iterator */
	bool qf_iterator(const QF *qf, QFi *qfi, uint64_t position);

	/* Initialize an iterator and position it at the smallest index containing a
	 * hash value greater than or equal to "hash". */
	bool qf_iterator_hash(const QF *qf, QFi *qfi, uint64_t hash);

	/* Returns 0 if the iterator is still valid (i.e. has not reached the end of
	 * the QF. 
	 * If the hash mode is INVERTIBLE or NONE.
	 * If the hash mode is DEFAULT then returns -1.
	 */
	int qfi_get_key(const QFi *qfi, uint64_t *key, uint64_t *value, uint64_t
									*count);

	/* Returns 0 if the iterator is still valid (i.e. has not reached the end of
	 * the QF. 
	 */
	int qfi_get_hash(const QFi *qfi, uint64_t *hash, uint64_t *value, uint64_t
									*count);

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
	void qf_merge(const QF *qfa, const QF *qfb, QF *qfc);

	/* merge multiple QFs into the final QF one. */
	void qf_multi_merge(const QF *qf_arr[], int nqf, QF *qfr);

	/* find cosine similarity between two QFs. */
	uint64_t qf_inner_product(const QF *qfa, const QF *qfb);

	/* magnitude of a QF. */
	uint64_t qf_magnitude(const QF *qf);

#ifdef __cplusplus
}
#endif

#endif /* _GQF_H_ */
