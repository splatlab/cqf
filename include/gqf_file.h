/*
 * ============================================================================
 *
 *        Authors:  Prashant Pandey <ppandey@cs.stonybrook.edu>
 *                  Rob Johnson <robj@vmware.com>   
 *
 * ============================================================================
 */

#ifndef _GQF_FILE_H_
#define _GQF_FILE_H_

#include <inttypes.h>
#include <pthread.h>
#include <stdbool.h>

#include "gqf.h"

#ifdef __cplusplus
extern "C" {
#endif

	/* Initialize a file-backed CQF at "filename". */
	bool qf_initfile(QF *qf, uint64_t nslots, uint64_t key_bits, uint64_t
									value_bits, enum qf_hashmode hash, uint32_t seed, const char*
									filename);

#define QF_USEFILE_READ_ONLY (0x01)
#define QF_USEFILE_READ_WRITE (0x02)

	/* Read "filename" into "qf". */
	uint64_t qf_usefile(QF* qf, const char* filename, int flag);

	/* Resize the QF to the specified number of slots.  Uses mmap to
	 * initialize the new file, and calls munmap() on the old memory.
	 * Return value:
	 *    >= 0: number of keys copied during resizing.
	 * */
	int64_t qf_resize_file(QF *qf, uint64_t nslots);

	bool qf_closefile(QF* qf);

	bool qf_deletefile(QF* qf);

	/* write data structure of to the disk */
	uint64_t qf_serialize(const QF *qf, const char *filename);

	/* read data structure off the disk */
	uint64_t qf_deserialize(QF *qf, const char *filename);

#ifdef __cplusplus
}
#endif

#endif // _GQF_FILE_H_
