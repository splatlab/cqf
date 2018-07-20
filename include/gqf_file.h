/*
 * =====================================================================================
 *
 *       Filename:  gqf_file.h
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
									value_bits, enum qf_hashmode hash, uint32_t seed, char*
									filename);

	/* Read "filename" into "qf". */
	uint64_t qf_usefile(QF* qf, const char* filename);

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
