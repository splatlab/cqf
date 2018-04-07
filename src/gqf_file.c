/*
 * =====================================================================================
 *
 *       Filename:  gqf_file.c
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2018-03-21 10:57:02 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Prashant Pandey (), ppandey@cs.stonybrook.edu
 *   Organization:  Stony Brook University
 *
 * =====================================================================================
 */

#include <stdlib.h>
#if 0
# include <assert.h>
#else
# define assert(x)
#endif
#include <string.h>
#include <inttypes.h>
#include <stdio.h>
#include <unistd.h>
#include <math.h>
#include <time.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "include/hashutil.h"
#include "include/gqf.h"
#include "include/gqf_file.h"


bool qf_initfile(QF *qf, uint64_t nslots, uint64_t key_bits, uint64_t
								 value_bits, enum lockingmode lock, enum hashmode hash,
								 uint32_t seed, char* filename)
{
	uint64_t total_num_bytes = qf_init(qf, nslots, key_bits, value_bits,
																		 lock, hash, seed, NULL, 0);

	int ret;
	qf->runtimedata = (qfruntime *)calloc(sizeof(qfruntime), 1);
	if (qf->runtimedata == NULL) {
		perror("Couldn't allocate memory for runtime data.");
		exit(EXIT_FAILURE);
	}
	qf->runtimedata->f_info.fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, S_IRWXU);
	if (qf->runtimedata->f_info.fd < 0) {
		perror("Couldn't open file.");
		exit(EXIT_FAILURE);
	}
	if((ret = posix_fallocate(qf->runtimedata->f_info.fd, 0, total_num_bytes)) < 0) {
		perror("Couldn't fallocate file:\n");
		exit(EXIT_FAILURE);
	}
	qf->metadata = (qfmetadata *)mmap(NULL, total_num_bytes, PROT_READ |
																		PROT_WRITE, MAP_SHARED,
																		qf->runtimedata->f_info.fd, 0);
	ret = madvise(qf->metadata, total_num_bytes, MADV_RANDOM);
	if (ret < 0) {
		perror("Couldn't fallocate file.");
		exit(EXIT_FAILURE);
	}
	qf->blocks = (qfblock *)(qf->metadata + 1);

	uint64_t init_size = qf_init(qf, nslots, key_bits, value_bits, lock, hash,
															 seed, qf->metadata, total_num_bytes);
	qf->runtimedata->f_info.filepath = malloc(strlen(filename));
	strcpy(qf->runtimedata->f_info.filepath, filename);

	if (init_size == total_num_bytes)
		return true;
	else
		return false;
}

uint64_t qf_usefile(QF* qf, enum lockingmode lock, char* filename)
{
	struct stat sb;
	int ret;

	qf->runtimedata = (qfruntime *)calloc(sizeof(qfruntime), 1);
	qf->runtimedata->f_info.fd = open(filename, O_RDWR, S_IRWXU);
	if (qf->runtimedata->f_info.fd < 0) {
		perror("Couldn't open file.");
		exit(EXIT_FAILURE);
	}

	ret = fstat (qf->runtimedata->f_info.fd, &sb);
	if ( ret < 0) {
		perror ("fstat");
		exit(EXIT_FAILURE);
	}

	if (!S_ISREG (sb.st_mode)) {
		fprintf (stderr, "%s is not a file.\n", filename);
		exit(EXIT_FAILURE);
	}

	qf->runtimedata->f_info.filepath = malloc(strlen(filename));
	strcpy(qf->runtimedata->f_info.filepath, filename);
	qf->runtimedata->lock_mode = lock;
	/* initialize all the locks to 0 */
	qf->runtimedata->metadata_lock = 0;
	qf->runtimedata->locks = (volatile int *)calloc(qf->runtimedata->num_locks,
																					sizeof(volatile int));
#ifdef LOG_WAIT_TIME
	qf->runtimedata->wait_times = (wait_time_data* )calloc(qf->runtimedata->num_locks+1,
																								 sizeof(wait_time_data));
#endif
	qf->metadata = (qfmetadata *)mmap(NULL, sb.st_size, PROT_READ | PROT_WRITE,
																		MAP_SHARED, qf->runtimedata->f_info.fd, 0);
	qf->blocks = (qfblock *)(qf->metadata + 1);

	return sizeof(qfmetadata) + qf->metadata->total_size_in_bytes;
}

bool qf_closefile(QF* qf)
{
	assert(qf->metadata != NULL);
	int fd = qf->runtimedata->f_info.fd;
	uint64_t size = qf->metadata->total_size_in_bytes + sizeof(qfmetadata);
	void *buffer = qf_destroy(qf);
	if (buffer != NULL) {
		munmap(buffer, size);
		close(fd);
		return true;
	}

	return false;
}

bool qf_deletefile(QF* qf)
{
	assert(qf->metadata != NULL);
	char *path = malloc(strlen(qf->runtimedata->f_info.filepath));
	strcpy(path, qf->runtimedata->f_info.filepath);
	if (qf_closefile(qf)) {
		remove(path);
		return true;
	}

	return false;
}

uint64_t qf_serialize(const QF *qf, const char *filename)
{
	FILE *fout;
	fout = fopen(filename, "wb+");
	if (fout == NULL) {
		perror("Error opening file for serializing.");
		exit(EXIT_FAILURE);
	}
	fwrite(qf->metadata, sizeof(qfmetadata), 1, fout);
	fwrite(qf->blocks, qf->metadata->total_size_in_bytes, 1, fout);
	fclose(fout);
	
	return sizeof(qfmetadata) + qf->metadata->total_size_in_bytes;
}

uint64_t qf_deserialize(QF *qf, const char *filename)
{
	FILE *fin;
	fin = fopen(filename, "rb");
	if (fin == NULL) {
		perror("Error opening file for deserializing.");
		exit(EXIT_FAILURE);
	}

	qf->runtimedata = (qfruntime *)calloc(sizeof(qfruntime), 1);
	if (qf->runtimedata == NULL) {
		perror("Couldn't allocate memory for runtime data.");
		exit(EXIT_FAILURE);
	}
	qf->metadata = (qfmetadata *)calloc(sizeof(qfmetadata), 1);
	fread(qf->metadata, sizeof(qfmetadata), 1, fin);
	/* initlialize the locks in the QF */
	qf->runtimedata->num_locks = (qf->metadata->xnslots/NUM_SLOTS_TO_LOCK)+2;
	qf->runtimedata->metadata_lock = 0;
	/* initialize all the locks to 0 */
	qf->runtimedata->locks = (volatile int *)calloc(qf->runtimedata->num_locks,
																					sizeof(volatile int));
	qf->blocks = (qfblock *)calloc(qf->metadata->total_size_in_bytes, 1);
	fread(qf->blocks, qf->metadata->total_size_in_bytes, 1, fin);
	fclose(fin);

	return sizeof(qfmetadata) + qf->metadata->total_size_in_bytes;
}

