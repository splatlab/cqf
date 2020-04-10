/*
 * ============================================================================
 *
 *        Authors:  Prashant Pandey <ppandey@cs.stonybrook.edu>
 *                  Rob Johnson <robj@vmware.com>   
 *
 * ============================================================================
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

#include "hashutil.h"
#include "gqf.h"
#include "gqf_int.h"
#include "gqf_file.h"

#define NUM_SLOTS_TO_LOCK (1ULL<<16)

bool qf_initfile(QF *qf, uint64_t nslots, uint64_t key_bits, uint64_t
								 value_bits, enum qf_hashmode hash, uint32_t seed, const char*
								 filename)
{
	uint64_t total_num_bytes = qf_init(qf, nslots, key_bits, value_bits, hash,
																		 seed, NULL, 0);

	int ret;
	qf->runtimedata = (qfruntime *)calloc(sizeof(qfruntime), 1);
	if (qf->runtimedata == NULL) {
		perror("Couldn't allocate memory for runtime data.");
		exit(EXIT_FAILURE);
	}
	qf->runtimedata->f_info.fd = open(filename, O_RDWR | O_CREAT | O_TRUNC,
																		S_IRWXU);
	if (qf->runtimedata->f_info.fd < 0) {
		perror("Couldn't open file.");
		exit(EXIT_FAILURE);
	}
	ret = posix_fallocate(qf->runtimedata->f_info.fd, 0, total_num_bytes);
	if (ret < 0) {
		perror("Couldn't fallocate file:\n");
		exit(EXIT_FAILURE);
	}
	qf->metadata = (qfmetadata *)mmap(NULL, total_num_bytes, PROT_READ |
																		PROT_WRITE, MAP_SHARED,
																		qf->runtimedata->f_info.fd, 0);
	if (qf->metadata == MAP_FAILED) {
		perror("Couldn't mmap metadata.");
		exit(EXIT_FAILURE);
	}
	ret = madvise(qf->metadata, total_num_bytes, MADV_RANDOM);
	if (ret < 0) {
		perror("Couldn't fallocate file.");
		exit(EXIT_FAILURE);
	}
	qf->blocks = (qfblock *)(qf->metadata + 1);

	uint64_t init_size = qf_init(qf, nslots, key_bits, value_bits, hash, seed,
															 qf->metadata, total_num_bytes);
	qf->runtimedata->f_info.filepath = (char *)malloc(strlen(filename) + 1);
	if (qf->runtimedata->f_info.filepath == NULL) {
		perror("Couldn't allocate memory for runtime f_info filepath.");
		exit(EXIT_FAILURE);
	}
	strcpy(qf->runtimedata->f_info.filepath, filename);
	/* initialize container resize */
	qf->runtimedata->container_resize = qf_resize_file;

	if (init_size == total_num_bytes)
		return true;
	else
		return false;
}

uint64_t qf_usefile(QF* qf, const char* filename, int flag)
{
	struct stat sb;
	int ret;

	int open_flag = 0, mmap_flag = 0;
	if (flag == QF_USEFILE_READ_ONLY) {
		open_flag = O_RDONLY;
		mmap_flag = PROT_READ;
	} else if(flag == QF_USEFILE_READ_WRITE) {
		open_flag = O_RDWR;
		mmap_flag = PROT_READ | PROT_WRITE;
	} else {
		fprintf(stderr, "Wrong flag specified.\n");
		return 0;
	}

	qf->runtimedata = (qfruntime *)calloc(sizeof(qfruntime), 1);
	if (qf->runtimedata == NULL) {
		perror("Couldn't allocate memory for runtime data.");
		exit(EXIT_FAILURE);
	}
	qf->runtimedata->f_info.fd = open(filename, open_flag);
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

	qf->runtimedata->f_info.filepath = (char *)malloc(strlen(filename) + 1);
	if (qf->runtimedata->f_info.filepath == NULL) {
		perror("Couldn't allocate memory for runtime f_info filepath.");
		exit(EXIT_FAILURE);
	}
	strcpy(qf->runtimedata->f_info.filepath, filename);
	/* initialize container resize */
	qf->runtimedata->container_resize = qf_resize_file;
	/* initialize all the locks to 0 */
	qf->runtimedata->metadata_lock = 0;
	qf->runtimedata->locks = (volatile int *)calloc(qf->runtimedata->num_locks,
																					sizeof(volatile int));
	if (qf->runtimedata->locks == NULL) {
		perror("Couldn't allocate memory for runtime locks.");
		exit(EXIT_FAILURE);
	}
#ifdef LOG_WAIT_TIME
	qf->runtimedata->wait_times = (wait_time_data* )calloc(qf->runtimedata->num_locks+1,
																								 sizeof(wait_time_data));
	if (qf->runtimedata->wait_times == NULL) {
		perror("Couldn't allocate memory for runtime wait_times.");
		exit(EXIT_FAILURE);
	}
#endif
	qf->metadata = (qfmetadata *)mmap(NULL, sb.st_size, mmap_flag, MAP_SHARED,
																		qf->runtimedata->f_info.fd, 0);
	if (qf->metadata == MAP_FAILED) {
		perror("Couldn't mmap metadata.");
		exit(EXIT_FAILURE);
	}
	if (qf->metadata->magic_endian_number != MAGIC_NUMBER) {
		fprintf(stderr, "Can't read the CQF. It was written on a different endian machine.");
		exit(EXIT_FAILURE);
	}
	qf->blocks = (qfblock *)(qf->metadata + 1);

	pc_init(&qf->runtimedata->pc_nelts, (int64_t*)&qf->metadata->nelts, 8, 100);
	pc_init(&qf->runtimedata->pc_ndistinct_elts, (int64_t*)&qf->metadata->ndistinct_elts, 8, 100);
	pc_init(&qf->runtimedata->pc_noccupied_slots, (int64_t*)&qf->metadata->noccupied_slots, 8, 100);

	return sizeof(qfmetadata) + qf->metadata->total_size_in_bytes;
}

int64_t qf_resize_file(QF *qf, uint64_t nslots)
{
	// calculate the new filename length
	int new_filename_len = strlen(qf->runtimedata->f_info.filepath) + 1;
	new_filename_len += 13; // To have an underscore and the nslots.
	char *new_filename = (char *)malloc(new_filename_len);
	if (new_filename == NULL) {
		perror("Couldn't allocate memory for filename buffer during resize.");
		exit(EXIT_FAILURE);
	}
	// Create new filename
	uint64_t ret = snprintf(new_filename, new_filename_len, "%s_%ld",
										 qf->runtimedata->f_info.filepath, nslots);
	if (ret <= strlen(qf->runtimedata->f_info.filepath)) {
		fprintf(stderr, "Wrong new filename created!");
		return -1;
	}

	QF new_qf;
	if (!qf_initfile(&new_qf, nslots, qf->metadata->key_bits,
								 qf->metadata->value_bits, qf->metadata->hash_mode,
								 qf->metadata->seed, new_filename))
		return false;
	if (qf->runtimedata->auto_resize)
		qf_set_auto_resize(&new_qf, true);

	// copy keys from qf into new_qf
	QFi qfi;
	qf_iterator_from_position(qf, &qfi, 0);
	int64_t ret_numkeys = 0;
	do {
		uint64_t key, value, count;
		qfi_get_hash(&qfi, &key, &value, &count);
		qfi_next(&qfi);
		int ret = qf_insert(&new_qf, key, value, count, QF_NO_LOCK | QF_KEY_IS_HASH);
		if (ret < 0) {
			fprintf(stderr, "Failed to insert key: %ld into the new CQF.\n", key);
			return ret;
		}
		ret_numkeys++;
	} while(!qfi_end(&qfi));

	// Copy old QF path in temp.
	char *path = (char *)malloc(strlen(qf->runtimedata->f_info.filepath) + 1);
	if (qf->runtimedata->f_info.filepath == NULL) {
		perror("Couldn't allocate memory for runtime f_info filepath.");
		exit(EXIT_FAILURE);
	}
	strcpy(path, qf->runtimedata->f_info.filepath);

	// delete old QF
	qf_deletefile(qf);
	memcpy(qf, &new_qf, sizeof(QF));

	rename(qf->runtimedata->f_info.filepath, path);
	strcpy(qf->runtimedata->f_info.filepath, path);

	return ret_numkeys;
}

bool qf_closefile(QF* qf)
{
	assert(qf->metadata != NULL);
	int fd = qf->runtimedata->f_info.fd;
	qf_sync_counters(qf);
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
	char *path = (char *)malloc(strlen(qf->runtimedata->f_info.filepath) + 1);
	if (qf->runtimedata->f_info.filepath == NULL) {
		perror("Couldn't allocate memory for runtime f_info filepath.");
		exit(EXIT_FAILURE);
	}
	strcpy(path, qf->runtimedata->f_info.filepath);
	if (qf_closefile(qf)) {
		remove(path);
		free(path);
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
	qf_sync_counters(qf);
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
	if (qf->metadata == NULL) {
		perror("Couldn't allocate memory for metadata.");
		exit(EXIT_FAILURE);
	}
	int ret = fread(qf->metadata, sizeof(qfmetadata), 1, fin);
	if (ret < 1) {
		perror("Couldn't read metadata from file.");
		exit(EXIT_FAILURE);
	}
	if (qf->metadata->magic_endian_number != MAGIC_NUMBER) {
		fprintf(stderr, "Can't read the CQF. It was written on a different endian machine.");
		exit(EXIT_FAILURE);
	}

	qf->runtimedata->f_info.filepath = (char *)malloc(strlen(filename) + 1);
	if (qf->runtimedata->f_info.filepath == NULL) {
		perror("Couldn't allocate memory for runtime f_info filepath.");
		exit(EXIT_FAILURE);
	}
	strcpy(qf->runtimedata->f_info.filepath, filename);
	/* initlialize the locks in the QF */
	qf->runtimedata->num_locks = (qf->metadata->xnslots/NUM_SLOTS_TO_LOCK)+2;
	qf->runtimedata->metadata_lock = 0;
	/* initialize all the locks to 0 */
	qf->runtimedata->locks = (volatile int *)calloc(qf->runtimedata->num_locks,
																									sizeof(volatile int));
	if (qf->runtimedata->locks == NULL) {
		perror("Couldn't allocate memory for runtime locks.");
		exit(EXIT_FAILURE);
	}
	qf->metadata = (qfmetadata *)realloc(qf->metadata,
																			 qf->metadata->total_size_in_bytes +
																			 sizeof(qfmetadata));
	if (qf->metadata == NULL) {
		perror("Couldn't allocate memory for metadata.");
		exit(EXIT_FAILURE);
	}
	qf->blocks = (qfblock *)(qf->metadata + 1);
	if (qf->blocks == NULL) {
		perror("Couldn't allocate memory for blocks.");
		exit(EXIT_FAILURE);
	}
	ret = fread(qf->blocks, qf->metadata->total_size_in_bytes, 1, fin);
	if (ret < 1) {
		perror("Couldn't read metadata from file.");
		exit(EXIT_FAILURE);
	}
	fclose(fin);

	pc_init(&qf->runtimedata->pc_nelts, (int64_t*)&qf->metadata->nelts, 8, 100);
	pc_init(&qf->runtimedata->pc_ndistinct_elts, (int64_t*)&qf->metadata->ndistinct_elts, 8, 100);
	pc_init(&qf->runtimedata->pc_noccupied_slots, (int64_t*)&qf->metadata->noccupied_slots, 8, 100);

	return sizeof(qfmetadata) + qf->metadata->total_size_in_bytes;
}

#define MADVISE_GRANULARITY (32)
#define ROUND_TO_PAGE_GROUP(p) ((char *)(((intptr_t)(p)) - (((intptr_t)(p)) % (page_size * MADVISE_GRANULARITY))))

static void make_madvise_calls(const QF *qf, uint64_t oldrun, uint64_t newrun)
{
  int page_size = sysconf(_SC_PAGESIZE);

  char * oldblock = (char *)get_block(qf, oldrun / QF_SLOTS_PER_BLOCK);
  char * newblock = (char *)get_block(qf, newrun / QF_SLOTS_PER_BLOCK);

  oldblock = ROUND_TO_PAGE_GROUP(oldblock);
  newblock = ROUND_TO_PAGE_GROUP(newblock);

  if (oldblock < (char *)qf->blocks)
    return;
  
  while (oldblock < newblock) {
    madvise(oldblock, page_size * MADVISE_GRANULARITY, MADV_DONTNEED);
    oldblock += page_size * MADVISE_GRANULARITY;
  }
}

/* This wraps qfi_next, using madvise(DONTNEED) to reduce our RSS.
   Only valid on mmapped QFs, i.e. cqfs from qf_initfile and
   qf_usefile. */
int qfi_next_madvise(QFi *qfi)
{
  uint64_t oldrun = qfi->run;
  int      result = qfi_next(qfi);
  uint64_t newrun = qfi->run;

  make_madvise_calls(qfi->qf, oldrun, newrun);
  
  return result;
}

/* Furthermore, you can call this immediately after constructing the
   qfi to call madvise(DONTNEED) on the portion of the cqf up to the
   first element visited by the qfi. */
int qfi_initial_madvise(QFi *qfi)
{
  make_madvise_calls(qfi->qf, 0, qfi->run);
  return 0;
}
