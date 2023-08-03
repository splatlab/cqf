/*
 * ============================================================================
 *
 *        Authors:  Prashant Pandey <ppandey@cs.stonybrook.edu>
 *                  Rob Johnson <robj@vmware.com>
 *                  Rob Johnson <robj@vmware.com>
 *
 * ============================================================================
 */

#include <assert.h>
#include <fcntl.h>
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <time.h>
#include <unistd.h>

#include "gqf.h"
#include "rhm.h"
#include "trhm.h"
#include "gqf_int.h"
#include "hashutil.h"
#include "hashutil.h"

/******************************************************************
 * Code for managing the metadata bits and slots w/o interpreting *
 * the content of the slots.
 ******************************************************************/

#define MIN(X, Y) (((X) < (Y)) ? (X) : (Y))
#define MAX(X, Y) (((X) > (Y)) ? (X) : (Y))
#define MAX_VALUE(nbits) ((1ULL << (nbits)) - 1)
#define BITMASK(nbits) ((nbits) == 64 ? 0xffffffffffffffff : MAX_VALUE(nbits))
#define NUM_SLOTS_TO_LOCK (1ULL << 16)
#define CLUSTER_SIZE (1ULL << 14)
#define METADATA_WORD(qf, field, slot_index)                                   \
  (get_block((qf), (slot_index) / QF_SLOTS_PER_BLOCK)                          \
       ->field[((slot_index) % QF_SLOTS_PER_BLOCK) / 64])
#define SET_O(qf, index)                                                       \
  (METADATA_WORD((qf), occupieds, (index)) |=                                  \
   1ULL << ((index) % QF_SLOTS_PER_BLOCK))
#define SET_R(qf, index)                                                       \
  (METADATA_WORD((qf), runends, (index)) |= 1ULL                               \
                                            << ((index) % QF_SLOTS_PER_BLOCK))
#define SET_T(qf, index)                                                       \
  (METADATA_WORD((qf), tombstones, (index)) |=                                 \
   1ULL << ((index) % QF_SLOTS_PER_BLOCK))
#define RESET_O(qf, index)                                                     \
  (METADATA_WORD((qf), occupieds, (index)) &=                                  \
   ~(1ULL << ((index) % QF_SLOTS_PER_BLOCK)))
#define RESET_R(qf, index)                                                     \
  (METADATA_WORD((qf), runends, (index)) &=                                    \
   ~(1ULL << ((index) % QF_SLOTS_PER_BLOCK)))
#define RESET_T(qf, index)                                                     \
  (METADATA_WORD((qf), tombstones, (index)) &=                                 \
   ~(1ULL << ((index) % QF_SLOTS_PER_BLOCK)))
#define GET_NO_LOCK(flag) (flag & QF_NO_LOCK)
#define GET_TRY_ONCE_LOCK(flag) (flag & QF_TRY_ONCE_LOCK)
#define GET_WAIT_FOR_LOCK(flag) (flag & QF_WAIT_FOR_LOCK)
#define GET_KEY_HASH(flag) (flag & QF_KEY_IS_HASH)

#define DISTANCE_FROM_HOME_SLOT_CUTOFF 1000
#define BILLION 1000000000L

#ifdef DEBUG
#define PRINT_DEBUG 1
#else
#define PRINT_DEBUG 0
#endif

#define DEBUG_CQF(fmt, ...)                                                    \
  do {                                                                         \
    if (PRINT_DEBUG)                                                           \
      fprintf(stderr, fmt, __VA_ARGS__);                                       \
  } while (0)
#define DEBUG_CQF(fmt, ...)                                                    \
  do {                                                                         \
    if (PRINT_DEBUG)                                                           \
      fprintf(stderr, fmt, __VA_ARGS__);                                       \
  } while (0)

#define DEBUG_DUMP(qf)                                                         \
  do {                                                                         \
    if (PRINT_DEBUG)                                                           \
      qf_dump_metadata(qf);                                                    \
  } while (0)
#define DEBUG_DUMP(qf)                                                         \
  do {                                                                         \
    if (PRINT_DEBUG)                                                           \
      qf_dump_metadata(qf);                                                    \
  } while (0)

static __inline__ unsigned long long rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

#ifdef LOG_WAIT_TIME
static inline bool qf_spin_lock(QF *qf, volatile int *lock, uint64_t idx,
                                uint8_t flag) {
  struct timespec start, end;
  bool ret;

  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start);
  if (GET_WAIT_FOR_LOCK(flag) != QF_WAIT_FOR_LOCK) {
    ret = !__sync_lock_test_and_set(lock, 1);
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end);
    qf->runtimedata->wait_times[idx].locks_acquired_single_attempt++;
    qf->runtimedata->wait_times[idx].total_time_single +=
        BILLION * (end.tv_sec - start.tv_sec) + end.tv_nsec - start.tv_nsec;
  } else {
    if (!__sync_lock_test_and_set(lock, 1)) {
      clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end);
      qf->runtimedata->wait_times[idx].locks_acquired_single_attempt++;
      qf->runtimedata->wait_times[idx].total_time_single +=
          BILLION * (end.tv_sec - start.tv_sec) + end.tv_nsec - start.tv_nsec;
    } else {
      while (__sync_lock_test_and_set(lock, 1))
        while (*lock)
          ;
      clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end);
      qf->runtimedata->wait_times[idx].total_time_spinning +=
          BILLION * (end.tv_sec - start.tv_sec) + end.tv_nsec - start.tv_nsec;
    }
    ret = true;
  }
  qf->runtimedata->wait_times[idx].locks_taken++;

  return ret;

  /*start = rdtsc();*/
  /*if (!__sync_lock_test_and_set(lock, 1)) {*/
  /*clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end);*/
  /*qf->runtimedata->wait_times[idx].locks_acquired_single_attempt++;*/
  /*qf->runtimedata->wait_times[idx].total_time_single += BILLION * (end.tv_sec
   * - start.tv_sec) + end.tv_nsec - start.tv_nsec;*/
  /*} else {*/
  /*while (__sync_lock_test_and_set(lock, 1))*/
  /*while (*lock);*/
  /*clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end);*/
  /*qf->runtimedata->wait_times[idx].total_time_spinning += BILLION *
   * (end.tv_sec - start.tv_sec) + end.tv_nsec - start.tv_nsec;*/
  /*}*/

  /*end = rdtsc();*/
  /*qf->runtimedata->wait_times[idx].locks_taken++;*/
  /*return;*/
}
#else
/**
 * Try to acquire a lock once and return even if the lock is busy.
 * If spin flag is set, then spin until the lock is available.
 */
static inline bool qf_spin_lock(volatile int *lock, uint8_t flag) {
  if (GET_WAIT_FOR_LOCK(flag) != QF_WAIT_FOR_LOCK) {
    return !__sync_lock_test_and_set(lock, 1);
  } else {
    while (__sync_lock_test_and_set(lock, 1))
      while (*lock)
        ;
    return true;
  }

  return false;
}
#endif

static inline void qf_spin_unlock(volatile int *lock) {
  __sync_lock_release(lock);
  return;
}

static bool qf_lock(QF *qf, uint64_t hash_bucket_index, bool small,
                    uint8_t runtime_lock) {
  uint64_t hash_bucket_lock_offset = hash_bucket_index % NUM_SLOTS_TO_LOCK;
  if (small) {
#ifdef LOG_WAIT_TIME
    if (!qf_spin_lock(
            qf, &qf->runtimedata->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK],
            hash_bucket_index / NUM_SLOTS_TO_LOCK, runtime_lock))
      return false;
    if (NUM_SLOTS_TO_LOCK - hash_bucket_lock_offset <= CLUSTER_SIZE) {
      if (!qf_spin_lock(qf,
                        &qf->runtimedata
                             ->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK + 1],
                        hash_bucket_index / NUM_SLOTS_TO_LOCK + 1,
                        runtime_lock)) {
        qf_spin_unlock(
            &qf->runtimedata->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK]);
        return false;
      }
    }
#else
    if (!qf_spin_lock(
            &qf->runtimedata->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK],
            runtime_lock))
      return false;
    if (NUM_SLOTS_TO_LOCK - hash_bucket_lock_offset <= CLUSTER_SIZE) {
      if (!qf_spin_lock(&qf->runtimedata
                             ->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK + 1],
                        runtime_lock)) {
        qf_spin_unlock(
            &qf->runtimedata->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK]);
        return false;
      }
    }
#endif
  } else {
#ifdef LOG_WAIT_TIME
    if (hash_bucket_index >= NUM_SLOTS_TO_LOCK &&
        hash_bucket_lock_offset <= CLUSTER_SIZE) {
      if (!qf_spin_lock(qf,
                        &qf->runtimedata
                             ->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK - 1],
                        runtime_lock))
        return false;
    }
    if (!qf_spin_lock(
            qf, &qf->runtimedata->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK],
            runtime_lock)) {
      if (hash_bucket_index >= NUM_SLOTS_TO_LOCK &&
          hash_bucket_lock_offset <= CLUSTER_SIZE)
        qf_spin_unlock(
            &qf->runtimedata->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK - 1]);
      return false;
    }
    if (!qf_spin_lock(
            qf,
            &qf->runtimedata->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK + 1],
            runtime_lock)) {
      qf_spin_unlock(
          &qf->runtimedata->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK]);
      if (hash_bucket_index >= NUM_SLOTS_TO_LOCK &&
          hash_bucket_lock_offset <= CLUSTER_SIZE)
        qf_spin_unlock(
            &qf->runtimedata->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK - 1]);
      return false;
    }
#else
    if (hash_bucket_index >= NUM_SLOTS_TO_LOCK &&
        hash_bucket_lock_offset <= CLUSTER_SIZE) {
      if (!qf_spin_lock(&qf->runtimedata
                             ->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK - 1],
                        runtime_lock))
        return false;
    }
    if (!qf_spin_lock(
            &qf->runtimedata->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK],
            runtime_lock)) {
      if (hash_bucket_index >= NUM_SLOTS_TO_LOCK &&
          hash_bucket_lock_offset <= CLUSTER_SIZE)
        qf_spin_unlock(
            &qf->runtimedata->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK - 1]);
      return false;
    }
    if (!qf_spin_lock(
            &qf->runtimedata->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK + 1],
            runtime_lock)) {
      qf_spin_unlock(
          &qf->runtimedata->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK]);
      if (hash_bucket_index >= NUM_SLOTS_TO_LOCK &&
          hash_bucket_lock_offset <= CLUSTER_SIZE)
        qf_spin_unlock(
            &qf->runtimedata->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK - 1]);
      return false;
    }
#endif
  }
  return true;
}

static void qf_unlock(QF *qf, uint64_t hash_bucket_index, bool small) {
  uint64_t hash_bucket_lock_offset = hash_bucket_index % NUM_SLOTS_TO_LOCK;
  if (small) {
    if (NUM_SLOTS_TO_LOCK - hash_bucket_lock_offset <= CLUSTER_SIZE) {
      qf_spin_unlock(
          &qf->runtimedata->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK + 1]);
    }
    qf_spin_unlock(
        &qf->runtimedata->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK]);
  } else {
    qf_spin_unlock(
        &qf->runtimedata->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK + 1]);
    qf_spin_unlock(
        &qf->runtimedata->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK]);
    if (hash_bucket_index >= NUM_SLOTS_TO_LOCK &&
        hash_bucket_lock_offset <= CLUSTER_SIZE)
      qf_spin_unlock(
          &qf->runtimedata->locks[hash_bucket_index / NUM_SLOTS_TO_LOCK - 1]);
  }
}

/*static void modify_metadata(QF *qf, uint64_t *metadata, int cnt)*/
/*{*/
/*#ifdef LOG_WAIT_TIME*/
/*qf_spin_lock(qf, &qf->runtimedata->metadata_lock,*/
/*qf->runtimedata->num_locks, QF_WAIT_FOR_LOCK);*/
/*#else*/
/*qf_spin_lock(&qf->runtimedata->metadata_lock, QF_WAIT_FOR_LOCK);*/
/*#endif*/
/**metadata = *metadata + cnt;*/
/*qf_spin_unlock(&qf->runtimedata->metadata_lock);*/
/*return;*/
/*}*/

/* Increase the metadata by cnt.*/
static void modify_metadata(pc_t *metadata, int cnt) {
  pc_add(metadata, cnt);
  return;
}

static inline int popcnt(uint64_t val) {
  asm("popcnt %[val], %[val]" : [val] "+r"(val) : : "cc");
  return val;
}

static inline int64_t bitscanreverse(uint64_t val) {
  if (val == 0) {
    return -1;
  } else {
    asm("bsr %[val], %[val]" : [val] "+r"(val) : : "cc");
    return val;
  }
}

static inline int popcntv(const uint64_t val, int ignore) {
  if (ignore % 64)
    return popcnt(val & ~BITMASK(ignore % 64));
  else
    return popcnt(val);
}

// Returns the number of 1s up to (and including) the pos'th bit
// Bits are numbered from 0
static inline int bitrank(uint64_t val, int pos) {
  val = val & ((2ULL << pos) - 1);
  asm("popcnt %[val], %[val]" : [val] "+r"(val) : : "cc");
  return val;
}

/**
 * Returns the position of the k-th 1 in the 64-bit word x.
 * k is 0-based, so k=0 returns the position of the first 1.
 *
 * Uses the broadword selection algorithm by Vigna [1], improved by Gog
 * and Petri [2] and Vigna [3].
 *
 * [1] Sebastiano Vigna. Broadword Implementation of Rank/Select
 *    Queries. WEA, 2008
 *
 * [2] Simon Gog, Matthias Petri. Optimized succinct data
 * structures for massive data. Softw. Pract. Exper., 2014
 *
 * [3] Sebastiano Vigna. MG4J 5.2.1. http://mg4j.di.unimi.it/
 * The following code is taken from
 * https://github.com/facebook/folly/blob/b28186247104f8b90cfbe094d289c91f9e413317/folly/experimental/Select64.h
 */
const uint8_t kSelectInByte[2048] = {
    8, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3,
    0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0,
    1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1,
    0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0,
    2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2,
    0, 1, 0, 7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0,
    1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1,
    0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0,
    3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5,
    0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0,
    1, 0, 2, 0, 1, 0, 8, 8, 8, 1, 8, 2, 2, 1, 8, 3, 3, 1, 3, 2, 2, 1, 8, 4, 4,
    1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1, 8, 5, 5, 1, 5, 2, 2, 1, 5, 3, 3, 1,
    3, 2, 2, 1, 5, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1, 8, 6, 6, 1, 6,
    2, 2, 1, 6, 3, 3, 1, 3, 2, 2, 1, 6, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2,
    2, 1, 6, 5, 5, 1, 5, 2, 2, 1, 5, 3, 3, 1, 3, 2, 2, 1, 5, 4, 4, 1, 4, 2, 2,
    1, 4, 3, 3, 1, 3, 2, 2, 1, 8, 7, 7, 1, 7, 2, 2, 1, 7, 3, 3, 1, 3, 2, 2, 1,
    7, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1, 7, 5, 5, 1, 5, 2, 2, 1, 5,
    3, 3, 1, 3, 2, 2, 1, 5, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1, 7, 6,
    6, 1, 6, 2, 2, 1, 6, 3, 3, 1, 3, 2, 2, 1, 6, 4, 4, 1, 4, 2, 2, 1, 4, 3, 3,
    1, 3, 2, 2, 1, 6, 5, 5, 1, 5, 2, 2, 1, 5, 3, 3, 1, 3, 2, 2, 1, 5, 4, 4, 1,
    4, 2, 2, 1, 4, 3, 3, 1, 3, 2, 2, 1, 8, 8, 8, 8, 8, 8, 8, 2, 8, 8, 8, 3, 8,
    3, 3, 2, 8, 8, 8, 4, 8, 4, 4, 2, 8, 4, 4, 3, 4, 3, 3, 2, 8, 8, 8, 5, 8, 5,
    5, 2, 8, 5, 5, 3, 5, 3, 3, 2, 8, 5, 5, 4, 5, 4, 4, 2, 5, 4, 4, 3, 4, 3, 3,
    2, 8, 8, 8, 6, 8, 6, 6, 2, 8, 6, 6, 3, 6, 3, 3, 2, 8, 6, 6, 4, 6, 4, 4, 2,
    6, 4, 4, 3, 4, 3, 3, 2, 8, 6, 6, 5, 6, 5, 5, 2, 6, 5, 5, 3, 5, 3, 3, 2, 6,
    5, 5, 4, 5, 4, 4, 2, 5, 4, 4, 3, 4, 3, 3, 2, 8, 8, 8, 7, 8, 7, 7, 2, 8, 7,
    7, 3, 7, 3, 3, 2, 8, 7, 7, 4, 7, 4, 4, 2, 7, 4, 4, 3, 4, 3, 3, 2, 8, 7, 7,
    5, 7, 5, 5, 2, 7, 5, 5, 3, 5, 3, 3, 2, 7, 5, 5, 4, 5, 4, 4, 2, 5, 4, 4, 3,
    4, 3, 3, 2, 8, 7, 7, 6, 7, 6, 6, 2, 7, 6, 6, 3, 6, 3, 3, 2, 7, 6, 6, 4, 6,
    4, 4, 2, 6, 4, 4, 3, 4, 3, 3, 2, 7, 6, 6, 5, 6, 5, 5, 2, 6, 5, 5, 3, 5, 3,
    3, 2, 6, 5, 5, 4, 5, 4, 4, 2, 5, 4, 4, 3, 4, 3, 3, 2, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 3, 8, 8, 8, 8, 8, 8, 8, 4, 8, 8, 8, 4, 8, 4, 4, 3,
    8, 8, 8, 8, 8, 8, 8, 5, 8, 8, 8, 5, 8, 5, 5, 3, 8, 8, 8, 5, 8, 5, 5, 4, 8,
    5, 5, 4, 5, 4, 4, 3, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 6, 8, 6, 6, 3, 8, 8,
    8, 6, 8, 6, 6, 4, 8, 6, 6, 4, 6, 4, 4, 3, 8, 8, 8, 6, 8, 6, 6, 5, 8, 6, 6,
    5, 6, 5, 5, 3, 8, 6, 6, 5, 6, 5, 5, 4, 6, 5, 5, 4, 5, 4, 4, 3, 8, 8, 8, 8,
    8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 3, 8, 8, 8, 7, 8, 7, 7, 4, 8, 7, 7, 4, 7,
    4, 4, 3, 8, 8, 8, 7, 8, 7, 7, 5, 8, 7, 7, 5, 7, 5, 5, 3, 8, 7, 7, 5, 7, 5,
    5, 4, 7, 5, 5, 4, 5, 4, 4, 3, 8, 8, 8, 7, 8, 7, 7, 6, 8, 7, 7, 6, 7, 6, 6,
    3, 8, 7, 7, 6, 7, 6, 6, 4, 7, 6, 6, 4, 6, 4, 4, 3, 8, 7, 7, 6, 7, 6, 6, 5,
    7, 6, 6, 5, 6, 5, 5, 3, 7, 6, 6, 5, 6, 5, 5, 4, 6, 5, 5, 4, 5, 4, 4, 3, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 4, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 5, 8, 8, 8,
    8, 8, 8, 8, 5, 8, 8, 8, 5, 8, 5, 5, 4, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 6, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 6, 8, 6, 6, 4, 8, 8, 8, 8, 8,
    8, 8, 6, 8, 8, 8, 6, 8, 6, 6, 5, 8, 8, 8, 6, 8, 6, 6, 5, 8, 6, 6, 5, 6, 5,
    5, 4, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8,
    7, 8, 8, 8, 7, 8, 7, 7, 4, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 5,
    8, 8, 8, 7, 8, 7, 7, 5, 8, 7, 7, 5, 7, 5, 5, 4, 8, 8, 8, 8, 8, 8, 8, 7, 8,
    8, 8, 7, 8, 7, 7, 6, 8, 8, 8, 7, 8, 7, 7, 6, 8, 7, 7, 6, 7, 6, 6, 4, 8, 8,
    8, 7, 8, 7, 7, 6, 8, 7, 7, 6, 7, 6, 6, 5, 8, 7, 7, 6, 7, 6, 6, 5, 7, 6, 6,
    5, 6, 5, 5, 4, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 5, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    6, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 8, 8, 8, 8, 6,
    8, 8, 8, 6, 8, 6, 6, 5, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 5, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7,
    8, 7, 7, 6, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 6, 8, 8, 8, 7, 8,
    7, 7, 6, 8, 7, 7, 6, 7, 6, 6, 5, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 6, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7};

static inline uint64_t _select64(uint64_t x, int k) {
  if (k >= popcnt(x)) {
    return 64;
  }

  const uint64_t kOnesStep4 = 0x1111111111111111ULL;
  const uint64_t kOnesStep8 = 0x0101010101010101ULL;
  const uint64_t kMSBsStep8 = 0x80ULL * kOnesStep8;

  uint64_t s = x;
  s = s - ((s & 0xA * kOnesStep4) >> 1);
  s = (s & 0x3 * kOnesStep4) + ((s >> 2) & 0x3 * kOnesStep4);
  s = (s + (s >> 4)) & 0xF * kOnesStep8;
  uint64_t byteSums = s * kOnesStep8;

  uint64_t kStep8 = k * kOnesStep8;
  uint64_t geqKStep8 = (((kStep8 | kMSBsStep8) - byteSums) & kMSBsStep8);
  uint64_t place = popcnt(geqKStep8) * 8;
  uint64_t byteRank = k - (((byteSums << 8) >> place) & (uint64_t)(0xFF));
  return place + kSelectInByte[((x >> place) & 0xFF) | (byteRank << 8)];
}

// Returns the position of the rank'th 1.  (rank = 0 returns the 1st 1)
// Returns 64 if there are fewer than rank+1 1s.
// Little-endian code, rank from right to left.
static inline uint64_t bitselect(uint64_t val, int rank) {
#ifdef __SSE4_2_
  uint64_t i = 1ULL << rank;
  asm("pdep %[val], %[mask], %[val]" : [val] "+r"(val) : [mask] "r"(i));
  asm("tzcnt %[bit], %[index]" : [index] "=r"(i) : [bit] "g"(val) : "cc");
  return i;
#endif
  return _select64(val, rank);
}

// Returns the position of the rank'th 1 from right, ignoring the first
// ignore 1.
static inline uint64_t bitselectv(const uint64_t val, int ignore, int rank) {
  return bitselect(val & ~BITMASK(ignore % 64), rank);
}

static inline int is_runend(const QF *qf, uint64_t index) {
  return (METADATA_WORD(qf, runends, index) >>
          ((index % QF_SLOTS_PER_BLOCK) % 64)) &
         1ULL;
}

static inline int is_occupied(const QF *qf, uint64_t index) {
  return (METADATA_WORD(qf, occupieds, index) >>
          ((index % QF_SLOTS_PER_BLOCK) % 64)) &
         1ULL;
}

static inline int is_tombstone(const QF *qf, uint64_t index) {
  return (METADATA_WORD(qf, tombstones, index) >>
          ((index % QF_SLOTS_PER_BLOCK) % 64)) &
         1ULL;
}

#if QF_BITS_PER_SLOT == 8 || QF_BITS_PER_SLOT == 16 ||                         \
    QF_BITS_PER_SLOT == 32 || QF_BITS_PER_SLOT == 64

static inline uint64_t get_slot(const QF *qf, uint64_t index) {
  assert(index < qf->metadata->xnslots);
  return get_block(qf, index / QF_SLOTS_PER_BLOCK)
      ->slots[index % QF_SLOTS_PER_BLOCK];
}

static inline void set_slot(const QF *qf, uint64_t index, uint64_t value) {
  assert(index < qf->metadata->xnslots);
  get_block(qf, index / QF_SLOTS_PER_BLOCK)->slots[index % QF_SLOTS_PER_BLOCK] =
      value & BITMASK(qf->metadata->bits_per_slot);
}

#elif QF_BITS_PER_SLOT > 0

/* Little-endian code ....  Big-endian is TODO */

static inline uint64_t get_slot(const QF *qf, uint64_t index) {
  /* Should use __uint128_t to support up to 64-bit remainders, but gcc seems
   * to generate buggy code.  :/  */
  assert(index < qf->metadata->xnslots);
  uint64_t *p =
      (uint64_t *)&get_block(qf, index / QF_SLOTS_PER_BLOCK)
          ->slots[(index % QF_SLOTS_PER_BLOCK) * QF_BITS_PER_SLOT / 8];
  return (uint64_t)(((*p) >>
                     (((index % QF_SLOTS_PER_BLOCK) * QF_BITS_PER_SLOT) % 8)) &
                    BITMASK(QF_BITS_PER_SLOT));
}

static inline void set_slot(const QF *qf, uint64_t index, uint64_t value) {
  /* Should use __uint128_t to support up to 64-bit remainders, but gcc seems
   * to generate buggy code.  :/  */
  assert(index < qf->metadata->xnslots);
  uint64_t *p =
      (uint64_t *)&get_block(qf, index / QF_SLOTS_PER_BLOCK)
          ->slots[(index % QF_SLOTS_PER_BLOCK) * QF_BITS_PER_SLOT / 8];
  uint64_t t = *p;
  uint64_t mask = BITMASK(QF_BITS_PER_SLOT);
  uint64_t v = value;
  int shift = ((index % QF_SLOTS_PER_BLOCK) * QF_BITS_PER_SLOT) % 8;
  mask <<= shift;
  v <<= shift;
  t &= ~mask;
  t |= v;
  *p = t;
}

#else

/* Little-endian code ....  Big-endian is TODO */

static inline uint64_t get_slot(const QF *qf, uint64_t index) {
  assert(index < qf->metadata->xnslots);
  /* Should use __uint128_t to support up to 64-bit remainders, but gcc seems
   * to generate buggy code.  :/  */
  uint64_t *p = (uint64_t *)&get_block(qf, index / QF_SLOTS_PER_BLOCK)
                    ->slots[(index % QF_SLOTS_PER_BLOCK) *
                            qf->metadata->bits_per_slot / 8];
  return (uint64_t)(((*p) >> (((index % QF_SLOTS_PER_BLOCK) *
                               qf->metadata->bits_per_slot) %
                              8)) &
                    BITMASK(qf->metadata->bits_per_slot));
}

static inline uint64_t get_slot_remainder(const QF *qf, uint64_t index) {
  return get_slot(qf, index )>> (qf->metadata->value_bits);
}

static inline void set_slot(const QF *qf, uint64_t index, uint64_t value) {
  assert(index < qf->metadata->xnslots);
  /* Should use __uint128_t to support up to 64-bit remainders, but gcc seems
   * to generate buggy code.  :/  */
  uint64_t *p = (uint64_t *)&get_block(qf, index / QF_SLOTS_PER_BLOCK)
                    ->slots[(index % QF_SLOTS_PER_BLOCK) *
                            qf->metadata->bits_per_slot / 8];
  uint64_t t = *p;
  uint64_t mask = BITMASK(qf->metadata->bits_per_slot);
  uint64_t v = value;
  int shift = ((index % QF_SLOTS_PER_BLOCK) * qf->metadata->bits_per_slot) % 8;
  mask <<= shift;
  v <<= shift;
  t &= ~mask;
  t |= v;
  *p = t;
}

#endif

static inline uint64_t run_end(const QF *qf, uint64_t hash_bucket_index);

static inline uint64_t block_offset(const QF *qf, uint64_t blockidx) {
  /* If we have extended counters and a 16-bit (or larger) offset
           field, then we can safely ignore the possibility of overflowing
           that field. */
  if (sizeof(qf->blocks[0].offset) > 1 ||
      get_block(qf, blockidx)->offset <
          BITMASK(8 * sizeof(qf->blocks[0].offset)))
    return get_block(qf, blockidx)->offset;

  return run_end(qf, QF_SLOTS_PER_BLOCK * blockidx - 1) -
         QF_SLOTS_PER_BLOCK * blockidx + 1;
}

/* Return the end index of a run if the run exists */
static inline uint64_t run_end(const QF *qf, uint64_t hash_bucket_index) {
  uint64_t bucket_block_index = hash_bucket_index / QF_SLOTS_PER_BLOCK;
  uint64_t bucket_intrablock_offset = hash_bucket_index % QF_SLOTS_PER_BLOCK;
  uint64_t bucket_blocks_offset = block_offset(qf, bucket_block_index);

  uint64_t bucket_intrablock_rank =
      bitrank(get_block(qf, bucket_block_index)->occupieds[0],
              bucket_intrablock_offset);

  if (bucket_intrablock_rank == 0) {
    if (bucket_blocks_offset <= bucket_intrablock_offset)
      return hash_bucket_index;
    else
      return QF_SLOTS_PER_BLOCK * bucket_block_index + bucket_blocks_offset - 1;
  }

  uint64_t runend_block_index =
      bucket_block_index + bucket_blocks_offset / QF_SLOTS_PER_BLOCK;
  uint64_t runend_ignore_bits = bucket_blocks_offset % QF_SLOTS_PER_BLOCK;
  uint64_t runend_rank = bucket_intrablock_rank - 1;
  uint64_t runend_block_offset =
      bitselectv(get_block(qf, runend_block_index)->runends[0],
                 runend_ignore_bits, runend_rank);
  if (runend_block_offset == QF_SLOTS_PER_BLOCK) {
    if (bucket_blocks_offset == 0 && bucket_intrablock_rank == 0) {
      /* The block begins in empty space, and this bucket is in that region of
       * empty space */
      return hash_bucket_index;
    } else {
      do {
        runend_rank -= popcntv(get_block(qf, runend_block_index)->runends[0],
                               runend_ignore_bits);
        runend_block_index++;
        runend_ignore_bits = 0;
        runend_block_offset =
            bitselectv(get_block(qf, runend_block_index)->runends[0],
                       runend_ignore_bits, runend_rank);
      } while (runend_block_offset == QF_SLOTS_PER_BLOCK);
    }
  }

  uint64_t runend_index =
      QF_SLOTS_PER_BLOCK * runend_block_index + runend_block_offset;
  if (runend_index < hash_bucket_index)
    return hash_bucket_index;
  else
    return runend_index;
}

/* Return n_occupieds in [0, slot_index] minus n_runends in [0, slot_index) */
static inline int offset_lower_bound(const QF *qf, uint64_t slot_index) {
  const qfblock *b = get_block(qf, slot_index / QF_SLOTS_PER_BLOCK);
  const uint64_t slot_offset = slot_index % QF_SLOTS_PER_BLOCK;
  const uint64_t boffset = b->offset;
  // Extract the slot_offset+1 right most bits of occupieds
  const uint64_t occupieds = b->occupieds[0] & BITMASK(slot_offset + 1);
  assert(QF_SLOTS_PER_BLOCK == 64);
  if (boffset <= slot_offset) {
    // Extract the slot_offset right most bits of occupieds
    const uint64_t runends = (b->runends[0] & BITMASK(slot_offset)) >> boffset;
    return popcnt(occupieds) - popcnt(runends);
  }
  return boffset - slot_offset + popcnt(occupieds);
}

static inline int is_empty(const QF *qf, uint64_t slot_index) {
  // TODO: Try to check if is tombstone first
  return offset_lower_bound(qf, slot_index) == 0;
}

static inline uint64_t find_first_empty_slot(QF *qf, uint64_t from, uint64_t *empty_slot) {
  do {
    int t = offset_lower_bound(qf, from);
    if (t < 0) {
      return -1;
    }
    if (t == 0)
      break;
    from = from + t;
  } while (1);
  *empty_slot = from;
  return 0;
}

/* Find the index of first tombstone, it can be empty or not empty. */
static inline int find_first_tombstone(QF *qf, uint64_t from, uint64_t * tombstone_index) {
  uint64_t block_index = from / QF_SLOTS_PER_BLOCK;
  qfblock *b = get_block(qf, block_index);
  const uint64_t slot_offset = from % QF_SLOTS_PER_BLOCK;
  uint64_t tomb_offset =
      bitselect(b->tombstones[0] & (~BITMASK(slot_offset)), 0);
  while (tomb_offset == 64) { // No tombstone in the rest of this block.
    block_index++;
    tomb_offset = bitselect(get_block(qf, block_index)->tombstones[0], 0);
  }
  *tombstone_index = block_index * QF_SLOTS_PER_BLOCK + tomb_offset;
  return 0;
}

// shift the part of b (bend, bstart] to the left by amount, little endian,
// index from right. if bstart == 0, shift left part of a into the right part of
// b. returns the result in b.
static inline uint64_t shift_into_b(const uint64_t a, const uint64_t b,
                                    const int bstart, const int bend,
                                    const int amount) {
  const uint64_t a_component = bstart == 0 ? (a >> (64 - amount)) : 0;
  const uint64_t b_shifted_mask = BITMASK(bend - bstart) << bstart;
  const uint64_t b_shifted = ((b_shifted_mask & b) << amount) & b_shifted_mask;
  const uint64_t b_mask = ~b_shifted_mask;
  return a_component | b_shifted | (b & b_mask);
}

#if QF_BITS_PER_SLOT == 8 || QF_BITS_PER_SLOT == 16 ||                         \
    QF_BITS_PER_SLOT == 32 || QF_BITS_PER_SLOT == 64

static inline void shift_remainders(QF *qf, uint64_t start_index,
                                    uint64_t empty_index) {
  uint64_t start_block = start_index / QF_SLOTS_PER_BLOCK;
  uint64_t start_offset = start_index % QF_SLOTS_PER_BLOCK;
  uint64_t empty_block = empty_index / QF_SLOTS_PER_BLOCK;
  uint64_t empty_offset = empty_index % QF_SLOTS_PER_BLOCK;

  assert(start_index <= empty_index && empty_index < qf->metadata->xnslots);

  while (start_block < empty_block) {
    memmove(&get_block(qf, empty_block)->slots[1],
            &get_block(qf, empty_block)->slots[0],
            empty_offset * sizeof(qf->blocks[0].slots[0]));
    get_block(qf, empty_block)->slots[0] =
        get_block(qf, empty_block - 1)->slots[QF_SLOTS_PER_BLOCK - 1];
    empty_block--;
    empty_offset = QF_SLOTS_PER_BLOCK - 1;
  }

  memmove(&get_block(qf, empty_block)->slots[start_offset + 1],
          &get_block(qf, empty_block)->slots[start_offset],
          (empty_offset - start_offset) * sizeof(qf->blocks[0].slots[0]));
}

#else

#define REMAINDER_WORD(qf, i)                                                  \
  ((uint64_t *)&(get_block(qf, (i) / qf->metadata->bits_per_slot)              \
                     ->slots[8 * ((i) % qf->metadata->bits_per_slot)]))

/* shift remainders in range [start_index, empty_index) by 1 to the right. */
static inline void shift_remainders(QF *qf, const uint64_t start_index,
                                    const uint64_t empty_index) {
  uint64_t last_word = (empty_index + 1) * qf->metadata->bits_per_slot / 64;
  const uint64_t first_word = start_index * qf->metadata->bits_per_slot / 64;
  int bend = ((empty_index + 1) * qf->metadata->bits_per_slot) % 64;
  const int bstart = (start_index * qf->metadata->bits_per_slot) % 64;

  while (last_word != first_word) {
    *REMAINDER_WORD(qf, last_word) = shift_into_b(
        *REMAINDER_WORD(qf, last_word - 1), *REMAINDER_WORD(qf, last_word), 0,
        bend, qf->metadata->bits_per_slot);
    last_word--;
    bend = 64;
  }
  *REMAINDER_WORD(qf, last_word) =
      shift_into_b(0, *REMAINDER_WORD(qf, last_word), bstart, bend,
                   qf->metadata->bits_per_slot);
}

#endif
static inline void qf_dump_block_long(const QF *qf, uint64_t i) {
  uint64_t j;

#if QF_BITS_PER_SLOT == 8 || QF_BITS_PER_SLOT == 16 || QF_BITS_PER_SLOT == 32
  for (j = 0; j < QF_SLOTS_PER_BLOCK; j++)
    printf("%02x ", get_block(qf, i)->slots[j]);
#elif QF_BITS_PER_SLOT == 64
  for (j = 0; j < QF_SLOTS_PER_BLOCK; j++)
    printf("%02lx ", get_block(qf, i)->slots[j]);
#else
printf("BL O R T V\n");
  for (j = 0; j < QF_SLOTS_PER_BLOCK;  j++) {
    printf("%02lx", j); // , get_block(qf, i)->slots[j]);
    printf(" %d",
           (get_block(qf, i)->occupieds[j / 64] & (1ULL << (j % 64))) ? 1 : 0);
    printf(" %d",
           (get_block(qf, i)->runends[j / 64] & (1ULL << (j % 64))) ? 1 : 0);
    printf(" %d ",
           (get_block(qf, i)->tombstones[j / 64] & (1ULL << (j % 64))) ? 1 : 0);
    uint64_t slot = i * QF_SLOTS_PER_BLOCK + j;
    if (slot < qf->metadata->xnslots) {
      printf("%" PRIx64, get_slot(qf, i*QF_SLOTS_PER_BLOCK + j));
    } else {
      printf("-");
    }
    printf("\n");
  }
}
#endif

static inline void qf_dump_block(const QF *qf, uint64_t i) {
  uint64_t j;

  printf("%-192d", get_block(qf, i)->offset);
  printf("\n");

  for (j = 0; j < QF_SLOTS_PER_BLOCK; j++)
    printf("%02lx ", j);
  printf("\n");

  for (j = 0; j < QF_SLOTS_PER_BLOCK; j++)
    printf(" %d ",
           (get_block(qf, i)->occupieds[j / 64] & (1ULL << (j % 64))) ? 1 : 0);
  printf("\n");

  for (j = 0; j < QF_SLOTS_PER_BLOCK; j++)
    printf(" %d ",
           (get_block(qf, i)->runends[j / 64] & (1ULL << (j % 64))) ? 1 : 0);
  printf("\n");

  for (j = 0; j < QF_SLOTS_PER_BLOCK; j++)
    printf(" %d ",
           (get_block(qf, i)->tombstones[j / 64] & (1ULL << (j % 64))) ? 1 : 0);
  printf("\n");

#if QF_BITS_PER_SLOT == 8 || QF_BITS_PER_SLOT == 16 || QF_BITS_PER_SLOT == 32
  for (j = 0; j < QF_SLOTS_PER_BLOCK; j++)
    printf("%02x ", get_block(qf, i)->slots[j]);
#elif QF_BITS_PER_SLOT == 64
  for (j = 0; j < QF_SLOTS_PER_BLOCK; j++)
    printf("%02lx ", get_block(qf, i)->slots[j]);
#else
  for (j = 0; j < QF_SLOTS_PER_BLOCK * qf->metadata->bits_per_slot / 8; j++)
    printf("%02x ", get_block(qf, i)->slots[j]);
#endif

  printf("\n");

  printf("\n");
}

void qf_dump_metadata(const QF *qf) {
  printf("Slots: %lu Occupied: %lu Elements: %lu\n", qf->metadata->nslots,
         qf->metadata->noccupied_slots, qf->metadata->nelts);
  printf(
      "Key_bits: %lu Value_bits: %lu Remainder_bits: %lu Bits_per_slot: %lu\n",
      qf->metadata->key_bits, qf->metadata->value_bits,
      qf->metadata->key_remainder_bits, qf->metadata->bits_per_slot);
}

void qf_dump(const QF *qf) {
  uint64_t i;

  printf("nblocks: %lu; nelts: %lu.\n", qf->metadata->nblocks,
         qf->metadata->nelts);

  for (i = 0; i < qf->metadata->nblocks; i++) {
    qf_dump_block(qf, i);
  }
}
void qf_dump_long(const QF *qf) {
  uint64_t i;

  printf("%lu %lu\n", qf->metadata->nblocks, qf->metadata->nelts);

  for (i = 0; i < qf->metadata->nblocks; i++) {
    qf_dump_block_long(qf, i);
  }
  for (i = 0; i < qf->metadata->nblocks; i++) {
    qf_dump_block(qf, i);
  }
}

static inline void shift_slots(QF *qf, int64_t first, uint64_t last,
                               uint64_t distance) {
  int64_t i;
  if (distance == 1)
    shift_remainders(qf, first, last + 1);
  else
    for (i = last; i >= first; i--)
      set_slot(qf, i + distance, get_slot(qf, i));
}

// RHM need this function to shift the runends without tombstones.
static inline void shift_runends(QF *qf, int64_t first, uint64_t last,
                                 uint64_t distance) {
  assert(last < qf->metadata->xnslots && distance < 64);
  uint64_t first_word = first / 64;
  uint64_t bstart = first % 64;
  uint64_t last_word = (last + distance + 1) / 64;
  uint64_t bend = (last + distance + 1) % 64;

  if (last_word != first_word) {
    METADATA_WORD(qf, runends, 64 * last_word) = shift_into_b(
        METADATA_WORD(qf, runends, 64 * (last_word - 1)),
        METADATA_WORD(qf, runends, 64 * last_word), 0, bend, distance);
    bend = 64;
    last_word--;
    while (last_word != first_word) {
      METADATA_WORD(qf, runends, 64 * last_word) = shift_into_b(
          METADATA_WORD(qf, runends, 64 * (last_word - 1)),
          METADATA_WORD(qf, runends, 64 * last_word), 0, bend, distance);
      last_word--;
    }
  }
  METADATA_WORD(qf, runends, 64 * last_word) = shift_into_b(
      0, METADATA_WORD(qf, runends, 64 * last_word), bstart, bend, distance);
}

// Shift metadata runends and tombstones to the big direction by distance.
// Fill with 0s in the small size.
// TODO: Refactor this function.
static inline void shift_runends_tombstones(QF *qf, int64_t first,
                                            uint64_t last, uint64_t distance) {
  assert(last < qf->metadata->xnslots);
  assert(distance < 64);
  uint64_t first_word = first / 64;
  uint64_t bstart = first % 64;
  uint64_t last_word = (last + distance - 1) / 64;
  uint64_t bend = (last + distance) % 64;

  if (last_word != first_word) {
    METADATA_WORD(qf, runends, 64 * last_word) = shift_into_b(
        METADATA_WORD(qf, runends, 64 * (last_word - 1)),
        METADATA_WORD(qf, runends, 64 * last_word), 0, bend, distance);
    METADATA_WORD(qf, tombstones, 64 * last_word) = shift_into_b(
        METADATA_WORD(qf, tombstones, 64 * (last_word - 1)),
        METADATA_WORD(qf, tombstones, 64 * last_word), 0, bend, distance);
    bend = 64;
    last_word--;
    while (last_word != first_word) {
      METADATA_WORD(qf, runends, 64 * last_word) = shift_into_b(
          METADATA_WORD(qf, runends, 64 * (last_word - 1)),
          METADATA_WORD(qf, runends, 64 * last_word), 0, bend, distance);
      METADATA_WORD(qf, tombstones, 64 * last_word) = shift_into_b(
          METADATA_WORD(qf, tombstones, 64 * (last_word - 1)),
          METADATA_WORD(qf, tombstones, 64 * last_word), 0, bend, distance);
      last_word--;
    }
  }
  METADATA_WORD(qf, runends, 64 * last_word) = shift_into_b(
      0, METADATA_WORD(qf, runends, 64 * last_word), bstart, bend, distance);
  METADATA_WORD(qf, tombstones, 64 * last_word) = shift_into_b(
      0, METADATA_WORD(qf, tombstones, 64 * last_word), bstart, bend, distance);
}

static inline int
remove_tombstones(
    QF *qf, int operation, uint64_t bucket_index, uint64_t tombstone_start_index, uint64_t num_tombstones) {

  // TODO: !!!!!!!!!!!! UPDATE METADATA !!!!!!!!!!!!

  // If this is the last thing in its run, then we may need to set a new runend
  // bit
  if (is_runend(qf, tombstone_start_index + num_tombstones - 1)) {
    if (tombstone_start_index> bucket_index &&
               !is_runend(qf, tombstone_start_index - 1)) {
      // If we're deleting this entry entirely, but it is not the first entry in
      // this run, then set the preceding entry to be the runend
      METADATA_WORD(qf, runends, tombstone_start_index - 1) |=
          1ULL << ((tombstone_start_index - 1) % 64);
    }
  }

  // shift slots back one run at a time
  uint64_t original_bucket = bucket_index;
  uint64_t current_bucket = bucket_index;
  uint64_t current_slot = tombstone_start_index;
  uint64_t current_distance = num_tombstones;
  int ret_current_distance = current_distance;

  while (current_distance > 0) {
    if (is_runend(qf, current_slot + current_distance - 1)) {
      do {
        current_bucket++;
      } while (current_bucket < current_slot + current_distance &&
               !is_occupied(qf, current_bucket));
    }

    if (current_bucket <= current_slot) {
      set_slot(qf, current_slot, get_slot(qf, current_slot + current_distance));
      if (is_runend(qf, current_slot) !=
          is_runend(qf, current_slot + current_distance))
        METADATA_WORD(qf, runends, current_slot) ^= 1ULL << (current_slot % 64);

      if (is_tombstone(qf, current_slot) !=
          is_tombstone(qf, current_slot + current_distance))
        METADATA_WORD(qf, tombstones, current_slot) ^= 1ULL << (current_slot % 64);

      current_slot++;

    } else if (current_bucket <= current_slot + current_distance) {
      uint64_t i;
      for (i = current_slot; i < current_slot + current_distance; i++) {
        set_slot(qf, i, 0);
        METADATA_WORD(qf, runends, i) &= ~(1ULL << (i % 64));
        METADATA_WORD(qf, tombstones, i) |= (1ULL << (i % 64));
      }
      current_distance = current_slot + current_distance - current_bucket;
      current_slot = current_bucket;
    } else {
      current_distance = 0;
    }
  }

  // reset the occupied bit of the hash bucket index if the hash is the
  // only item in the run and is removed completely.
  if (operation)
    METADATA_WORD(qf, occupieds, bucket_index) &=
        ~(1ULL << (bucket_index % 64));

  // update the offset bits.
  // find the number of occupied slots in the original_bucket block.
  // Then find the runend slot corresponding to the last run in the
  // original_bucket block.
  // Update the offset of the block to which it belongs.
  uint64_t original_block = original_bucket / QF_SLOTS_PER_BLOCK;
    while (1) {
      uint64_t last_occupieds_hash_index =
          QF_SLOTS_PER_BLOCK * original_block + (QF_SLOTS_PER_BLOCK - 1);
      uint64_t runend_index = run_end(qf, last_occupieds_hash_index);
      // runend spans across the block
      // update the offset of the next block
      if (runend_index / QF_SLOTS_PER_BLOCK ==
          original_block) { // if the run ends in the same block
        if (get_block(qf, original_block + 1)->offset == 0)
          break;
        get_block(qf, original_block + 1)->offset = 0;
      } else { // if the last run spans across the block
        if (get_block(qf, original_block + 1)->offset ==
            (runend_index - last_occupieds_hash_index))
          break;
        get_block(qf, original_block + 1)->offset =
            (runend_index - last_occupieds_hash_index);
      }
      original_block++;
    }

  return ret_current_distance;
}

static inline int
remove_replace_slots_and_shift_remainders_and_runends_and_offsets(
    QF *qf,
    int operation,              // only_item_in_the_run
    uint64_t bucket_index,      // Home slot, or the quotient part of the hash
    uint64_t overwrite_index,   // index of the start of the remainder
    const uint64_t *remainders, // the new counter
    uint64_t total_remainders,  // length of the new counter
    uint64_t old_length) {
  uint64_t i;

  // Update the slots
  for (i = 0; i < total_remainders; i++)
    set_slot(qf, overwrite_index + i, remainders[i]);

  // If this is the last thing in its run, then we may need to set a new runend
  // bit
  if (is_runend(qf, overwrite_index + old_length - 1)) {
    if (total_remainders > 0) {
      // If we're not deleting this entry entirely, then it will still the last
      // entry in this run
      SET_R(qf, overwrite_index + total_remainders - 1);
    } else if (overwrite_index > bucket_index &&
               !is_runend(qf, overwrite_index - 1)) {
      // If we're deleting this entry entirely, but it is not the first entry in
      // this run, then set the preceding entry to be the runend
      SET_R(qf, overwrite_index - 1);
    }
  }

  // shift slots back one run at a time
  uint64_t original_bucket = bucket_index;
  uint64_t current_bucket = bucket_index; // the home slot of run to shift
  uint64_t current_slot =
      overwrite_index + total_remainders; // the slot to shift to
  uint64_t current_distance =
      old_length - total_remainders;      // the distance to shift
  int ret_current_distance = current_distance;

  while (current_distance > 0) {
    // when we reach the end of a run
    if (is_runend(qf, current_slot + current_distance - 1)) {
      // find the next run to shift
      do {
        current_bucket++;
      } while (current_bucket < current_slot + current_distance &&
               !is_occupied(qf, current_bucket));
    }
    // shift one slot by current_distance to the current_slot
    if (current_bucket <= current_slot) {
      set_slot(qf, current_slot, get_slot(qf, current_slot + current_distance));
      if (is_runend(qf, current_slot) !=
          is_runend(qf, current_slot + current_distance))
        METADATA_WORD(qf, runends, current_slot) ^= 1ULL << (current_slot % 64);
      current_slot++;
      // when we reached the end of the cluster
    } else if (current_bucket <= current_slot + current_distance) {
      uint64_t i;
      for (i = current_slot; i < current_slot + current_distance; i++) {
        set_slot(qf, i, 0);
        RESET_R(qf, i);
      }

      current_distance = current_slot + current_distance - current_bucket;
      current_slot = current_bucket;
    } else {
      current_distance = 0;
    }
  }

  // reset the occupied bit of the hash bucket index if the hash is the
  // only item in the run and is removed completely.
  if (operation && !total_remainders)
    RESET_O(qf, bucket_index);

  // update the offset bits.
  // find the number of occupied slots in the original_bucket block.
  // Then find the runend slot corresponding to the last run in the
  // original_bucket block.
  // Update the offset of the block to which it belongs.
  uint64_t original_block = original_bucket / QF_SLOTS_PER_BLOCK;
  if (old_length >
      total_remainders) { // we only update offsets if we shift/delete anything
    while (1) {
      uint64_t last_occupieds_hash_index =
          QF_SLOTS_PER_BLOCK * original_block + (QF_SLOTS_PER_BLOCK - 1);
      uint64_t runend_index = run_end(qf, last_occupieds_hash_index);
      // runend spans across the block
      // update the offset of the next block
      if (runend_index / QF_SLOTS_PER_BLOCK ==
          original_block) { // if the run ends in the same block
        if (get_block(qf, original_block + 1)->offset == 0)
          break;
        get_block(qf, original_block + 1)->offset = 0;
      } else { // if the last run spans across the block
        if (get_block(qf, original_block + 1)->offset ==
            (runend_index - last_occupieds_hash_index))
          break;
        get_block(qf, original_block + 1)->offset =
            (runend_index - last_occupieds_hash_index);
      }
      original_block++;
    }
  }

  int num_slots_freed = old_length - total_remainders;
  modify_metadata(&qf->runtimedata->pc_noccupied_slots, -num_slots_freed);
  /*qf->metadata->noccupied_slots -= (old_length - total_remainders);*/
  if (!total_remainders) {
    // modify_metadata(&qf->runtimedata->pc_ndistinct_elts, -1);
    /*qf->metadata->ndistinct_elts--;*/
  }

  return ret_current_distance;
}

/*****************************************************************************
 * Code that uses the above to implement a QF with keys and valuess.         *
 *****************************************************************************/

/* return the next slot which corresponds to a
 * different element
 * */
static inline uint64_t next_slot(QF *qf, uint64_t current) {
  uint64_t rem = get_slot(qf, current);
  current++;

  while (get_slot(qf, current) == rem && current <= qf->metadata->nslots) {
    current++;
  }
  return current;
}

/* Return the hash of the key. */
static inline uint64_t key2hash(const QF *qf, const uint64_t key,
                                const uint8_t flags) {
  if (GET_KEY_HASH(flags) == QF_HASH_INVERTIBLE)
    return hash_64(key, BITMASK(qf->metadata->key_bits));
  return key & BITMASK(qf->metadata->key_bits);
}

/* split the hash into quotient and remainder. */
static inline void quotien_remainder(const QF *qf, const uint64_t hash,
                                     uint64_t *const quotient,
                                     uint64_t *const remainder) {
  *quotient = hash >> qf->metadata->key_remainder_bits;
  *remainder = hash & BITMASK(qf->metadata->key_remainder_bits);
}

/* Find the index of the hash (quotient+remainder) and the range of the run.
 * The range will be [run_start_index, run_end_index).
 * Return:
 * 		1: if found it.
 * run_end_index 
 *    0: if didn't find it, the index and range would be where to insert it.
 */
static int find(const QF *qf, const uint64_t quotient, const uint64_t remainder,
                uint64_t *const index, uint64_t *const run_start_index,
                uint64_t *const run_end_index) {
  *run_start_index = 0;
  if (quotient != 0)
    *run_start_index = run_end(qf, quotient - 1) + 1;
  *run_start_index = MAX(*run_start_index, quotient);
  if (!is_occupied(qf, quotient)) {
    *index = *run_start_index;
    *run_end_index = *run_start_index + 1;
    return 0;
  }
  *run_end_index = run_end(qf, quotient) + 1;
  uint64_t curr_remainder;
  *index = *run_start_index;
  do {
    if (!is_tombstone(qf, *index)) {
      curr_remainder = get_slot(qf, *index) >> qf->metadata->value_bits;
      if (remainder == curr_remainder)
        return 1;
      if (remainder < curr_remainder)
        return 0;
    }
    *index += 1;
  } while (*index < *run_end_index);
  return 0;
}

/*****************************************************************************
 * Code that uses the above to implement key-value operations.               *
 *****************************************************************************/

/* TODO: If tombstone_space == 0 and/or nrebuilds == 0, automaticlly calculate
 * them based on current load factor when rebuiding. */
uint64_t qf_init_advanced(QF *qf, uint64_t nslots, uint64_t key_bits,
                          uint64_t value_bits, uint64_t tombstone_space,
                          uint64_t nrebuilds, enum qf_hashmode hash,
                          uint32_t seed, void *buffer, uint64_t buffer_len) {
  uint64_t num_slots, xnslots, nblocks;
  uint64_t key_remainder_bits, bits_per_slot;
  uint64_t size;
  uint64_t total_num_bytes;
  // number of partition counters and the count threshold
  uint32_t num_counters = 8, threshold = 100;

  assert(popcnt(nslots) == 1); /* nslots must be a power of 2 */
  num_slots = nslots;
  xnslots = nslots + 10 * sqrt((double)nslots);
  nblocks = (xnslots + QF_SLOTS_PER_BLOCK - 1) / QF_SLOTS_PER_BLOCK;
  key_remainder_bits = key_bits;
  // set remainder_bits = key_bits - size_bits, where size_bits = log2(nslots)
  while (nslots > 1 && key_remainder_bits > 0) {
    key_remainder_bits--;
    nslots >>= 1;
  }
  // TODO: Why?
  assert(key_remainder_bits >= 2);

  bits_per_slot = key_remainder_bits + value_bits;
  assert(QF_BITS_PER_SLOT == 0 ||
         QF_BITS_PER_SLOT == qf->metadata->bits_per_slot);
  assert(bits_per_slot > 1);
#if QF_BITS_PER_SLOT == 8 || QF_BITS_PER_SLOT == 16 ||                         \
    QF_BITS_PER_SLOT == 32 || QF_BITS_PER_SLOT == 64
  size = nblocks * sizeof(qfblock);
#else
  size = nblocks * (sizeof(qfblock) + QF_SLOTS_PER_BLOCK * bits_per_slot / 8);
#endif

  total_num_bytes = sizeof(qfmetadata) + size;
  if (buffer == NULL || total_num_bytes > buffer_len)
    return total_num_bytes;

  memset(buffer, 0, total_num_bytes);
  qf->metadata = (qfmetadata *)(buffer);
  qf->blocks = (qfblock *)(qf->metadata + 1);

  qf->metadata->magic_endian_number = MAGIC_NUMBER;
  qf->metadata->reserved = 0;
  qf->metadata->hash_mode = hash;
  qf->metadata->total_size_in_bytes = size;
  qf->metadata->seed = seed;
  qf->metadata->nslots = num_slots;
  qf->metadata->xnslots = xnslots;
  // qf->metadata->tombstone_space = tombstone_space;
  // qf->metadata->nrebuilds = nrebuilds;
  // qf->metadata->rebuild_slots = xnslots / nrebuilds + 1;
  qf->metadata->key_bits = key_bits;
  qf->metadata->value_bits = value_bits;
  qf->metadata->key_remainder_bits = key_remainder_bits;
  qf->metadata->bits_per_slot = bits_per_slot;

  qf->metadata->range = qf->metadata->nslots;
  qf->metadata->range <<= qf->metadata->key_remainder_bits;
  qf->metadata->nblocks =
      (qf->metadata->xnslots + QF_SLOTS_PER_BLOCK - 1) / QF_SLOTS_PER_BLOCK;
  // qf->metadata->rebuild_pos = 0;
  // qf->metadata->next_tombstone = qf->metadata->tombstone_space;
  qf->metadata->nelts = 0;
  qf->metadata->noccupied_slots = 0;
  qf->metadata->n_tombstones = 0;

  // Set all tombstones
  char *b = (char *)(qf->blocks);
  size_t block_size =
      sizeof(qfblock) + QF_SLOTS_PER_BLOCK * qf->metadata->bits_per_slot / 8;
  for (uint64_t i = 0; i < qf->metadata->nblocks; i++) {
    ((qfblock *)b)->tombstones[0] = 0xffffffffffffffffULL;
    b += block_size;
  }

  qf->runtimedata->num_locks = (qf->metadata->xnslots / NUM_SLOTS_TO_LOCK) + 2;

  pc_init(&qf->runtimedata->pc_nelts, (int64_t *)&qf->metadata->nelts,
          num_counters, threshold);
  pc_init(&qf->runtimedata->pc_noccupied_slots,
          (int64_t *)&qf->metadata->noccupied_slots, num_counters, threshold);
  pc_init(&qf->runtimedata->pc_n_tombstones,
          (int64_t *)&qf->metadata->n_tombstones, num_counters, threshold);
  /* initialize container resize */
  qf->runtimedata->auto_resize = 0;
  qf->runtimedata->container_resize = qf_resize_malloc;
  /* initialize all the locks to 0 */
  qf->runtimedata->metadata_lock = 0;
  qf->runtimedata->locks =
      (volatile int *)calloc(qf->runtimedata->num_locks, sizeof(volatile int));
  if (qf->runtimedata->locks == NULL) {
    perror("Couldn't allocate memory for runtime locks.");
    exit(EXIT_FAILURE);
  }
#ifdef LOG_WAIT_TIME
  qf->runtimedata->wait_times = (wait_time_data *)calloc(
      qf->runtimedata->num_locks + 1, sizeof(wait_time_data));
  if (qf->runtimedata->wait_times == NULL) {
    perror("Couldn't allocate memory for runtime wait_times.");
    exit(EXIT_FAILURE);
  }
#endif

  return total_num_bytes;
}

uint64_t qf_init(QF *qf, uint64_t nslots, uint64_t key_bits,
                 uint64_t value_bits, enum qf_hashmode hash, uint32_t seed,
                 void *buffer, uint64_t buffer_len) {
  return qf_init_advanced(qf, nslots, key_bits, value_bits, 0, 0, hash, seed,
                          buffer, buffer_len);
}

uint64_t qf_use(QF *qf, void *buffer, uint64_t buffer_len) {
  qf->metadata = (qfmetadata *)(buffer);
  if (qf->metadata->total_size_in_bytes + sizeof(qfmetadata) > buffer_len) {
    return qf->metadata->total_size_in_bytes + sizeof(qfmetadata);
  }
  qf->blocks = (qfblock *)(qf->metadata + 1);

  qf->runtimedata = (qfruntime *)calloc(sizeof(qfruntime), 1);
  if (qf->runtimedata == NULL) {
    perror("Couldn't allocate memory for runtime data.");
    exit(EXIT_FAILURE);
  }
  /* initialize all the locks to 0 */
  qf->runtimedata->metadata_lock = 0;
  qf->runtimedata->locks =
      (volatile int *)calloc(qf->runtimedata->num_locks, sizeof(volatile int));
  if (qf->runtimedata->locks == NULL) {
    perror("Couldn't allocate memory for runtime locks.");
    exit(EXIT_FAILURE);
  }
#ifdef LOG_WAIT_TIME
  qf->runtimedata->wait_times = (wait_time_data *)calloc(
      qf->runtimedata->num_locks + 1, sizeof(wait_time_data));
  if (qf->runtimedata->wait_times == NULL) {
    perror("Couldn't allocate memory for runtime wait_times.");
    exit(EXIT_FAILURE);
  }
#endif

  return sizeof(qfmetadata) + qf->metadata->total_size_in_bytes;
}

void *qf_destroy(QF *qf) {
  assert(qf->runtimedata != NULL);
  if (qf->runtimedata->locks != NULL)
    free((void *)qf->runtimedata->locks);
  if (qf->runtimedata->wait_times != NULL)
    free(qf->runtimedata->wait_times);
  free(qf->runtimedata);

  return (void *)qf->metadata;
}

bool qf_malloc(QF *qf, uint64_t nslots, uint64_t key_bits, uint64_t value_bits,
               enum qf_hashmode hash, uint32_t seed) {
  return qf_malloc_advance(qf, nslots, key_bits, value_bits, hash, seed, 0, 0);
}

bool qf_malloc_advance(QF *qf, uint64_t nslots, uint64_t key_bits,
                       uint64_t value_bits, enum qf_hashmode hash,
                       uint32_t seed, uint64_t tombstone_space,
                       uint64_t nrebuilds) {
  uint64_t total_num_bytes =
      qf_init_advanced(qf, nslots, key_bits, value_bits, tombstone_space,
                       nrebuilds, hash, seed, NULL, 0);

  void *buffer = malloc(total_num_bytes);
  if (buffer == NULL) {
    perror("Couldn't allocate memory for the CQF.");
    exit(EXIT_FAILURE);
  }

  qf->runtimedata = (qfruntime *)calloc(sizeof(qfruntime), 1);
  if (qf->runtimedata == NULL) {
    perror("Couldn't allocate memory for runtime data.");
    exit(EXIT_FAILURE);
  }

  uint64_t init_size =
      qf_init_advanced(qf, nslots, key_bits, value_bits, tombstone_space,
                       nrebuilds, hash, seed, buffer, total_num_bytes);

  if (init_size == total_num_bytes)
    return true;
  else
    return false;
}

bool qf_free(QF *qf) {
  assert(qf->metadata != NULL);
  void *buffer = qf_destroy(qf);
  if (buffer != NULL) {
    free(buffer);
    return true;
  }

  return false;
}

void qf_copy(QF *dest, const QF *src) {
  DEBUG_CQF("%s\n", "Source CQF");
  DEBUG_DUMP(src);
  memcpy(dest->runtimedata, src->runtimedata, sizeof(qfruntime));
  memcpy(dest->metadata, src->metadata, sizeof(qfmetadata));
  memcpy(dest->blocks, src->blocks, src->metadata->total_size_in_bytes);
  DEBUG_CQF("%s\n", "Destination CQF after copy.");
  DEBUG_DUMP(dest);
}

void qf_reset(QF *qf) {
  qf->metadata->nelts = 0;
  qf->metadata->noccupied_slots = 0;

#ifdef LOG_WAIT_TIME
  memset(qf->wait_times, 0,
         (qf->runtimedata->num_locks + 1) * sizeof(wait_time_data));
#endif
#if QF_BITS_PER_SLOT == 8 || QF_BITS_PER_SLOT == 16 ||                         \
    QF_BITS_PER_SLOT == 32 || QF_BITS_PER_SLOT == 64
  memset(qf->blocks, 0, qf->metadata->nblocks * sizeof(qfblock));
#else
  memset(qf->blocks, 0,
         qf->metadata->nblocks *
             (sizeof(qfblock) +
              QF_SLOTS_PER_BLOCK * qf->metadata->bits_per_slot / 8));
#endif
}

uint64_t qf_get_key_from_index(const QF *qf, const size_t index) {
  return get_slot(qf, index) >> qf->metadata->value_bits;
}

int64_t qf_get_unique_index(const QF *qf, uint64_t key, uint64_t value,
                            uint8_t flags) {
  if (GET_KEY_HASH(flags) == QF_HASH_INVERTIBLE)
    key = hash_64(key, BITMASK(qf->metadata->key_bits));

  uint64_t hash = (key << qf->metadata->value_bits) |
                  (value & BITMASK(qf->metadata->value_bits));
  uint64_t hash_remainder = hash & BITMASK(qf->metadata->bits_per_slot);
  int64_t hash_bucket_index = hash >> qf->metadata->bits_per_slot;

  if (!is_occupied(qf, hash_bucket_index))
    return QF_DOESNT_EXIST;

  int64_t runstart_index =
      hash_bucket_index == 0 ? 0 : run_end(qf, hash_bucket_index - 1) + 1;
  if (runstart_index < hash_bucket_index)
    runstart_index = hash_bucket_index;

  /* printf("MC RUNSTART: %02lx RUNEND: %02lx\n", runstart_index, runend_index);
   */

  uint64_t current_remainder, current_end;
  do {
    current_end = runstart_index;
    current_remainder = get_slot(qf, current_end);
    if (current_remainder == hash_remainder)
      return runstart_index;

    runstart_index = current_end + 1;
  } while (!is_runend(qf, current_end));

  return QF_DOESNT_EXIST;
}

enum qf_hashmode qf_get_hashmode(const QF *qf) {
  return qf->metadata->hash_mode;
}
uint64_t qf_get_hash_seed(const QF *qf) { return qf->metadata->seed; }
__uint128_t qf_get_hash_range(const QF *qf) { return qf->metadata->range; }

uint64_t qf_get_total_size_in_bytes(const QF *qf) {
  return qf->metadata->total_size_in_bytes;
}
uint64_t qf_get_nslots(const QF *qf) { return qf->metadata->nslots; }
uint64_t qf_get_num_occupied_slots(const QF *qf) {
  pc_sync(&qf->runtimedata->pc_noccupied_slots);
  return qf->metadata->noccupied_slots;
}

uint64_t qf_get_num_key_bits(const QF *qf) { return qf->metadata->key_bits; }
uint64_t qf_get_num_value_bits(const QF *qf) {
  return qf->metadata->value_bits;
}
uint64_t qf_get_num_key_remainder_bits(const QF *qf) {
  return qf->metadata->key_remainder_bits;
}
uint64_t qf_get_bits_per_slot(const QF *qf) {
  return qf->metadata->bits_per_slot;
}

void qf_sync_counters(const QF *qf) {
  pc_sync(&qf->runtimedata->pc_nelts);
  pc_sync(&qf->runtimedata->pc_noccupied_slots);
  pc_sync(&qf->runtimedata->pc_n_tombstones);
}

/* initialize the iterator at the run corresponding
 * to the position index
 */
int64_t qf_iterator_from_position(const QF *qf, QFi *qfi, uint64_t position) {
  if (position == 0xffffffffffffffff) {
    qfi->current = 0xffffffffffffffff;
    qfi->qf = qf;
    return QFI_INVALID;
  }
  assert(position < qf->metadata->nslots);
  if (!is_occupied(qf, position)) {
    uint64_t block_index = position;
    uint64_t idx = bitselect(get_block(qf, block_index)->occupieds[0], 0);
    if (idx == 64) {
      while (idx == 64 && block_index < qf->metadata->nblocks) {
        block_index++;
        idx = bitselect(get_block(qf, block_index)->occupieds[0], 0);
      }
    }
    position = block_index * QF_SLOTS_PER_BLOCK + idx;
  }

  qfi->qf = qf;
  qfi->num_clusters = 0;
  qfi->run = position;
  qfi->current = position == 0 ? 0 : run_end(qfi->qf, position - 1) + 1;
  if (qfi->current < position)
    qfi->current = position;

#ifdef LOG_CLUSTER_LENGTH
  qfi->c_info =
      (cluster_data *)calloc(qf->metadata->nslots / 32, sizeof(cluster_data));
  if (qfi->c_info == NULL) {
    perror("Couldn't allocate memory for c_info.");
    exit(EXIT_FAILURE);
  }
  qfi->cur_start_index = position;
  qfi->cur_length = 1;
#endif

  if (qfi->current >= qf->metadata->nslots)
    return QFI_INVALID;
  return qfi->current;
}

int64_t qf_iterator_from_key_value(const QF *qf, QFi *qfi, uint64_t key,
                                   uint64_t value, uint8_t flags) {
  if (key >= qf->metadata->range) {
    qfi->current = 0xffffffffffffffff;
    qfi->qf = qf;
    return QFI_INVALID;
  }

  qfi->qf = qf;
  qfi->num_clusters = 0;

  if (GET_KEY_HASH(flags) == QF_HASH_INVERTIBLE)
    key = hash_64(key, BITMASK(qf->metadata->key_bits));

  uint64_t hash = (key << qf->metadata->value_bits) |
                  (value & BITMASK(qf->metadata->value_bits));

  uint64_t hash_remainder = hash & BITMASK(qf->metadata->bits_per_slot);
  uint64_t hash_bucket_index = hash >> qf->metadata->bits_per_slot;
  bool flag = false;

  // If a run starts at "position" move the iterator to point it to the
  // smallest key greater than or equal to "hash".
  if (is_occupied(qf, hash_bucket_index)) {
    uint64_t runstart_index =
        hash_bucket_index == 0 ? 0 : run_end(qf, hash_bucket_index - 1) + 1;
    if (runstart_index < hash_bucket_index)
      runstart_index = hash_bucket_index;
    uint64_t current_remainder, current_end;
    do {
      current_end = runstart_index;
      current_remainder = get_slot(qf, current_end);
      if (current_remainder >= hash_remainder) {
        flag = true;
        break;
      }
      runstart_index = current_end + 1;
    } while (!is_runend(qf, current_end));
    // found "hash" or smallest key greater than "hash" in this run.
    if (flag) {
      qfi->run = hash_bucket_index;
      qfi->current = runstart_index;
    }
  }
  // If a run doesn't start at "position" or the largest key in the run
  // starting at "position" is smaller than "hash" then find the start of the
  // next run.
  if (!is_occupied(qf, hash_bucket_index) || !flag) {
    uint64_t position = hash_bucket_index;
    assert(position < qf->metadata->nslots);
    uint64_t block_index = position / QF_SLOTS_PER_BLOCK;
    uint64_t idx = bitselect(get_block(qf, block_index)->occupieds[0], 0);
    if (idx == 64) {
      while (idx == 64 && block_index < qf->metadata->nblocks) {
        block_index++;
        idx = bitselect(get_block(qf, block_index)->occupieds[0], 0);
      }
    }
    position = block_index * QF_SLOTS_PER_BLOCK + idx;
    qfi->run = position;
    qfi->current = position == 0 ? 0 : run_end(qfi->qf, position - 1) + 1;
    if (qfi->current < position)
      qfi->current = position;
  }

  if (qfi->current >= qf->metadata->nslots)
    return QFI_INVALID;
  return qfi->current;
}

static int qfi_get(const QFi *qfi, uint64_t *key, uint64_t *value) {
  if (qfi_end(qfi))
    return QFI_INVALID;

  uint64_t current_remainder = get_slot(qfi->qf, qfi->current);

  *value = current_remainder & BITMASK(qfi->qf->metadata->value_bits);
  current_remainder = current_remainder >> qfi->qf->metadata->value_bits;
  *key =
      (qfi->run << qfi->qf->metadata->key_remainder_bits) | current_remainder;

  return 0;
}

int qfi_get_key(const QFi *qfi, uint64_t *key, uint64_t *value) {
  *key = *value = 0;
  int ret = qfi_get(qfi, key, value);
  if (ret == 0) {
    if (qfi->qf->metadata->hash_mode == QF_HASH_INVERTIBLE)
      *key = hash_64i(*key, BITMASK(qfi->qf->metadata->key_bits));
  }

  return ret;
}

int qfi_get_hash(const QFi *qfi, uint64_t *key, uint64_t *value) {
  *key = *value = 0;
  return qfi_get(qfi, key, value);
}

int qfi_next(QFi *qfi) {
  if (qfi_end(qfi))
    return QFI_INVALID;
  else {
    if (!is_runend(qfi->qf, qfi->current)) {
      qfi->current++;
#ifdef LOG_CLUSTER_LENGTH
      qfi->cur_length++;
#endif
      if (qfi_end(qfi))
        return QFI_INVALID;
      return 0;
    } else {
#ifdef LOG_CLUSTER_LENGTH
      /* save to check if the new current is the new cluster. */
      uint64_t old_current = qfi->current;
#endif
      uint64_t block_index = qfi->run / QF_SLOTS_PER_BLOCK;
      uint64_t rank = bitrank(get_block(qfi->qf, block_index)->occupieds[0],
                              qfi->run % QF_SLOTS_PER_BLOCK);
      uint64_t next_run =
          bitselect(get_block(qfi->qf, block_index)->occupieds[0], rank);
      if (next_run == 64) {
        rank = 0;
        while (next_run == 64 && block_index < qfi->qf->metadata->nblocks) {
          block_index++;
          next_run =
              bitselect(get_block(qfi->qf, block_index)->occupieds[0], rank);
        }
      }
      if (block_index == qfi->qf->metadata->nblocks) {
        /* set the index values to max. */
        qfi->run = qfi->current = qfi->qf->metadata->xnslots;
        return QFI_INVALID;
      }
      qfi->run = block_index * QF_SLOTS_PER_BLOCK + next_run;
      qfi->current++;
      if (qfi->current < qfi->run)
        qfi->current = qfi->run;
#ifdef LOG_CLUSTER_LENGTH
      if (qfi->current > old_current + 1) { /* new cluster. */
        if (qfi->cur_length > 10) {
          qfi->c_info[qfi->num_clusters].start_index = qfi->cur_start_index;
          qfi->c_info[qfi->num_clusters].length = qfi->cur_length;
          qfi->num_clusters++;
        }
        qfi->cur_start_index = qfi->run;
        qfi->cur_length = 1;
      } else {
        qfi->cur_length++;
      }
#endif
      return 0;
    }
  }
}

bool qfi_end(const QFi *qfi) {
  if (qfi->current >=
      qfi->qf->metadata->xnslots /*&& is_runend(qfi->qf, qfi->current)*/)
    return true;
  return false;
}

/***********************************************************************
 * Tombstone cleaning functions.                                       *
 ***********************************************************************/

bool rhm_malloc(RHM *rhm, uint64_t nslots, uint64_t key_bits,
                uint64_t value_bits, enum qf_hashmode hash, uint32_t seed) {
  return qf_malloc(rhm, nslots, key_bits, value_bits, hash, seed);
}

void rhm_destroy(RHM *rhm) {
  qf_destroy(rhm);
}

bool rhm_free(RHM *rhm) {
  return qf_free(rhm);
}

static inline int rhm_insert1(QF *qf, __uint128_t hash, uint8_t runtime_lock) {
  int ret_distance = 0;
  uint64_t hash_slot_value = (hash & BITMASK(qf->metadata->bits_per_slot));
  uint64_t hash_remainder = hash_slot_value >> qf->metadata->value_bits;
  uint64_t hash_bucket_index = hash >> qf->metadata->bits_per_slot;
  uint64_t hash_bucket_block_offset = hash_bucket_index % QF_SLOTS_PER_BLOCK;
  if (GET_NO_LOCK(runtime_lock) != QF_NO_LOCK) {
    if (!qf_lock(qf, hash_bucket_index, /*small*/ true, runtime_lock))
      return QF_COULDNT_LOCK;
  }

  if (is_empty(qf, hash_bucket_index) /* might_be_empty(qf, hash_bucket_index) && runend_index == hash_bucket_index */) {
    METADATA_WORD(qf, runends, hash_bucket_index) |=
        1ULL << (hash_bucket_block_offset % 64);
    set_slot(qf, hash_bucket_index, hash_slot_value);
    METADATA_WORD(qf, occupieds, hash_bucket_index) |=
        1ULL << (hash_bucket_block_offset % 64);
    ret_distance = 0;
    modify_metadata(&qf->runtimedata->pc_noccupied_slots, 1);
    modify_metadata(&qf->runtimedata->pc_nelts, 1);
  } else {
    uint64_t runend_index = run_end(qf, hash_bucket_index);
    int operation = 0; /* Insert into empty bucket */
    uint64_t insert_index = runend_index + 1;
    uint64_t new_value = hash_slot_value;
    uint64_t runstart_index =
        hash_bucket_index == 0 ? 0 : run_end(qf, hash_bucket_index - 1) + 1;
    if (is_occupied(qf, hash_bucket_index)) {
      uint64_t current_remainder = get_slot_remainder(qf, runstart_index);
      while (current_remainder < hash_remainder && runstart_index <= runend_index) {
        runstart_index++;
        /* This may read past the end of the run, but the while loop
                 condition will prevent us from using the invalid result in
                 that case. */
        current_remainder = get_slot_remainder(qf, runstart_index);
      }
      /* If this is the first time we've inserted the new remainder,
               and it is larger than any remainder in the run. */
      if (runstart_index > runend_index) {
        operation = 1;
        insert_index = runstart_index;
        new_value = hash_slot_value;
        modify_metadata(&qf->runtimedata->pc_nelts, 1);
      /* Replace the current slot with this new hash. Don't shift anything. */
      } else if (current_remainder == hash_remainder) {
        operation = -1;
        insert_index = runstart_index;
        new_value = hash_slot_value;
        set_slot(qf, insert_index, new_value);
      /* First time we're inserting this remainder, but there are 
          are larger remainders in the run. */
      } else {
        operation = 2; /* Inserting */
        insert_index = runstart_index;
        new_value = hash_slot_value;
        modify_metadata(&qf->runtimedata->pc_nelts, 1);
      }
    } else {
        modify_metadata(&qf->runtimedata->pc_nelts, 1);
    }
    if (operation >= 0) {
      uint64_t empty_slot_index;
      int ret = find_first_empty_slot(qf, runend_index + 1, &empty_slot_index);
      if (ret < 0) return QF_NO_SPACE;
      shift_remainders(qf, insert_index, empty_slot_index);
      set_slot(qf, insert_index, new_value);
      ret_distance = insert_index - hash_bucket_index;

      shift_runends(qf, insert_index, empty_slot_index - 1, 1);

      switch (operation) {
      case 0:
        METADATA_WORD(qf, runends, insert_index) |=
            1ULL << ((insert_index % QF_SLOTS_PER_BLOCK) % 64);
        break;
      case 1:
        METADATA_WORD(qf, runends, insert_index - 1) &=
            ~(1ULL << (((insert_index - 1) % QF_SLOTS_PER_BLOCK) % 64));
        METADATA_WORD(qf, runends, insert_index) |=
            1ULL << ((insert_index % QF_SLOTS_PER_BLOCK) % 64);
        break;
      case 2:
        METADATA_WORD(qf, runends, insert_index) &=
            ~(1ULL << ((insert_index % QF_SLOTS_PER_BLOCK) % 64));
        break;
      default:
        fprintf(stderr, "Invalid operation %d\n", operation);
        abort();
      }
      /*
       * Increment the offset for each block between the hash bucket index
       * and block of the empty slot
       * */
      uint64_t i;
      for (i = hash_bucket_index / QF_SLOTS_PER_BLOCK + 1;
           i <= empty_slot_index / QF_SLOTS_PER_BLOCK; i++) {
        if (get_block(qf, i)->offset <
            BITMASK(8 * sizeof(qf->blocks[0].offset)))
          get_block(qf, i)->offset++;
        assert(get_block(qf, i)->offset != 0);
      }
      modify_metadata(&qf->runtimedata->pc_noccupied_slots, 1);
      modify_metadata(&qf->runtimedata->pc_nelts, 1);
    }
    METADATA_WORD(qf, occupieds, hash_bucket_index) |=
        1ULL << (hash_bucket_block_offset % 64);
  }
  if (GET_NO_LOCK(runtime_lock) != QF_NO_LOCK) {
    qf_unlock(qf, hash_bucket_index, /*small*/ true);
  }
  return ret_distance;
}

int rhm_insert(RHM *qf, uint64_t key, uint64_t value, uint8_t flags) {
  if (qf_get_num_occupied_slots(qf) >= qf->metadata->nslots * 0.99) {
    return QF_NO_SPACE;
  }
  if (GET_KEY_HASH(flags) != QF_KEY_IS_HASH) {
    fprintf(stderr, "RobinHood HM assumes key is hash for now.");
    abort();
  }
  uint64_t hash = key;
  hash = (hash<< qf->metadata->value_bits) |
                  (value & BITMASK(qf->metadata->value_bits));
            
  int ret = rhm_insert1(qf, hash, flags);
  
  return ret;
}

int rhm_remove(RHM *qf, uint64_t key, uint8_t flags) {
  if (GET_KEY_HASH(flags) != QF_KEY_IS_HASH) {
    fprintf(stderr, "RobinHood HM assumes key is hash for now.");
    abort();
  }
  uint64_t hash = key;
  uint64_t hash_remainder = hash & BITMASK(qf->metadata->key_remainder_bits);
  int64_t hash_bucket_index = hash >> qf->metadata->key_remainder_bits;

  if (GET_NO_LOCK(flags) != QF_NO_LOCK) {
		if (!qf_lock(qf, hash_bucket_index, /*small*/ false, flags))
			return QF_COULDNT_LOCK;
	}

  /* Empty bucket */
  if (!is_occupied(qf, hash_bucket_index))
    return QF_DOESNT_EXIST;

  uint64_t runstart_index =
      hash_bucket_index == 0 ? 0 : run_end(qf, hash_bucket_index - 1) + 1;
  int only_item_in_the_run = 0;
  uint64_t current_index = runstart_index;
  uint64_t current_remainder = get_slot_remainder(qf, current_index);
  while (current_remainder < hash_remainder && !is_runend(qf, current_index)) {
    	current_index = current_index + 1;
		  current_remainder = get_slot_remainder(qf, current_index);
  }
	if (current_remainder != hash_remainder)
		return QF_DOESNT_EXIST;

  if (runstart_index == current_index && is_runend(qf, current_index))
		only_item_in_the_run = 1;
  uint64_t *p = 0x00; // The New Counter length is 0.
  int ret_numfreedslots = remove_replace_slots_and_shift_remainders_and_runends_and_offsets(qf,
																																		only_item_in_the_run,
																																		hash_bucket_index,
																																		current_index,
																																		p,
																																		0,
																																		1);
  modify_metadata(&qf->runtimedata->pc_nelts, -1);
	if (GET_NO_LOCK(flags) != QF_NO_LOCK) {
		qf_unlock(qf, hash_bucket_index, /*small*/ false);
	}
	return ret_numfreedslots;

}

int rhm_lookup(const QF *qf, uint64_t key, uint64_t *value, uint8_t flags) {
  if (GET_KEY_HASH(flags) != QF_KEY_IS_HASH) {
    fprintf(stderr, "RobinHood HM assumes key is hash for now.");
    abort();
  }
  uint64_t hash = key;
  uint64_t hash_remainder = hash & BITMASK(qf->metadata->key_remainder_bits);
  int64_t hash_bucket_index = hash >> qf->metadata->key_remainder_bits;
  if (!is_occupied(qf, hash_bucket_index))
    return QF_DOESNT_EXIST;

  int64_t runstart_index =
      hash_bucket_index == 0 ? 0 : run_end(qf, hash_bucket_index - 1) + 1;
  if (runstart_index < hash_bucket_index)
    runstart_index = hash_bucket_index;

  uint64_t current_slot_value, current_index, current_remainder;
  current_index = runstart_index;
  do {
    current_slot_value = get_slot(qf, current_index);
    current_remainder = current_slot_value >> qf->metadata->value_bits;
    if (current_remainder == hash_remainder) {
      *value = current_slot_value & BITMASK(qf->metadata->value_bits);
      return (current_index - runstart_index + 1);
    }
    current_index++;
  } while (!is_runend(qf, current_index - 1));
  return QF_DOESNT_EXIST;
}

/******************************************************************
 * Tombsone Robinhood Hashmap *
 ******************************************************************/

uint64_t trhm_init(TRHM *trhm, uint64_t nslots, uint64_t key_bits,
                  uint64_t value_bits, enum qf_hashmode hash, uint32_t seed,
                  void *buffer, uint64_t buffer_len) {
  return qf_init(trhm, nslots, key_bits, value_bits, hash, seed, buffer, buffer_len);
}

bool trhm_malloc(RHM *rhm, uint64_t nslots, uint64_t key_bits,
                uint64_t value_bits, enum qf_hashmode hash, uint32_t seed) {
  return qf_malloc(rhm, nslots, key_bits, value_bits, hash, seed);
}

void trhm_destroy(RHM *rhm) {
  qf_destroy(rhm);
}

bool trhm_free(RHM *rhm) {
  return qf_free(rhm);
}

int trhm_insert(RHM *qf, uint64_t key, uint64_t value, uint8_t flags) {
  if (qf_get_num_occupied_slots(qf) >= qf->metadata->nslots * 0.99) {
    return QF_NO_SPACE;
  }
  if (GET_KEY_HASH(flags) != QF_KEY_IS_HASH) {
    fprintf(stderr, "RobinHood Tombstone HM assumes key is hash for now.");
    abort();
  }
  uint64_t hash = key;
  hash = (hash<< qf->metadata->value_bits) |
                  (value & BITMASK(qf->metadata->value_bits));
  // int ret = trhm_insert1(qf, hash, flags);
  size_t ret_distance = 0;
  uint64_t hash = key2hash(qf, key, flags);
  uint64_t hash_remainder, hash_bucket_index; // remainder and quotient.
  quotien_remainder(qf, hash, &hash_bucket_index, &hash_remainder);
  uint64_t new_value = (hash_remainder << qf->metadata->value_bits) |
                       (value & BITMASK(qf->metadata->value_bits));

  if (GET_NO_LOCK(flags) != QF_NO_LOCK) {
    if (!qf_lock(qf, hash_bucket_index, /*small*/ true, flags))
      return QF_COULDNT_LOCK;
  }

  if (is_empty(qf, hash_bucket_index)) {
    set_slot(qf, hash_bucket_index, new_value);
    SET_R(qf, hash_bucket_index);
    SET_O(qf, hash_bucket_index);
    RESET_T(qf, hash_bucket_index);

    modify_metadata(&qf->runtimedata->pc_noccupied_slots, 1);
    modify_metadata(&qf->runtimedata->pc_nelts, 1);
  } else {
    uint64_t insert_index, runstart_index, runend_index;
    int ret = find(qf, hash_bucket_index, hash_remainder, &insert_index,
                   &runstart_index, &runend_index);
    if (ret == 1)
      return QF_KEY_EXISTS;
    uint64_t available_slot_index;
    ret = find_first_tombstone(qf, insert_index, &available_slot_index);
    ret_distance = available_slot_index - hash_bucket_index;
    if (available_slot_index >= qf->metadata->xnslots)
      return QF_NO_SPACE;
    // counts
    modify_metadata(&qf->runtimedata->pc_nelts, 1);
    if (is_empty(qf, available_slot_index))
      modify_metadata(&qf->runtimedata->pc_noccupied_slots, 1);
    else { // use a tombstone
      modify_metadata(&qf->runtimedata->pc_n_tombstones, -1);
    }
    // shift
    shift_remainders(qf, insert_index, available_slot_index);
    // shift_runends_tombstones(qf, insert_index, available_slot_index, 1);
    set_slot(qf, insert_index, new_value);
    // Fix metadata
    // If it is a new run, we need a new runend
    if (!is_occupied(qf, hash_bucket_index)) {
      shift_runends_tombstones(qf, insert_index, available_slot_index, 1);
      SET_R(qf, insert_index);
    } else if (insert_index >= runend_index) {
      // insert to the end of the run
      shift_runends_tombstones(qf, insert_index - 1, available_slot_index, 1);
    } else {
      // insert to the begin or middle
      shift_runends_tombstones(qf, insert_index, available_slot_index, 1);
    }
    SET_O(qf, hash_bucket_index);
    /* Increment the offset for each block between the hash bucket index
     * and block of the empty slot
     */
    uint64_t i;
    for (i = hash_bucket_index / QF_SLOTS_PER_BLOCK + 1;
         i <= available_slot_index / QF_SLOTS_PER_BLOCK; i++) {
      if (get_block(qf, i)->offset < BITMASK(8 * sizeof(qf->blocks[0].offset)))
        get_block(qf, i)->offset++;
      assert(get_block(qf, i)->offset != 0);
    }
  }

  if (GET_NO_LOCK(flags) != QF_NO_LOCK) {
    qf_unlock(qf, hash_bucket_index, /*small*/ true);
  }

  return ret_distance;
}

int trhm_remove(RHM *qf, uint64_t key, uint8_t flags) {
  uint64_t hash = key2hash(qf, key, flags);
  uint64_t hash_remainder, hash_bucket_index;
  quotien_remainder(qf, hash, &hash_bucket_index, &hash_remainder);

  if (GET_NO_LOCK(flags) != QF_NO_LOCK) {
		if (!qf_lock(qf, hash_bucket_index, /*small*/ false, flags))
			return QF_COULDNT_LOCK;
	}

  /* Empty bucket */
  if (!is_occupied(qf, hash_bucket_index))
    return QF_DOESNT_EXIST;

  uint64_t current_index, runstart_index, runend_index;
  int ret = find(qf, hash_bucket_index, hash_remainder, &current_index,
                 &runstart_index, &runend_index);
  // remainder not found
  if (ret == 0)
    return QF_DOESNT_EXIST;
  
  SET_T(qf, current_index);
	modify_metadata(&qf->runtimedata->pc_nelts, -1);

  // Make sure that the run never end with a tombstone.
  while (is_runend(qf, current_index) && is_tombstone(qf, current_index)) {
    RESET_R(qf, current_index);
    // if it is the only element in the run
    if (current_index - runstart_index == 0) {
      RESET_O(qf, hash_bucket_index);
      if (is_empty(qf, current_index))
        modify_metadata(&qf->runtimedata->pc_noccupied_slots, -1);
      break;
    } else {
      SET_R(qf, current_index-1);
      if (is_empty(qf, current_index))
        modify_metadata(&qf->runtimedata->pc_noccupied_slots, -1);
      --current_index;
    }
  }

  if (GET_NO_LOCK(flags) != QF_NO_LOCK) {
    qf_unlock(qf, hash_bucket_index, /*small*/ false);
  }

  return current_index - runstart_index + 1;
}

int trhm_lookup(const QF *qf, uint64_t key, uint64_t *value, uint8_t flags) {
  uint64_t hash = key2hash(qf, key, flags);
  uint64_t hash_remainder, hash_bucket_index;
  quotien_remainder(qf, hash, &hash_bucket_index, &hash_remainder);

  if (!is_occupied(qf, hash_bucket_index))
    return QF_DOESNT_EXIST;

  uint64_t current_index, runstart_index, runend_index;
  int ret = find(qf, hash_bucket_index, hash_remainder, &current_index,
                 &runstart_index, &runend_index);
  if (ret == 0)
    return QF_DOESNT_EXIST;
  *value = get_slot(qf, current_index) & BITMASK(qf->metadata->value_bits);
  return 0;
}

uint64_t trhm_clear_tombstones_in_run(QF *qf, uint64_t home_slot, uint64_t run_start, uint8_t flags) {
	uint64_t idx = run_start;
	uint64_t runend_index = run_end(qf, home_slot);
	while (idx <= runend_index) {
		if (!is_tombstone(qf, idx)) {
			idx++;
			continue;
		}
		uint64_t tombstone_start = idx;
		uint64_t tombstone_end = idx;
		// TODO: There must be a more efficient way to find this.
		while (is_tombstone(qf, tombstone_end) && tombstone_end <= runend_index) {
			tombstone_end++;
		}
		int only_element = (tombstone_start == run_start && tombstone_end == runend_index+1);
		// tombstone_end is one step ahead of the last tombstone in this cluster.
		remove_tombstones(
			qf, only_element, home_slot, tombstone_start, tombstone_end - tombstone_start
		);
		if (only_element) {
			runend_index = run_start;
			break;
		}
		runend_index -= (tombstone_end-tombstone_start);
		idx++;
	}
	return runend_index;
}

int trhm_clear_tombstones(QF *qf, uint8_t flags) {
	// TODO: Lock the whole Hashset.
  printf("BEFORE CLEARING\n");
  qf_dump(qf);
	uint64_t run_start = 0;
	for (uint64_t idx=0; idx < qf->metadata->nslots; idx++) {
		if (idx > run_start) {
			run_start = idx;
		}
		if (is_occupied(qf, idx)) {
			run_start = trhm_clear_tombstones_in_run(qf, idx, run_start, flags);
			run_start++;
		}
	}
  printf("AFTER CLEARING\n");
  qf_dump(qf);
  return 0;
}
/* Rebuild the next rebuild intervals.
 * Return value:
 *   >= 0: number of tombstones being pushed forwards. If it is too big,
 * 				consider to rebuild it again.
 * 	== QF_COULDNT_LOCK: TRY_ONCE_LOCK has failed to acquire the lock.
 */
int qf_rebuild(const QF *qf, uint8_t flags);