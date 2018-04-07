#ifndef ZIPF_H
#define ZIPF_H

/* Zipfian number generator.
 * Goals: Fast (10M numbers/s)
 *        Configurable exponent.
 *        Capable of generating at least 2^{32} (4 billion) distinct numbers.
 * There are two parameters:
 *   s the characteristic exponent, and
 *   N the number of elements in the universe.
 * Once created, this data structure is read-only, and can be used in a multithreaded fashion.  This code
 *  calls random(), which is generally multihread-safe these days.
 *
 * Copyright 2011 Bradley C. Kuszmaul 
 */

#include <inttypes.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct zipfian const *ZIPFIAN;
ZIPFIAN create_zipfian (double s, long N, long int (*randomfun)(void));
// Effect: Create a generator of zipfian numbers.

void destroy_zipfian (const ZIPFIAN);
// Effect; Destroy the zipfian generator (freeing all it's memory, for example).

long zipfian_gen (const ZIPFIAN);
// Effect: return a number from 0 (inclusive) to N (exlusive) with probability distribution approximately as follows.
//   $k-1$ is returned with probability  $1/(k^s H_{N,s})$
//   where $H_{N,s}$ is the $N$th generalized harmonic number $\sum_{n=1}^{N} 1/n^s$.

long zipfian_hash (const ZIPFIAN);
// Effect: Return a random 64-bit number.  The numbers themselves are uniform hashes of the numbers from 0 (inclusive) to N (exclusive)

void generate_random_keys (uint64_t *elems, long N, long gencount, double s);

#ifdef __cplusplus
}
#endif

#endif
