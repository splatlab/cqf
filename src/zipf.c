/* See zipf.h for the specification of this file.
 * Copyright 2011 Bradley C. Kuszmaul
 */

#include "include/zipf.h"
#include <math.h>
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>

#ifndef  USE_MYRANDOM
#define RFUN random
#define RSEED srandom
#else
#define RFUN myrandom
#define RSEED mysrandom

static unsigned int m_z = 1;
static unsigned int m_w = 1;
static void mysrandom (unsigned int seed) {
	m_z = seed;
	m_w = (seed<<16) + (seed >> 16);
}

static long myrandom()
{
	m_z = 36969 * (m_z & 65535) + (m_z >> 16);
	m_w = 18000 * (m_w & 65535) + (m_w >> 16);
	return ((m_z << 16) + m_w) % 0x7FFFFFFF;
}
#endif

struct zpair {                   // For the ith element of the array:
    long num;                    //   How many elements are represented by this bucket
    long low;                    //   How many elements are represented by all the previous buckets.
    double cumulative;           //   The sum of the all the probabilities of all the elements represented by previous buckets.
};

enum { NPAIRS = 1000000 };

struct zipfian {
    double s;                    // s, the characteristic exponent.
    long N;                      // N, the size of the universe.
    double H_Ns;                 // H_{N,s}.
    long int (*randomfun)(void);
    struct zpair pairs[NPAIRS]; 
};

static void zprint (ZIPFIAN z) {
    int i = 0;
    printf("s=%f, N=%ld, H_sN=%f\n", z->s, z->N, z->H_Ns);
    for (i=0; i<NPAIRS; i++) {
	double diff =    ((i+1>=NPAIRS) ? z->H_Ns : z->pairs[i+1].cumulative) - z->pairs[i].cumulative;
	printf("%2ld %2ld %f (delta=%f)\n", z->pairs[i].num, z->pairs[i].low, z->pairs[i].cumulative, diff);
    }
}

ZIPFIAN create_zipfian (double s, long N, long int (*randomfun)(void)) {
    assert(s > 0);
    assert(0 < N);
    struct zipfian *z = (struct zipfian *)malloc(sizeof(*z));
    assert(z);
    z->s = s;
    z->N = N;
    z->randomfun = randomfun;

    // Calculate the total probability distribution
    double H_Ns = 0;
    long i = 0;
    for (i=0; i<N; i++) {
	H_Ns += pow(i+1, -s);
    }

    // For the first half of the pairs do things exactly
    double cumulative = 0;
    for (i=0; i<NPAIRS/2; i++) {
	z->pairs[i] = (struct zpair){.cumulative = cumulative,
				     .low        = i,
				     .num        = 1};
	cumulative += pow(i+1, -s);
    }
    // For the second half divide up the remaining part of N evenly by the probability.
    

    long last_n = NPAIRS/2;
    long next_n = last_n;

    for (i=NPAIRS/2; i<NPAIRS; i++) {
	double remaining_probability = H_Ns - cumulative;
	double next_target = cumulative + remaining_probability/(NPAIRS-i);
	double next_cumulative = cumulative;
	while (next_n < N && next_cumulative < next_target) {
	    next_cumulative += pow(next_n+1, -s);
	    next_n++;
	}
	z->pairs[i] = (struct zpair){.cumulative = cumulative,
				     .low        = last_n,
				     .num        = next_n - last_n};
	last_n = next_n;
	cumulative = next_cumulative;
    }
    z->H_Ns = H_Ns;

    if (0) zprint(z);

    return z;
}

static long z_search (ZIPFIAN s, double C, long low, long pcount) 
// Find the first zpair for which the cumulative probability of the previous pairs is less than C.
// Generate a value in its range uniformly randomly.
{
    assert(pcount>0);
    if (pcount==1) {
	struct zpair const *p = &s->pairs[low];
	assert(p->cumulative <= C);
	return p->low + s->randomfun()%p->num;	
    } else {
	long mid = low + pcount/2;
	struct zpair const *p = &s->pairs[mid];
	if (p->cumulative > C) {
	    return z_search(s, C, low, pcount/2);
	} else {
	    return z_search(s, C, low+pcount/2, pcount-pcount/2);
	}
    }
}

long zipfian_gen (ZIPFIAN z) {
    // we're going to have to use two calls to random() to get enough random bits.
    const long rand_limit = ((long)RAND_MAX)+1;
    const double one_over = 1/(double)rand_limit;
    const double scale_factor = one_over * one_over;
    long v = (long)(z->randomfun()) * rand_limit + z->randomfun();
    double scaled = v * z->H_Ns * scale_factor;
    return z_search(z, scaled, 0, NPAIRS);
}

void destroy_zipfian (ZIPFIAN z) {
    free((struct zipfian *)z);
}

void generate_random_keys (uint64_t *elems, long N, long gencount, double s) {
	int i;
	uint32_t *counts;
	/*struct timeval a,b,c;*/
	printf("Generating %ld elements in universe of %ld items with characteristic exponent %f\n",
				 gencount, N, s);
	/*gettimeofday(&a, NULL);*/
	ZIPFIAN z = create_zipfian(s, N, RFUN);
	counts = (uint32_t*)calloc(N, sizeof(counts));

	/*gettimeofday(&b, NULL);*/
	/*printf("Setup time    = %0.6fs\n", tdiff(&a, &b));*/
	for (i=0; i<gencount; i++) {
		long g = zipfian_gen(z);
		assert(0<=g && g<N);
		counts[g]++;
		elems[i] = g;
	}
	/*gettimeofday(&c, NULL);*/
	/*double rtime = tdiff(&b, &c);*/
	/*printf("Generate time = %0.6fs (%f per second)\n", rtime, gencount/rtime);*/
	if (0) {
		for (i=0; i<N; i++) {
			printf("%4.1f (%4.1f)\n", counts[0]/(double)counts[i],
						 i/(counts[0]/(double)counts[i]));
				/*printf("%d ", counts[i]);*/
		}
		printf("\n");
	}
	destroy_zipfian(z);
}

