/*
 * ============================================================================
*
 *         Author:  Prashant Pandey (), ppandey@cs.stonybrook.edu
 *   Organization:  Stony Brook University
 *
 * ============================================================================
 */

#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <sched.h>
#include <linux/unistd.h>
#include <sys/syscall.h>
#include <errno.h>

#include "partitioned_counter.h"

#include	<stdlib.h>

#define INC_TO 500000 // one million...
#define DEC_TO INC_BY/2 // one million...
#define INC_BY 100 // one million...

void *thread_routine(void *arg) {
	pc_t *pc_counter = (pc_t*)arg;

	for (int i = 0; i < INC_TO; i++) {
		pc_add(pc_counter, INC_BY);
	}
	for (int i = 0; i < DEC_TO; i++) {
		pc_add(pc_counter, -INC_BY);
	}

	return NULL;
}

/* 
 * ===  FUNCTION  =============================================================
 *         Name:  main
 *  Description:  
 * ============================================================================
 */
int main (int argc, char *argv[])
{
	int64_t global_counter = 0;
	pc_t pc_counter;
	if (argc < 2) {
		printf("Specify the number of threads.\n");
		return 1;
	}
	int procs = atoi(argv[1]);

	pc_init(&pc_counter, &global_counter, 8, 100);

	pthread_t *thrs = malloc( sizeof( pthread_t ) * procs);
	if (thrs == NULL)
	{
		perror( "malloc" );
		return -1;
	}
	printf( "Starting %d threads...\n", procs );

	for (int i = 0; i < procs; i++) {
		if (pthread_create(&thrs[i], NULL, thread_routine, (void *)(&pc_counter)))
		{
			perror( "pthread_create" );
			procs = i;
			break;
		}
	}
	for (int i = 0; i < procs; i++)
		pthread_join( thrs[i], NULL );

	pc_sync(&pc_counter);

	free(thrs);

	printf("After doing all the math, global_int value is: %ld\n",
				 global_counter);
	int64_t exp_count = (INC_TO - DEC_TO) * INC_BY * procs;
	printf("Expected value is: %ld\n", exp_count);
	if (global_counter != exp_count)
		printf("Counting failed!\n");
	else
		printf("Counting passed!\n");
	return EXIT_SUCCESS;
}				/* ----------  end of function main  ---------- */
