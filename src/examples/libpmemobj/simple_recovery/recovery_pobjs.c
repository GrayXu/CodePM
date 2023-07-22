/*
 * recovery_pobjs.c -- read pmem objects and verify data integrity
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpmemobj.h>
#include <libpangolin.h>
#include "layout.h"

#define K 9
#define PAGE_SIZE 4096

double timeval_diff(struct timespec *start, struct timespec *finish);

int
main(int argc, char *argv[])
{
	struct timespec start, end;
	double runtime;

	if (argc != 5) {
		printf("%s pool-file num-objs obj-size num-thread num-parity\n", argv[0]);
		return EINVAL;
	}

	// size_t num_objs = strtoul(argv[2], NULL, 0);
	// size_t obj_size = strtoul(argv[3], NULL, 0);
	size_t num_thread = strtoul(argv[4], NULL, 0);
	size_t num_parity = strtoul(argv[5], NULL, 0);

	PMEMobjpool *pop = pmemobj_open(argv[1], LAYOUT_NAME);

	if (pop == NULL) {
		printf("pmemobj_open()\n");
		return 1;
	}

	// PMEMoid *oids = (PMEMoid *)calloc(num_objs, sizeof(PMEMoid)); // oid list
	// size_t objs = 0;
	// TOID(uint8_t) toid;
	// size_t count = 0;
	// POBJ_FOREACH_TYPE(pop, toid) {
	// 	oids[count++] = toid.oid;
	// }
	// if (count != num_objs) {
	// 	printf("count %zu != num_objs %zu\n", count, num_objs);
	// 	exit(-1);
	// }

	// pgl_print_stats();

	clock_gettime(CLOCK_MONOTONIC, &start);
	if (pangolin_scan(pop, 0, num_thread, num_parity) != 0)
		printf(RED "pangolin pool check failed\n" RST);
	clock_gettime(CLOCK_MONOTONIC, &end);
	runtime = timeval_diff(&start, &end);
	printf("scan pool time: %f\n", runtime);

// close_pool:
	// /* check pool */
	// clock_gettime(CLOCK_MONOTONIC, &start);
	// if (pangolin_check_pool(pop, 0) != 0)
	// 	printf(RED "pangolin pool check failed\n" RST);
	// clock_gettime(CLOCK_MONOTONIC, &end);
	// runtime = timeval_diff(&start, &end);
	// printf("check pool time: %f\n", runtime);

	// /* check objects */
	// clock_gettime(CLOCK_MONOTONIC, &start);
	// if (pangolin_check_objs(pop, 0) != 0)
	// 	printf(RED "pangolin objs check failed\n" RST);
	// clock_gettime(CLOCK_MONOTONIC, &end);
	// runtime = timeval_diff(&start, &end);
	// printf("check objs time: %f\n", runtime);
	pgl_print_stats();
	pmemobj_close(pop);

	return 0;
}

double timeval_diff(struct timespec *start, struct timespec *finish) {
    double total_t = (finish->tv_sec - start->tv_sec);
    total_t += (finish->tv_nsec - start->tv_nsec) / 1000000000.0;
    return total_t;
}