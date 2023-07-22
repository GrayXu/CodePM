/*
 * Copyright 2018, University of California, San Diego
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * read_pobjs.c -- read pmem objects and verify data integrity
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpmemobj.h>
#include <libpangolin.h>

#include "layout.h"

/*
 * obj_check -- check object data integrity
 */
static int
obj_check(PMEMobjpool *pop, PMEMoid oid, size_t obj_size)
{
	size_t errors = 0;
	uint64_t *pobj = pmemobj_direct(oid);
	uint64_t obj_offset = oid.off;

	if (obj_offset == 0) {
		printf("invalid object offset\n");
		return 1;
	}

	for (size_t i = 0; i < obj_size >> 3; i++)
		if (pobj[i] != obj_offset + i)
			errors += 1;

	/*
	 * if (errors > 0)
	 * printf("obj @0x%lx has %zu errors\n", oid.off, errors);
	 */

	return errors;
}

int
main(int argc, char *argv[])
{
	uint64_t runtime;
	struct timespec start, end;

	if (argc != 3) {
		printf("%s pool-file obj-size\n", argv[0]);
		return EINVAL;
	}

	char *poolfile = argv[1];
	size_t obj_size = strtoul(argv[2], NULL, 0);

	PMEMobjpool *pop = pmemobj_open(argv[1], LAYOUT_NAME);
	if (pop == NULL) {
		printf("pmemobj_open()\n");
		return 1;
	}

	size_t objs = 0, errs = 0;
	TOID(uint8_t) toid;

	clock_gettime(CLOCK_MONOTONIC, &start);
	POBJ_FOREACH_TYPE(pop, toid) {
		objs++;

		/* obj_check only works if the pool is written by obj_write */
		if (strcmp(poolfile, "./pobjpool.set") != 0)
			continue;

		if (obj_check(pop, toid.oid, obj_size) != 0)
			errs++;
	}
	clock_gettime(CLOCK_MONOTONIC, &end);
	runtime = (end.tv_sec - start.tv_sec) * NSECPSEC + end.tv_nsec - start.tv_nsec;
	printf("Read %zu objects.\n", objs);
	printf("read objects time: %ld\n", runtime);

	if (errs > 0)
		printf("%zu/%zu objects have errors.\n", errs, objs);
	else
		printf("All %zu objects read correct.\n", objs);

	pgl_print_stats();

	if (pangolin_check_pool(pop, 0) != 0)
		printf(RED "pangolin pool check failed\n" RST);
	else
		printf(GRN "pangolin pool check passed\n" RST);

	if (pangolin_check_objs(pop, 0) != 0)
		printf(RED "pangolin objs check failed\n" RST);
	else
		printf(GRN "pangolin objs check passed\n" RST);

	pmemobj_close(pop);

	return 0;
}
