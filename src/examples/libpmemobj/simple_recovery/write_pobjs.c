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
 * write_pobjs.c -- write to pmem objects
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpmemobj.h>
#include <libpangolin.h>

#include "layout.h"

static const char *opmodes[] = {"non-atomic", "pmemobj_tx", "pgl_tx"};

static void
direct_write_op(PMEMobjpool *pop, PMEMoid *oidp, size_t objs, size_t size)
{
	for (size_t i = 0; i < objs; i++) {
		uint64_t *pobj = pmemobj_direct(oidp[i]);
		uint64_t obj_offset = oidp[i].off;
		for (size_t i = 0; i < size >> 3; i++)
			pobj[i] = obj_offset + i;
		pmemobj_persist(pop, pobj, size);
	}
}

static void
pmemobj_tx_write_op(PMEMobjpool *pop, PMEMoid *oidp, size_t objs, size_t size)
{
	TX_BEGIN(pop) {
		for (size_t i = 0; i < objs; i++) {
			uint64_t *pobj = pmemobj_direct(oidp[i]);
			uint64_t obj_offset = oidp[i].off;
			pmemobj_tx_add_range(oidp[i], 0, size);
			for (size_t j = 0; j < size >> 3; j++)
				pobj[j] = obj_offset + j;
		}
	} TX_END
}

static void
pgl_tx_write_op(PMEMobjpool *pop, PMEMoid *oidp, size_t objs, size_t size)
{
	PGL_TX_BEGIN(pop) {
		for (size_t i = 0; i < objs; i++) {
			uint64_t *pobj = pgl_tx_open(oidp[i]);
			uint64_t obj_offset = oidp[i].off;
			for (size_t j = 0; j < size >> 3; j++)
				pobj[j] = obj_offset + j;
		}
	} PGL_TX_END
}

static void
pgl_tx_write_one(PMEMobjpool *pop, PMEMoid *oidp, size_t objs, size_t size)
{
	uint64_t *pobj = pgl_open(pop, *oidp);
	uint64_t obj_offset = oidp->off;
	for (size_t j = 0; j < size >> 3; j++)
		pobj[j] = obj_offset + j;
	pgl_commit(pobj);
}

int
main(int argc, char *argv[])
{
	if (argc != 6) {
		printf("%s pool-file num-objs op-objs obj-size mode\n",
			argv[0]);
		return EINVAL;
	}

	size_t num_objs = strtoul(argv[2], NULL, 0);
	size_t objs_per_op = strtoul(argv[3], NULL, 0);
	size_t obj_size = strtoul(argv[4], NULL, 0);
	uint64_t opmode = strtoul(argv[5], NULL, 0);

	void (*write_op)(PMEMobjpool *pop, PMEMoid *oidp, size_t objs,
		size_t size);

	switch (opmode) {
	case 0:
		write_op = direct_write_op;
		break;
	case 1:
		write_op = pmemobj_tx_write_op;
		break;
	case 2:
		write_op = objs_per_op == 1 ?
			pgl_tx_write_one : pgl_tx_write_op;
		break;
	default:
		printf("Error: invalid write mode\n");
		return EINVAL;
	}

	PMEMobjpool *pop = pmemobj_open(argv[1], LAYOUT_NAME);
	if (pop == NULL) {
		printf("pmemobj_open()\n");
		return 1;
	}

	PMEMoid *oids = (PMEMoid *)calloc(num_objs, sizeof(PMEMoid));
	if (oids == NULL) {
		printf("calloc()\n");
		goto close_pool;
	}

	size_t count = 0;
	TOID(uint8_t) toid;

	POBJ_FOREACH_TYPE(pop, toid) {
		oids[count++] = toid.oid;
	}

	if (count != num_objs) {
		printf("count %zu != num_objs %zu\n", count, num_objs);
		goto free_oids;
	}

	struct timespec start, end;

	PMEMoid *oidp = oids;
	size_t nops = num_objs / objs_per_op;

	clock_gettime(CLOCK_MONOTONIC, &start);
	for (size_t i = 0; i < nops; i++) {
		write_op(pop, oidp, objs_per_op, obj_size);
		oidp += objs_per_op;
	}
	clock_gettime(CLOCK_MONOTONIC, &end);

	uint64_t runtime = (end.tv_sec - start.tv_sec) * NSECPSEC +
		end.tv_nsec - start.tv_nsec;
	uint64_t optime = runtime / nops;

	printf("Wrote to %zu objects, %zu per op, each %zu bytes, %s mode.\n",
		num_objs, objs_per_op, obj_size, opmodes[opmode]);

	printf("Run time = %ld ns, time per operation %ld.\n", runtime, optime);

	pgl_print_stats();

free_oids:
	free(oids);

close_pool:
	// if (pangolin_check_pool(pop, 0) != 0)
	// 	printf(RED "pangolin pool check failed\n" RST);
	// else
	// 	printf(GRN "pangolin pool check passed\n" RST);

	// if (pangolin_check_objs(pop, 0) != 0)
	// 	printf(RED "pangolin objs check failed\n" RST);
	// else
	// 	printf(GRN "pangolin objs check passed\n" RST);

	pmemobj_close(pop);

	return 0;
}
