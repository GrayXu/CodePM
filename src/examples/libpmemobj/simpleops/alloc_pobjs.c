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
 * alloc_pobjs.c -- allocate objects in pmem
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpmemobj.h>
#include <libpangolin.h>

#include "layout.h"

static const char *opmodes[] =
	{"pmemobj_zalloc", "pmemobj_tx_zalloc", "pgl_tx_alloc"};

static PMEMoid
pmemobj_zalloc_op(PMEMobjpool *pop, size_t obj_size, uint64_t type)
{
	PMEMoid oid = OID_NULL;

	int err = pmemobj_zalloc(pop, &oid, obj_size, type);
	if (err != 0)
		printf("pmemobj_zalloc()\n");

	return oid;
}

static PMEMoid
pmemobj_tx_zalloc_op(PMEMobjpool *pop, size_t obj_size, uint64_t type)
{
	PMEMoid oid = OID_NULL;

	TX_BEGIN(pop) {
		oid = pmemobj_tx_zalloc(obj_size, type);
	} TX_END

	return oid;
}

static PMEMoid
pgl_tx_alloc_op(PMEMobjpool *pop, size_t obj_size, uint64_t type)
{
	PMEMoid oid;

	PGL_TX_BEGIN(pop) {
		/* pgl_tx_alloc() is equivalent to pmemobj_tx_zalloc() */
		oid = pgl_tx_alloc(obj_size, type);
	} PGL_TX_END

	return oid;
}

int
main(int argc, char *argv[])
{
	if (argc != 5) {
		printf("%s pool-file num-objs obj-size mode\n", argv[0]);
		return EINVAL;
	}

	size_t num_objs = strtoul(argv[2], NULL, 0);
	size_t obj_size = strtoul(argv[3], NULL, 0);
	uint64_t opmode = strtoul(argv[4], NULL, 0);

	PMEMoid (*alloc_op)(PMEMobjpool *pop, size_t obj_size, uint64_t type);

	switch (opmode) {
	case 0:
		alloc_op = pmemobj_zalloc_op;
		break;
	case 1:
		alloc_op = pmemobj_tx_zalloc_op;
		break;
	case 2:
		alloc_op = pgl_tx_alloc_op;
		break;
	default:
		printf("invalid alloc mode\n");
		return EINVAL;
	}

	PMEMobjpool *pop = pmemobj_create(argv[1], LAYOUT_NAME, 0, 0666);
	if (pop == NULL) {
		printf("pmemobj_create()\n");
		return 1;
	}

	printf("Created poolset specified by %s.\n", argv[1]);

	/* allocate objects */
	PMEMoid oid;
	for (size_t i = 0; i < num_objs; i++) {
		oid = alloc_op(pop, obj_size, TOID_TYPE_NUM(uint8_t));
		if (OID_IS_NULL(oid)) {
			printf("%s\n", opmodes[opmode]);
			goto out;
		}
	}

	printf("Allocated %zu objects, each %zu bytes, %s mode.\n",
		num_objs, obj_size, opmodes[opmode]);

	pgl_print_stats();
out:

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
