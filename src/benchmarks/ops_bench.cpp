/*
 * Copyright 2018, University of California, San Diego
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *      * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *      * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *      * Neither the name of the copyright holder nor the names of its
 *        contributors may be used to endorse or promote products derived
 *        from this software without specific prior written permission.
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
 * ops_bench.cpp -- operation benchmarks
 */

#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <unistd.h>

#include "benchmark.hpp"
#include "libpangolin.h"
#include "os.h"

#define FILE_MODE 0666
#define LAYOUT_NAME "opsbench"

POBJ_LAYOUT_BEGIN(ops_bench);
POBJ_LAYOUT_TOID(ops_bench, uint8_t);
POBJ_LAYOUT_END(ops_bench);

#define OPTYPE(pa, op) (strcmp(pa->operation, op) == 0)

static unsigned scale; /* better use a local var for this (in bench_worker) */

/********************* data structures *********************/

/*
 * bench_args -- benchmark specific command line options
 */
struct bench_args {
	char *operation; /* pmemobj_tx_alloc, pmemobj_tx_write, ... */
	bool check_pool; /* check pool integrity after each run */
	bool check_user; /* check user data integrity after each run */
	unsigned n_ops_per_tx;
};

/*
 * bench_worker -- worker (thread) private data
 */
struct bench_worker {
	PMEMoid *oids;
};

/*
 * ops_bench -- benchmark specific context
 */
struct ops_bench {
	PMEMobjpool *pop;
	void *data;

	/* the actual benchmark operation */
	int (*bench_op)(PMEMobjpool *pop, void *data, PMEMoid *oids,
			size_t obj_id, size_t obj_size);
};

struct op {
	const char *name;
	int (*func)(PMEMobjpool *pop, void *data, PMEMoid *oids, size_t obj_id,
		    size_t obj_size);
};

/********************* non-atomic operations *********************/

/*
 * direct_write_op -- non-atomic direct write
 */
static int
direct_write_op(PMEMobjpool *pop, void *data, PMEMoid *oids, size_t obj_id,
		size_t obj_size)
{
	size_t words = obj_size >> 3; /* ignore trailing bytes */
	uint64_t off = oids[obj_id].off;
	uint64_t *ptr = (uint64_t *)pmemobj_direct(oids[obj_id]);

	/* do some simple work */
	for (size_t i = 0; i < words; i++)
		ptr[i] = off + i;
	pmemobj_persist(pop, ptr, obj_size);

	return 0;
}

/********************* libpmemobj operations *********************/

/*
 * pmemobj_memcpy_op -- copy from dram to pmem using pmemobj_memcpy
 */
static int
pmemobj_memcpy_op(PMEMobjpool *pop, void *data, PMEMoid *oids, size_t obj_id,
		  size_t obj_size)
{
	void *ptr = pmemobj_direct(oids[obj_id]);

	pmemobj_memcpy(pop, ptr, data, obj_size,
		       PMEMOBJ_F_MEM_NODRAIN | PMEMOBJ_F_MEM_NONTEMPORAL);

	return 0;
}

/*
 * pmemobj_tx_alloc_op -- pmemobj object allocation
 */
static int
pmemobj_tx_alloc_op(PMEMobjpool *pop, void *data, PMEMoid *oids, size_t obj_id,
		    size_t obj_size)
{
	void *uobj;
	size_t id = obj_id * scale;
	uint64_t type_num = TOID_TYPE_NUM(uint8_t);

	TX_BEGIN(pop)
	{
		for (size_t i = 0; i < scale; i++) {
			oids[id + i] = pmemobj_tx_zalloc(obj_size, type_num);
			uobj = pmemobj_direct(oids[id + i]);
			memset(uobj, 0xAB, obj_size);
		}
	}
	TX_END

	return 0;
}

/*
 * pmemobj_tx_write_op -- pmemobj transactional object write and commit
 */
static int
pmemobj_tx_write_op(PMEMobjpool *pop, void *data, PMEMoid *oids, size_t obj_id,
		    size_t obj_size)
{
	size_t words = obj_size >> 3; /* ignore trailing bytes */
	uint64_t off = oids[obj_id].off;
	uint64_t *ptr = (uint64_t *)pmemobj_direct(oids[obj_id]);

	TX_BEGIN(pop)
	{
		pmemobj_tx_add_range(oids[obj_id], 0, obj_size);
		for (size_t i = 0; i < words; i++)
			ptr[i] = off + i;
		/* changes will persist on transaction commit */
	}
	TX_END

	return 0;
}

/*
 * pmemobj_tx_free_op -- free pmemobj objects
 */
static int
pmemobj_tx_free_op(PMEMobjpool *pop, void *data, PMEMoid *oids, size_t obj_id,
		   size_t obj_size)
{
	size_t id = obj_id * scale;

	TX_BEGIN(pop)
	{
		for (size_t i = 0; i < scale; i++)
			pmemobj_tx_free(oids[id + i]);
	}
	TX_END

	return 0;
}

/********************* libpangolin operations *********************/

/*
 * pgl_tx_alloc_op -- pgl object allocation
 */
static int
pgl_tx_alloc_op(PMEMobjpool *pop, void *data, PMEMoid *oids, size_t obj_id,
		size_t obj_size)
{
	void *uobj;
	size_t id = obj_id * scale;
	uint64_t type_num = TOID_TYPE_NUM(uint8_t);

	PGL_TX_BEGIN(pop)
	{
		/* pgl_tx_alloc() is equivalent to pmemobj_tx_zalloc() */
		for (size_t i = 0; i < scale; i++) {
			uobj = pgl_tx_alloc_open(obj_size, type_num);
			memset(uobj, 0xAB, obj_size);
			oids[id + i] = pgl_oid(uobj);
		}
	}
	PGL_TX_END

	return 0;
}

#define ALIGN_256B(ptr) ((void *)(((uintptr_t)(ptr) + 255) & ~255))
#define OBJ_ALIGN_OFFSET 192

/*
 * pgl_tx_write_op -- pgl object commit
 */
static int
pgl_tx_write_op(PMEMobjpool *pop, void *data, PMEMoid *oids, size_t obj_id,
		size_t obj_size)
{
	size_t words = obj_size >> 3; /* ignore trailing bytes */
	uint64_t off = oids[obj_id].off;

	PGL_TX_BEGIN(pop)
	{
		uint64_t *ptr = (uint64_t *)pgl_tx_open(oids[obj_id]);
		
		pgl_tx_add_range(ptr, 0, obj_size);  // it's a full obj update
		// pgl_tx_add_range(ptr, OBJ_ALIGN_OFFSET, obj_size);
	
#ifdef PMEMBENCH_RANDOM
		// ptr = (uint64_t *) ALIGN_256B(ptr)-16;  // force align
		// ptr += OBJ_ALIGN_OFFSET/8;
#endif	
		// 8B write
		// for (size_t i = 0; i < words; i++)
		// 	ptr[i] = off + i;  // as random data
		memset(ptr, (int) off, obj_size);

	}
	PGL_TX_END

	return 0;
}

#ifndef RATIO
#define RATIO 0.5
#endif

/* return 1 or 0 by given probability */
int generateRandomValue(double probability) {
    int randomNum = rand();
    double normalizedRandom = (double)randomNum / RAND_MAX;
    if (normalizedRandom < probability)  return 1;  // write
	else return 0;  // read
}

/*
 * pgl_tx_rw_op -- pgl object read and write commit
 */
static int
pgl_tx_rw_op(PMEMobjpool *pop, void *data, PMEMoid *oids, size_t obj_id,
		size_t obj_size)
{
	size_t words = obj_size >> 3; /* ignore trailing bytes */
	uint64_t off = oids[obj_id].off;
	uint64_t buf[words];
	if (generateRandomValue(RATIO)) {  // write
		PGL_TX_BEGIN(pop)
		{
			uint64_t *ptr = (uint64_t *)pgl_tx_open(oids[obj_id]);
			pgl_tx_add_range(ptr, 0, obj_size);

			// 8B write
			// for (size_t i = 0; i < words; i++)
			// 	ptr[i] = off + i;
			memset(ptr, (int) off, obj_size);
		}
		PGL_TX_END
	} else {  // read
		void *ptr = pmemobj_direct(oids[obj_id]);
		memcpy(buf, ptr, obj_size);
	}

	return 0;
}

/*
 * pgl_tx_free_op -- free pmemobj objects
 */
static int
pgl_tx_free_op(PMEMobjpool *pop, void *data, PMEMoid *oids, size_t obj_id,
	       size_t obj_size)
{
	size_t id = obj_id * scale;

	PGL_TX_BEGIN(pop)
	{
		for (size_t i = 0; i < scale; i++)
			pgl_tx_free(oids[id + i]);
	}
	PGL_TX_END

	return 0;
}

/************************* xor operations *************************/

static int
memcpy_persist_op(PMEMobjpool *pop, void *data, PMEMoid *oids, size_t obj_id,
		  size_t obj_size)
{
	uint64_t *ptr = (uint64_t *)pmemobj_direct(oids[obj_id]);

	memcpy(ptr, data, obj_size);

	pmemobj_persist(pop, ptr, obj_size);

	return 0;
}

static int
normal_xor_persist_op(PMEMobjpool *pop, void *data, PMEMoid *oids,
		      size_t obj_id, size_t obj_size)
{
	size_t words = obj_size >> 3; /* ignore trailing bytes */
	uint64_t *pd = (uint64_t *)data;
	uint64_t *ptr = (uint64_t *)pmemobj_direct(oids[obj_id]);

	for (size_t i = 0; i < words; i++)
		ptr[i] ^= pd[i];

	pmemobj_persist(pop, ptr, obj_size);

	return 0;
}

static int
atomic_xor_persist_op(PMEMobjpool *pop, void *data, PMEMoid *oids,
		      size_t obj_id, size_t obj_size)
{
	size_t words = obj_size >> 3; /* ignore trailing bytes */
	uint64_t *pd = (uint64_t *)data;
	uint64_t *ptr = (uint64_t *)pmemobj_direct(oids[obj_id]);

	/*
	 * The following calls are equivalent and latencises are similar.
	 * __atomic_xor_fetch(&ptr[i], pd[i], __ATOMIC_RELAXED);
	 * __atomic_fetch_xor(&ptr[i], pd[i], __ATOMIC_RELAXED);
	 * __sync_xor_and_fetch(&ptr[i], pd[i]);
	 * __sync_fetch_and_xor(&ptr[i], pd[i]);
	 */
	for (size_t i = 0; i < words; i++)
		__atomic_xor_fetch(&ptr[i], pd[i], __ATOMIC_RELAXED);

	pmemobj_persist(pop, ptr, obj_size);

	return 0;
}

static int
pangolin_atomic_xor_op(PMEMobjpool *pop, void *data, PMEMoid *oids,
		       size_t obj_id, size_t obj_size)
{
	uint64_t *src = (uint64_t *)data;
	uint64_t *dst = (uint64_t *)pmemobj_direct(oids[obj_id]);

	pangolin_atomic_xor(pop, src, dst, dst, obj_size);

	return 0;
}

#define ALIGN_32(x) ((uint64_t)(x) & ~31UL)
#define MISALIGN_32(x) ((uint64_t)(x)&31UL)

static int
pangolin_avx_xor_op(PMEMobjpool *pop, void *data, PMEMoid *oids, size_t obj_id,
		    size_t obj_size)
{
	uint64_t *src = (uint64_t *)data;
	size_t src_misalign = MISALIGN_32(src);
	if (src_misalign)
		src = (uint64_t *)((char *)src + 32 - src_misalign);

	uint64_t *dst = (uint64_t *)pmemobj_direct(oids[obj_id]);
	size_t dst_misalign = MISALIGN_32(dst);
	if (dst_misalign)
		dst = (uint64_t *)((char *)dst + 32 - dst_misalign);

	size_t cut_size = (src_misalign < dst_misalign) ? 32 - src_misalign
							: 32 - dst_misalign;
	size_t size = ALIGN_32(obj_size - cut_size);

	pangolin_avx_xor(pop, src, dst, dst, size);

	return 0;
}

/************************* other operations *************************/

#define MISALIGN_4K(x) ((uint64_t)(x)&4095UL)

static int
pangolin_repair_page_op(PMEMobjpool *pop, void *data, PMEMoid *oids,
			size_t obj_id, size_t obj_size)
{
	void *page = pmemobj_direct(oids[obj_id]);
	size_t misalign = MISALIGN_4K(page);
	if (misalign)
		page = (void *)((char *)page + 4096 - misalign);

	pangolin_repair_page(page);

	return 0;
}

/********************* operation registrations *********************/

static struct op ops[] = {
	{"memcpy_persist", memcpy_persist_op},
	{"normal_xor_persist", normal_xor_persist_op},
	{"atomic_xor_persist", atomic_xor_persist_op},
	{"pangolin_atomic_xor", pangolin_atomic_xor_op},
	{"pangolin_avx_xor", pangolin_avx_xor_op},
	{"direct_write", direct_write_op},
	{"pmemobj_memcpy", pmemobj_memcpy_op},
	{"pmemobj_tx_alloc", pmemobj_tx_alloc_op},
	{"pmemobj_tx_write", pmemobj_tx_write_op},
	{"pmemobj_tx_free", pmemobj_tx_free_op},
	{"pgl_tx_alloc", pgl_tx_alloc_op},
	{"pgl_tx_write", pgl_tx_write_op},
	{"pgl_tx_rw", pgl_tx_rw_op},
	{"pgl_tx_free", pgl_tx_free_op},
	{"pangolin_repair_page", pangolin_repair_page_op},
};

#define NOPS (sizeof(ops) / sizeof(ops[0]))

/*
 * parse_op_type -- parses command line "--operation" argument and returns
 * proper operation type.
 */
static int
parse_op_type(const char *arg)
{
	for (unsigned i = 0; i < NOPS; i++) {
		if (strcmp(arg, ops[i].name) == 0)
			return i;
	}

	return -1;
}

/********************* benchmark init/exit *********************/

/*
 * ops_bench_init -- initialize benchmark environment
 */
static int
ops_bench_init(struct benchmark *bench, struct benchmark_args *args)
{
	assert(bench != NULL);
	assert(args != NULL);
	assert(args->opts != NULL);

	struct bench_args *pa = (struct bench_args *)args->opts;

	struct ops_bench *pb =
		(struct ops_bench *)malloc(sizeof(struct ops_bench));
	if (!pb) {
		perror("malloc() ops_bench");
		return -1;
	}

	int fid;

	/* poolsize is set to 0 to use the poolset file (args->fname) */
	pb->pop = pmemobj_create(args->fname, LAYOUT_NAME, 0, FILE_MODE);
	if (pb->pop == NULL) {
		perror("pmemobj_create()");
		goto free_pt;
	}

	/* operation type */
	fid = parse_op_type(pa->operation);
	if (fid == -1) {
		fprintf(stderr, "wrong operation: %s\n", pa->operation);
		goto free_pt;
	}
	pb->bench_op = ops[fid].func;

	pb->data = malloc(args->dsize);
	memset(pb->data, 0x01, args->dsize);

	/* set pointer to bench-specific data */
	pmembench_set_priv(bench, pb);

	return 0;

free_pt:
	free(pb);

	return -1;
}

/*
 * ops_bench_exit -- benchmark cleanup function
 */
static int
ops_bench_exit(struct benchmark *bench, struct benchmark_args *args)
{
	struct bench_args *pa = (struct bench_args *)args->opts;
	struct ops_bench *pb = (struct ops_bench *)pmembench_get_priv(bench);

	assert(pb != NULL);
	assert(pb->pop != NULL);

	if (pa->check_pool) {
		if (pangolin_check_pool(pb->pop, 0) != 0)
			fprintf(stderr,
				RED "pangolin pool check failed; "
				    "see details with debug build\n" RST);
	}
	if (pa->check_user) {
		if (pangolin_check_objs(pb->pop, 0) != 0)
			fprintf(stderr,
				RED "pangolin objs check failed; "
				    "see details with debug build\n" RST);
	}

	pmemobj_close(pb->pop);

	free(pb->data);
	free(pb);

	return 0;
}

/********************* worker functions *********************/

/*
 * ops_bench_init_worker -- worker initialization
 */
static int
ops_bench_init_worker(struct benchmark *bench, struct benchmark_args *args,
		      struct worker_info *worker)
{
	struct bench_args *pa = (struct bench_args *)args->opts;
	struct ops_bench *pb = (struct ops_bench *)pmembench_get_priv(bench);
	struct bench_worker *pw =
		(struct bench_worker *)calloc(1, sizeof(struct bench_worker));
	if (pw == NULL) {
		perror("calloc() worker");
		return -1;
	}

	size_t i, size = args->dsize;
	uint64_t type = TOID_TYPE_NUM(uint8_t);

	scale = pa->n_ops_per_tx;
	size_t nobjs = args->n_ops_per_thread * scale;

	pw->oids = (PMEMoid *)calloc(nobjs, sizeof(PMEMoid));
	if (pw->oids == NULL) {
		perror("calloc() oids");
		goto free_worker;
	}

	/* do not allocate objects if the target operation is obj_alloc */
	if (OPTYPE(pa, "pmemobj_tx_alloc") || OPTYPE(pa, "pgl_tx_alloc"))
		goto done;
	
#ifdef PMEMBENCH_RANDOM
	// size+=256;
#endif
	for (i = 0; i < nobjs; i++) {
		PGL_TX_BEGIN(pb->pop)
		{
			pw->oids[i] = pgl_tx_alloc(size, type);
		}
		PGL_TX_END
		if (OID_IS_NULL(pw->oids[i])) {
			perror("worker objects allocation");
			goto free_oids;
		}
	}

done:
	worker->priv = pw;

	return 0;

free_oids:
	for (; i > 0; i--) {
		pmemobj_free(&pw->oids[i - 1]);
	}
	free(pw->oids);
free_worker:
	free(pw);

	return -1;
}

/*
 * ops_bench_free_worker -- worker exit function
 */
static void
ops_bench_free_worker(struct benchmark *bench, struct benchmark_args *args,
		      struct worker_info *worker)
{
	struct bench_args *pa = (struct bench_args *)args->opts;
	struct bench_worker *pw = (struct bench_worker *)worker->priv;

	size_t words = (args->dsize) >> 3; /* ignore trailing bytes */
	int do_check =
		OPTYPE(pa, "pmemobj_tx_write") || OPTYPE(pa, "pgl_tx_write");

	if (pa->check_user && do_check) {
		int error = 0; /* result verification */
		for (size_t i = 0; i < args->n_ops_per_thread; i++) {
			uint64_t off = pw->oids[i].off;
			uint64_t *ptr = (uint64_t *)pmemobj_direct(pw->oids[i]);
			for (size_t j = 0; j < words; j++) {
				if (ptr[j] != off + j) {
					error++;
					break;
				}
			}
		}

		if (error > 0)
			fprintf(stderr, "%d objects found errors\n", error);
	}

	/*
	if (!OPTYPE(pa, "pmemobj_tx_free") && !OPTYPE(pa, "pgl_tx_free")) {
		for (size_t i = 0; i < args->n_ops_per_thread; i++) {
			pmemobj_free(&pw->oids[i]);
		}
	}
	*/

	free(pw->oids);
	free(pw);
}

/*
 * bench_worker_op -- actual benchmark operation
 */
static int
bench_worker_op(struct benchmark *bench, struct operation_info *info)
{
	struct ops_bench *pb = (struct ops_bench *)pmembench_get_priv(bench);
	struct bench_worker *pw = (struct bench_worker *)info->worker->priv;
	size_t obj_id = info->index;
	size_t obj_size = info->args->dsize;

	if (pb->bench_op(pb->pop, pb->data, pw->oids, obj_id, obj_size) != 0)
		return -1;

	return 0;
}

/********************* benchmark registration *********************/

/* structure to define command line arguments */
static struct benchmark_clo ops_bench_clos[4];
/* information about benchmark. */
static struct benchmark_info ops_bench_info;
CONSTRUCTOR(ops_bench_constructor)
void
ops_bench_constructor(void)
{
	ops_bench_clos[0].opt_short = 'o';
	ops_bench_clos[0].opt_long = "operation";
	ops_bench_clos[0].descr = "Operation name - pmemobj_tx_alloc, ... ";
	ops_bench_clos[0].type = CLO_TYPE_STR;
	ops_bench_clos[0].off = clo_field_offset(struct bench_args, operation);
	ops_bench_clos[0].def = "pmemobj_tx_write";

	ops_bench_clos[1].opt_long = "check-pool";
	ops_bench_clos[1].descr = "Check pool integrity after each run";
	ops_bench_clos[1].type = CLO_TYPE_FLAG;
	ops_bench_clos[1].off = clo_field_offset(struct bench_args, check_pool);
	ops_bench_clos[1].def = "false";

	ops_bench_clos[2].opt_long = "check-user";
	ops_bench_clos[2].descr = "Check user data integrity after each run";
	ops_bench_clos[2].type = CLO_TYPE_FLAG;
	ops_bench_clos[2].off = clo_field_offset(struct bench_args, check_user);
	ops_bench_clos[2].def = "false";

	ops_bench_clos[3].opt_long = "ops-per-tx";
	ops_bench_clos[3].descr = "Operations per TX";
	ops_bench_clos[3].off =
		clo_field_offset(struct bench_args, n_ops_per_tx);
	ops_bench_clos[3].type = CLO_TYPE_UINT;
	ops_bench_clos[3].def = "1";
	ops_bench_clos[3].type_uint.size =
		clo_field_size(struct bench_args, n_ops_per_tx);
	ops_bench_clos[3].type_uint.base = CLO_INT_BASE_DEC;
	ops_bench_clos[3].type_uint.min = 0;
	ops_bench_clos[3].type_uint.max = UINT_MAX;

	ops_bench_info.name = "ops_bench";
	ops_bench_info.brief = "Benchmark for operations";
	ops_bench_info.init = ops_bench_init;
	ops_bench_info.exit = ops_bench_exit;
	ops_bench_info.multithread = true;
	ops_bench_info.multiops = true;
	ops_bench_info.init_worker = ops_bench_init_worker;
	ops_bench_info.free_worker = ops_bench_free_worker;
	ops_bench_info.operation = bench_worker_op;
	ops_bench_info.measure_time = true;
	ops_bench_info.clos = ops_bench_clos;
	ops_bench_info.nclos = ARRAY_SIZE(ops_bench_clos);
	ops_bench_info.opts_size = sizeof(struct bench_args);
	ops_bench_info.rm_file = true;
	ops_bench_info.allow_poolset = true;
	REGISTER_BENCHMARK(ops_bench_info);
};
