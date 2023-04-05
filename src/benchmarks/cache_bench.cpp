/*
 * Copyright 2017, University of California, San Diego
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
 * cache_bench.cpp -- cache-related benchmarks
 */

#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <immintrin.h>
#include <numa.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <unistd.h>

#include "benchmark.hpp"
#include "libpmem.h"

#define CL_SIZE (64)
#define CL_MASK (CL_SIZE - 1)
#define CL_ALIGNED(p) (((uintptr_t)p & CL_MASK) == 0)

/*
 * cache_bench_args -- benchmark specific command line options
 */
struct cache_bench_args {
	char *operation;  /* memcpy, clflush, movnt_sse2, ... */
	unsigned unalign; /* unalignemnt in bytes */
};

/*
 * cache_bench -- benchmark specific context
 */
struct cache_bench {
	void *nowhere;
	size_t poolsize;
	uintptr_t **src;
	uintptr_t **dst;
	unsigned nthreads;

	/* the actual benchmark operation */
	void (*cache_op)(struct cache_bench *cb, size_t len, unsigned tid);
};

/*
 * memcpy_func -- memcpy local src to local dst
 */
void
memcpy_func(struct cache_bench *cb, size_t len, unsigned tid)
{
	memcpy(cb->dst[tid], cb->src[tid], len);
}

/*
 * memcpy_xdst -- memcpy to next thread's memory
 */
void
memcpy_xdst(struct cache_bench *cb, size_t len, unsigned tid)
{
	unsigned xtid = (tid == cb->nthreads - 1) ? 0 : tid + 1;
	if (cb->dst[xtid] != NULL)
		memcpy(cb->dst[xtid], cb->src[tid], len);
}

/*
 * clflush_func -- execute clflush on the dst memory
 */
void
clflush_func(struct cache_bench *cb, size_t len, unsigned tid)
{
	nvsl_clflush(cb->dst[tid], len);
}

/*
 * memcpy_clflush_src -- memcpy(dst, src, len) and then execute clflush on src
 *
 * Evaluate clflush on clean cache lines.
 */
void
memcpy_clflush_src(struct cache_bench *cb, size_t len, unsigned tid)
{
	memcpy(cb->dst[tid], cb->src[tid], len); /* local memcpy */
	nvsl_clflush(cb->src[tid], len);	 /* local clflush */
}

/*
 * memcpy_clflush_dst -- memcpy(dst, src, len) and then execute clflush on dst
 *
 * Evaluate clflush on dirty cache lines.
 */
void
memcpy_clflush_dst(struct cache_bench *cb, size_t len, unsigned tid)
{
	memcpy(cb->dst[tid], cb->src[tid], len); /* local memcpy */
	nvsl_clflush(cb->dst[tid], len);	 /* local clflush */
}

/*
 * memcpy_clflushopt_dst -- memcpy(dst, src, len) and then execute clflushopt
 * on dst
 *
 * Evaluate clflushopt on dirty cache lines.
 */
void
memcpy_clflushopt_dst(struct cache_bench *cb, size_t len, unsigned tid)
{
	memcpy(cb->dst[tid], cb->src[tid], len); /* local memcpy */
	nvsl_clflushopt(cb->dst[tid], len);      /* local clflushopt */
}

/*
 * memcpy_clwb_dst -- memcpy(dst, src, len) and then execute clwb on dst
 *
 * Evaluate clwb on dirty cache lines.
 */
void
memcpy_clwb_dst(struct cache_bench *cb, size_t len, unsigned tid)
{
	memcpy(cb->dst[tid], cb->src[tid], len); /* local memcpy */
	nvsl_clwb(cb->dst[tid], len);		 /* local clwb */
}

/*
 * memcpy_clflush_xsrc -- memcpy(dst, src, len) and then execute clflush on the
 * next thread's src
 *
 * Evaluate clflush between cores. Run it with thread-affinity.
 */
void
memcpy_clflush_xsrc(struct cache_bench *cb, size_t len, unsigned tid)
{
	unsigned xtid = (tid == cb->nthreads - 1) ? 0 : tid + 1;
	memcpy(cb->dst[tid], cb->src[tid], len); /* local memcpy */
	if (cb->src[xtid] != NULL)
		nvsl_clflush(cb->src[xtid], len); /* remote clflush */
}

/*
 * memcpy_clflush_xdst -- memcpy(dst, src, len) and then execute clflush on the
 * next thread's dst
 *
 * Evaluate clflush between cores. Run it with thread-affinity.
 */
void
memcpy_clflush_xdst(struct cache_bench *cb, size_t len, unsigned tid)
{
	unsigned xtid = (tid == cb->nthreads - 1) ? 0 : tid + 1;
	memcpy(cb->dst[tid], cb->src[tid], len); /* local memcpy */
	if (cb->dst[xtid] != NULL)
		nvsl_clflush(cb->dst[xtid], len); /* remote clflush */
}

/*
 * memcpy_xdst_clflush_dst -- memcpy to next thread's dst and then use clflush
 * on local dst
 */
void
memcpy_xdst_clflush_dst(struct cache_bench *cb, size_t len, unsigned tid)
{
	unsigned xtid = (tid == cb->nthreads - 1) ? 0 : tid + 1;
	if (cb->dst[xtid] != NULL)
		memcpy(cb->dst[xtid], cb->src[tid], len);
	nvsl_clflush(cb->dst[tid], len);
}

/*
 * memcpy_clflush_nowhere -- memcpy(dst, src, len) and then execute clflush on
 * a memory range not present in any processor cache
 */
void
memcpy_clflush_nowhere(struct cache_bench *cb, size_t len, unsigned tid)
{
	memcpy(cb->dst[tid], cb->src[tid], len); /* local memcpy */
	nvsl_clflush(cb->nowhere, len);		 /* nowhere clflush */
}

/*
 * movnt_sse2_func -- execute memory copy using 128-bit non-temporal stores
 */
void
movnt_sse2_func(struct cache_bench *cb, size_t len, unsigned tid)
{
	nvsl_memmove_sse2(cb->dst[tid], cb->src[tid], len);
}

/*
 * movnt_avx_func -- execute memory copy using 256-bit non-temporal stores
 */
void
movnt_avx_func(struct cache_bench *cb, size_t len, unsigned tid)
{
	nvsl_memmove_avx(cb->dst[tid], cb->src[tid], len);
}

/*
 * memcpy_movnt_sse2 -- first run regular memcpy to a local dst, and then use
 * movnt to copy to the same location
 */
void
memcpy_movnt_sse2(struct cache_bench *cb, size_t len, unsigned tid)
{
	memcpy(cb->dst[tid], cb->src[tid], len); /* local memcpy */
	nvsl_memmove_sse2(cb->dst[tid], cb->src[tid], len);
}

/*
 * memcpy_xdst_movnt_sse2 -- first run regular memcpy to copy local src to next
 * thread's dst, and then use movnt to copy to the local thread's dst
 */
void
memcpy_xdst_movnt_sse2(struct cache_bench *cb, size_t len, unsigned tid)
{
	unsigned xtid = (tid == cb->nthreads - 1) ? 0 : tid + 1;
	if (cb->dst[xtid] != NULL)
		memcpy(cb->dst[xtid], cb->src[tid], len); /* other memcpy */
	nvsl_memmove_sse2(cb->dst[tid], cb->src[tid], len);
}

/*
 * mmap_anon -- mmap anonymous to allocate page-aligned dram buffer
 *
 * Since malloc-returned pointer may not be cache-line aligned and may cause
 * nvsl_clflush(ptr, CL_SIZE) to flush more than one cache lines, we use mmap to
 * allocate memory, which should be page-aligned.
 *
static void *
mmap_anon(size_t size)
{
	void *ptr = mmap(NULL, size, PROT_READ | PROT_WRITE,
			 MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	if (ptr == MAP_FAILED) {
		perror("mmap anonymous");
		return NULL;
	}
	return ptr;
}
*/

/*
 * mmap_file -- mmap a file
 *
 * When /dev/zero is memory-mapped it is equivalent to using anonymous memory.
 *
static void *
mmap_file(char *filename, size_t size)
{
	int fd = open(filename, O_RDWR | O_SYNC);
	if (fd < 0) {
		perror("file open");
		close(fd);
		return NULL;
	}

	void *ptr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (ptr == MAP_FAILED) {
		perror("mmap file");
		goto out;
	}
out:
	close(fd);
	return ptr;
}
*/

struct op {
	const char *name;
	void (*func)(struct cache_bench *cb, size_t len, unsigned tid);
};

static struct op ops[] = {
	{"memcpy", memcpy_func},
	{"clflush", clflush_func},
	{"memcpy_xdst", memcpy_xdst},
	{"memcpy_clflush_src", memcpy_clflush_src},
	{"memcpy_clflush_dst", memcpy_clflush_dst},
	{"memcpy_clflushopt_dst", memcpy_clflushopt_dst},
	{"memcpy_clwb_dst", memcpy_clwb_dst},
	{"memcpy_clflush_xsrc", memcpy_clflush_xsrc},
	{"memcpy_clflush_xdst", memcpy_clflush_xdst},
	{"memcpy_clflush_nowhere", memcpy_clflush_nowhere},
	{"memcpy_xdst_clflush_dst", memcpy_xdst_clflush_dst},
	{"movnt_sse2", movnt_sse2_func},
	{"movnt_avx", movnt_avx_func},
	{"memcpy_movnt_sse2", memcpy_movnt_sse2},
	{"memcpy_xdst_movnt_sse2", memcpy_xdst_movnt_sse2},
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

/*
 * randbuf -- randomize the content of the buffer
 *
static size_t
randbuf(void *buf, size_t len)
{
	FILE *stream;
	stream = fopen("/dev/urandom", "r");
	if (!stream) {
		perror("fopen /dev/urandom");
		return 0;
	}

	size_t rdlen = fread(buf, 1, len, stream);
	if (rdlen != len) {
		fprintf(stderr, "fread failed, len = %zu, rdlen = %zu\n", len,
			rdlen);
		return rdlen;
	}

//	unsigned char *ptr = (unsigned char *)buf;
//	for (int i = 0; i < 32; i++)
//		printf("%02x%s%s", ptr[i], (i + 1) % 8 ? " " : "  ",
//				   (i + 1) % 16 ? "" : "\n");

	fclose(stream);

	return rdlen;
}
*/

/*
 * cache_bench_init -- initialize benchmark environment
 */
static int
cache_bench_init(struct benchmark *bench, struct benchmark_args *args)
{
	assert(bench != NULL);
	assert(args != NULL);
	assert(args->opts != NULL);

	/* check libnuma */
	if (numa_available() < 0) {
		fprintf(stderr, "NUMA control not available\n");
		return -1;
	}

	struct cache_bench_args *cba = (struct cache_bench_args *)args->opts;

	unsigned unalign = cba->unalign;
	if (unalign > 0) {
		fprintf(stderr, "unalign is not implemented yet\n");
		return -1;
	}

	struct cache_bench *cb =
		(struct cache_bench *)malloc(sizeof(struct cache_bench));
	if (!cb) {
		perror("malloc cache_bench");
		return -1;
	}

	unsigned nthreads = args->n_threads;
	cb->nthreads = nthreads;

	/* round up to page sizes for alignment */
	cb->poolsize = PAGE_ALIGNED_UP_SIZE(unalign + args->dsize);

	/* operation type */
	int fid = parse_op_type(cba->operation);
	if (fid == -1) {
		fprintf(stderr, "wrong operation: %s\n", cba->operation);
		goto free_cb;
	}
	cb->cache_op = ops[fid].func;

	/* worker's source buffers, numa-local */
	cb->src = (uintptr_t **)calloc(nthreads, sizeof(uintptr_t *));
	if (!cb->src) {
		perror("calloc for cb->src");
		goto free_cb;
	}

	/* worker's destination buffers, numa-local */
	cb->dst = (uintptr_t **)calloc(nthreads, sizeof(uintptr_t *));
	if (!cb->dst) {
		perror("calloc for cb->dst");
		goto free_src;
	}

	/* a memory range that should not be present in any processor cache */
	cb->nowhere = numa_alloc_local(cb->poolsize);
	if (cb->nowhere == NULL) {
		perror("numa_alloc_local for cb->nowhere");
		return -1;
	}
	if (!CL_ALIGNED(cb->nowhere))
		fprintf(stderr, "cb->nowhere %p is not cache-line aligned\n",
			cb->nowhere);
	memset(cb->nowhere, 'N', cb->poolsize);
	nvsl_clflush(cb->nowhere, cb->poolsize);

	/* set pointer to bench-specific data */
	pmembench_set_priv(bench, cb);

	return 0;

free_src:
	free(cb->src);
free_cb:
	free(cb);
	return -1;
}

/*
 * cache_bench_operation -- actual benchmark operation
 */
static int
cache_bench_operation(struct benchmark *bench, struct operation_info *info)
{
	struct cache_bench *cb =
		(struct cache_bench *)pmembench_get_priv(bench);
	size_t datasize = info->args->dsize;

	/* worker id or thread id */
	unsigned tid = (unsigned)info->worker->index;

	/*
	 * We put numa-aware allocation here for thread affinity. The allocation
	 * is timed but its impact is amortized when the thread operation is
	 * repeated large count of times.
	 */
	if (cb->src[tid] == NULL && cb->dst[tid] == NULL) {
		cb->src[tid] = (uintptr_t *)numa_alloc_local(cb->poolsize);
		if (cb->src[tid] == NULL) {
			perror("numa_alloc_local for cb->src[tid]");
			return -1;
		}
		if (!CL_ALIGNED(cb->src[tid]))
			fprintf(stderr, "cb->src[%u] %p is not cache-line "
					"aligned\n",
				tid, cb->src[tid]);

		cb->dst[tid] = (uintptr_t *)numa_alloc_local(cb->poolsize);
		if (cb->dst[tid] == NULL) {
			perror("numa_alloc_local for cb->dst[tid]");
			goto free_src;
		}
		if (!CL_ALIGNED(cb->dst[tid]))
			fprintf(stderr, "cb->dst[%u] %p is not cache-line "
					"aligned\n",
				tid, cb->dst[tid]);

		/*
		 * XXX: If we do not ever write to the src buffer, why is the
		 * memcpy_clflush_src() latency becomes be much larger,
		 * especially when cb->poolsize > 4K?
		 */
		memset(cb->src[tid], 'X', cb->poolsize);

		/*
		 * Initially flush the buffers out of the processor caches, so
		 * that the src buffer will be in a clean state when read in
		 * cb->cache_op().
		 */
		nvsl_clflush(cb->src[tid], cb->poolsize);
		nvsl_clflush(cb->dst[tid], cb->poolsize);
	}

	cb->cache_op(cb, datasize, tid);

	return 0;

free_src:
	numa_free(cb->src[tid], cb->poolsize);
	return -1;
}

/*
 * cache_bench_exit -- benchmark cleanup function
 */
static int
cache_bench_exit(struct benchmark *bench, struct benchmark_args *args)
{
	struct cache_bench *cb =
		(struct cache_bench *)pmembench_get_priv(bench);

	for (unsigned i = 0; i < cb->nthreads; i++) {
		assert(cb->src[i] != NULL);
		assert(cb->dst[i] != NULL);
		numa_free(cb->src[i], cb->poolsize);
		numa_free(cb->dst[i], cb->poolsize);
	}

	free(cb->src);
	free(cb->dst);
	free(cb);

	return 0;
}

/* structure to define command line arguments */
static struct benchmark_clo cache_bench_clos[2];
/* information about benchmark. */
static struct benchmark_info cache_bench_info;
CONSTRUCTOR(cache_bench_constructor)
void
cache_bench_constructor(void)
{
	cache_bench_clos[0].opt_short = 'o';
	cache_bench_clos[0].opt_long = "operation";
	cache_bench_clos[0].descr = "Operation name - memcpy, clflush, "
				    "memcpy_clflush_dst, movnt_sse2, ...";
	cache_bench_clos[0].type = CLO_TYPE_STR;
	cache_bench_clos[0].off =
		clo_field_offset(struct cache_bench_args, operation);
	cache_bench_clos[0].def = "memcpy";

	cache_bench_clos[1].opt_short = 'u';
	cache_bench_clos[1].opt_long = "unalignment";
	cache_bench_clos[1].descr = "Unalignment in bytes";
	cache_bench_clos[1].off =
		clo_field_offset(struct cache_bench_args, unalign);
	cache_bench_clos[1].type = CLO_TYPE_UINT;
	cache_bench_clos[1].def = "0";
	cache_bench_clos[1].type_uint.size =
		clo_field_size(struct cache_bench_args, unalign);
	cache_bench_clos[1].type_uint.base = CLO_INT_BASE_DEC;
	cache_bench_clos[1].type_uint.min = 0;
	cache_bench_clos[1].type_uint.max = 63;

	cache_bench_info.name = "cache_bench";
	cache_bench_info.brief = "Benchmark for cache-related opeartions";
	cache_bench_info.init = cache_bench_init;
	cache_bench_info.exit = cache_bench_exit;
	cache_bench_info.multithread = true;
	cache_bench_info.multiops = true;
	cache_bench_info.operation = cache_bench_operation;
	cache_bench_info.measure_time = true;
	cache_bench_info.clos = cache_bench_clos;
	cache_bench_info.nclos = ARRAY_SIZE(cache_bench_clos);
	cache_bench_info.opts_size = sizeof(struct cache_bench_args);
	cache_bench_info.rm_file = true;
	cache_bench_info.allow_poolset = true;
	REGISTER_BENCHMARK(cache_bench_info);
};
