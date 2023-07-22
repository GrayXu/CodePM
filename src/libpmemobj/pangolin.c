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
 * pangolin.c -- pangolin library implementations
 */

#include <sys/mman.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include "pangolin.h"
#include "libpangolin.h"
#include "objbuf_tx.h"

#include "x86intrin.h"
#include <immintrin.h>

#include <pthread.h>

#include "pangolin_ec.h"

// for easy code reading
#ifndef PANGOLIN_PARITY
#define PANGOLIN_PARITY
#endif

#define CLFLUSHOPT_ASM(addr)\
	asm volatile("clflushopt %0" :\
	"+m" (*(volatile char *)(addr)));

/*
 * pangolin_repair_page -- repair a page's data
 */
int
pangolin_repair_page(void *page)
{
#ifdef PANGOLIN_PARITY
	ASSERT(ALIGNED_4K(page));
	return pangolin_repair_corruption(page, Pagesize, 1, NULL);
#else
	return 0;
#endif
}

/* -------------------------- Pangolin Replication -------------------------- */

/*
 * pangolin_check_metarep -- check if metadata replicas equal
 *
 * This only works for lane and heap metadata.
 */
static int
pangolin_check_metarep(void *pop, const void *src, size_t len)
{
#ifdef PANGOLIN_METAREP
	void *rep = pangolin_repaddr(pop, src);

	return memcmp(src, rep, len);
#else
	return 0;
#endif
}

/*
 * pangolin_replicate_heap_header -- replicate struct heap_header
 */
void *
pangolin_replicate_heap_header(void *popaddr, const void *src, size_t len)
{
	PMEMobjpool *pop = (PMEMobjpool *)popaddr;
	void *rep = (char *)src - MREP_SIZE;

	/* sfence (drain) will be executed when persisting the main copy */
	void *ret = pmemops_memcpy(&pop->p_ops, rep, src, len,
		PMEMOBJ_F_MEM_WC | PMEMOBJ_F_MEM_NODRAIN | PMEMOBJ_F_RELAXED);

	return ret;
}

/*
 * pangolin_replicate_zone -- replicate struct zone
 */
void *
pangolin_replicate_zone(void *popaddr, const void *src, size_t len)
{
	PMEMobjpool *pop = (PMEMobjpool *)popaddr;
	void *rep = (char *)src + ZONE_META_SIZE;

	/* sfence (drain) will be executed when persisting the main copy */
	void *ret = pmemops_memcpy(&pop->p_ops, rep, src, len,
		PMEMOBJ_F_MEM_WC | PMEMOBJ_F_MEM_NODRAIN | PMEMOBJ_F_RELAXED);

	return ret;
}

/* ---------------------------- Pangolin Parity ---------------------------- */

/*
 * pangolin_adjust_zone_size -- adjust zone size for replication and parity
 */
uint32_t
pangolin_adjust_zone_size(uint32_t zone_id)
{
	uint32_t zone_rows;
	uint64_t zone_row_size;

	struct obuf_runtime *ort = obuf_get_runtime();

	if (zone_id != ort->last_zone_id) {
		zone_rows = ort->zone_rows;
		zone_row_size = ort->zone_row_size;
	} else {
		zone_rows = ort->last_zone_rows;
		zone_row_size = ort->last_zone_row_size;
	}

	uint32_t zone_row_chunks = (uint32_t)(zone_row_size / CHUNKSIZE);

	uint32_t size_idx = zone_row_chunks * (zone_rows - 1);

	return size_idx;
}

#ifdef PANGOLIN_PARITY
extern int
xor_gen_sse_pgl(int vects, size_t len, void **array);

extern int
xor_gen_sse_pipeline_pgl(int vects, size_t len, void **array);

extern int
xor_gen_avx512_pgl(int vects, size_t len, void **array);

extern int
xor_gen_avx_pgl(int vects, size_t len, void **array);

extern int
xor_gen_avx512_pipeline_pgl(int vects, size_t len, void **array);

// you can disable locks for parity updates
static inline int
os_rwlock_rdlock_wrap(os_rwlock_t *__restrict rwlock) {
#ifndef PANGOLIN_PARITY_NOLOCK
	return os_rwlock_rdlock(rwlock);
#endif
	return 0;
}

static inline int
os_rwlock_wrlock_wrap(os_rwlock_t *__restrict rwlock) {
#ifndef PANGOLIN_PARITY_NOLOCK
	return os_rwlock_wrlock(rwlock);
#endif
	return 0;
}

static inline int
os_rwlock_unlock_wrap(os_rwlock_t *__restrict rwlock) {
#ifndef PANGOLIN_PARITY_NOLOCK
	return os_rwlock_unlock(rwlock);
#endif
	return 0;
}

/*
 * pangolin_update_column_parity -- update parity of a range column
 */
static int
pangolin_update_column_parity(PMEMobjpool *pop, uint64_t *data, uint64_t *newd,
	uint64_t *parity, size_t size, uint32_t zid, uint32_t rclock)
{
	ASSERT(ALIGNED_8(size));
	ASSERT(ALIGNED_8(parity));
	ASSERT(size <= RCLOCKSIZE);

	struct obuf_runtime *ort = obuf_get_runtime();

#ifdef PANGOLIN_RCLOCKHLE
	/*
	 * Using HLE the performance may look worse due to hardware-level
	 * synchronization and aborts caused by lock contention.
	 *
	 * Do NOT use cache flushes and non-temporal stores within HLE or RTM
	 * section as the instructions will cause HTM to abort.
	 */
	while (__atomic_exchange_n(&ort->zone_rclock[zid][rclock], 1,
			__ATOMIC_ACQUIRE | __ATOMIC_HLE_ACQUIRE));
#else

	if (size >= ort->rclock_threshold)  
		os_rwlock_wrlock_wrap(&ort->zone_rclock[zid][rclock]);  // SIMD: read + write lock
	else
		os_rwlock_rdlock_wrap(&ort->zone_rclock[zid][rclock]);  // Otherwise: read lock

	if (size >= ort->rclock_threshold) {  // SIMD coding threshold
#endif
		/* 
		 * SIMD
		 * xor_gen_sse_pgl() requires 16-byte alignment
		 * xor_gen_avx() and xor_gen_avx512_pgl() require 32-byte alignment
		 */
		int (*xor_parity)(int vects, size_t len, void **array);

// xor_parity function
#ifdef PANGOLIN_AVX512CODE
	#ifdef PANGOLIN_PIPELINE
		xor_parity = &xor_gen_avx512_pipeline_pgl;
	#else
		xor_parity = &xor_gen_avx512_pgl;
	#endif
#else
	#ifdef PANGOLIN_PIPELINE
		xor_parity = &xor_gen_sse_pipeline_pgl;
	#else
		xor_parity = &xor_gen_sse_pgl;
	#endif
#endif
		if (newd == NULL) {  // no newd means xor 0, no need to compute. so it can also be considered as RAID-like append
			void *array[3] = {data, parity, parity};  // parity:=data+parity
			xor_parity(3, size, array); 
		} else {  // this is the update flow of new data, no mem footprint except data,newd,parity
			void *array[4] = {data, newd, parity, parity};  // parity:=data+newd+parity
			xor_parity(4, size, array);
		}
#ifndef PANGOLIN_RCLOCKHLE
	} else {
		/*
		 * atomic xor 
		*/
		size_t words = size >> 3;
		size_t flush_batches = words >> 3;
		size_t i = 0;
		if (newd == NULL) {  // append update
			for (i = 0; i < words; i++) {
				__atomic_xor_fetch(&parity[i], data[i], __ATOMIC_RELAXED);
	#ifdef PANGOLIN_PIPELINE
				if (((i+1) & 7) == 0) pmemops_xflush(&pop->p_ops, &parity[i-7], 64, PMEMOBJ_F_RELAXED);  // flush cache line
	#endif
			}
		} else {  // normal update
			for (i = 0; i < words; i++) {
				__atomic_xor_fetch(&parity[i], data[i] ^ newd[i], __ATOMIC_RELAXED);
	#ifdef PANGOLIN_PIPELINE
				if (((i+1) & 7) == 0) pmemops_xflush(&pop->p_ops, &parity[i-7], 64, PMEMOBJ_F_RELAXED);  // flush cache line
	#endif
			}
		}
	#ifdef PANGOLIN_PIPELINE
		// not 64B aligned -> an extra flush
		if (unlikely((i & 7) != 0)) pmemops_xflush(&pop->p_ops, &parity[i-i%8], 64, PMEMOBJ_F_RELAXED);
	#endif
	}
#endif

#ifdef PANGOLIN_RCLOCKHLE
	__atomic_clear(&ort->zone_rclock[zid][rclock],
		__ATOMIC_RELEASE | __ATOMIC_HLE_RELEASE);
#else
	os_rwlock_unlock_wrap(&ort->zone_rclock[zid][rclock]);
#endif

	return 0;
}

/**
 * multi-parity version, use isa-l-pm
*/ 
static int
pangolin_update_column_parity_multi(PMEMobjpool *pop, uint64_t *data, uint64_t *newd,
	uint64_t *parity, size_t size, uint32_t zid, uint32_t rclock, uint32_t vec_i)
{
	ASSERT(ALIGNED_8(size));
	ASSERT(ALIGNED_8(parity));
	struct obuf_runtime *ort = obuf_get_runtime();
	struct ec_runtime *ec = ec_get_runtime();

	uint64_t *parities[2];
	if (ort->last_zone_id == zid) {
		parities[0] = parity;
		if (ec->p != 1) parities[1] = (char *)parity + ort->last_zone_row_size;
		else parities[1] = NULL;
	} else {
		parities[0] = parity;
		if (ec->p != 1) parities[1] = (char *)parity + ort->zone_row_size;
		else parities[1] = NULL;
	}

#ifdef PANGOLIN_RCLOCKHLE
	while (__atomic_exchange_n(&ort->zone_rclock[zid][rclock], 1, __ATOMIC_ACQUIRE | __ATOMIC_HLE_ACQUIRE));
#else
	if (size >= ort->rclock_threshold) os_rwlock_wrlock_wrap(&ort->zone_rclock[zid][rclock]);
	else os_rwlock_rdlock_wrap(&ort->zone_rclock[zid][rclock]);

	if (size >= ort->rclock_threshold) {  // SIMD coding
#endif
		if (newd == NULL) { // append style (data == delta_data)
#ifdef PANGOLIN_PIPELINE
			encode_update_pipeline(ec, data, parities, size, vec_i, PANGOLIN_EC_TEMPORAL);
#else
			encode_update(ec, data, parities, size, vec_i, PANGOLIN_EC_TEMPORAL);
#endif
		} else {  // normal update with new data (w/ mid buffer)
#ifdef PANGOLIN_PIPELINE
			// update_parity_inplace_pipeline(ec, data, (char **)parities, size, vec_i, newd, PANGOLIN_EC_TEMPORAL);
			update_parity_pipeline(ec, data, (char **)parities, size, vec_i, newd, PANGOLIN_EC_TEMPORAL);
#else
			// update_parity_inplace(ec, data, (char **)parities, size, vec_i, newd, PANGOLIN_EC_TEMPORAL);
			update_parity(ec, data, (char **)parities, size, vec_i, newd, PANGOLIN_EC_TEMPORAL);
#endif
		}
#ifndef PANGOLIN_RCLOCKHLE
	} else { // atomic coding (multiple parity update with atomic uses an extra delta parity buf, compute it first, then atomic xor back. baseline performance is generally low, cache efficiency will be low)
		size_t words = size >> 3;  // 8B -> 1 write
		size_t flush_batches = words >> 3;  // 64B as a flush batch
		size_t i = 0;
		if (ec->p == 1) {  // normal and simple xor parity
			if (newd == NULL) {  // append update
				for (i = 0; i < words; i++) {
					__atomic_xor_fetch(&parity[i], data[i], __ATOMIC_RELAXED);
#ifdef PANGOLIN_PIPELINE
					if (((i+1) & 7) == 0) pmemops_xflush(&pop->p_ops, &parity[i-7], 64, PMEMOBJ_F_RELAXED);  // flush cache line
#endif
				}
			} else {  // normal update
/////////////////////////////////////////////

				for (i = 0; i < words; i++) {
					__atomic_xor_fetch(&parity[i], data[i] ^ newd[i], __ATOMIC_RELAXED);
#ifdef PANGOLIN_PIPELINE
					if (((i+1) & 7) == 0) pmemops_xflush(&pop->p_ops, &parity[i-7], 64, PMEMOBJ_F_RELAXED);  // flush cache line
#endif
				}

// 				for (i = 0 ;i < flush_batches; i++) {  // unfold the flush branch, no effect
// 					__atomic_xor_fetch(&parity[i*8  ], data[i*8  ] ^ newd[i*8  ], __ATOMIC_RELAXED);
// 					__atomic_xor_fetch(&parity[i*8+1], data[i*8+1] ^ newd[i*8+1], __ATOMIC_RELAXED);
// 					__atomic_xor_fetch(&parity[i*8+2], data[i*8+2] ^ newd[i*8+2], __ATOMIC_RELAXED);
// 					__atomic_xor_fetch(&parity[i*8+3], data[i*8+3] ^ newd[i*8+3], __ATOMIC_RELAXED);
// 					__atomic_xor_fetch(&parity[i*8+4], data[i*8+4] ^ newd[i*8+4], __ATOMIC_RELAXED);
// 					__atomic_xor_fetch(&parity[i*8+5], data[i*8+5] ^ newd[i*8+5], __ATOMIC_RELAXED);
// 					__atomic_xor_fetch(&parity[i*8+6], data[i*8+6] ^ newd[i*8+6], __ATOMIC_RELAXED);
// 					__atomic_xor_fetch(&parity[i*8+7], data[i*8+7] ^ newd[i*8+7], __ATOMIC_RELAXED);
// #ifdef PANGOLIN_PIPELINE
// 					pmemops_xflush(&pop->p_ops, &(parity[i*8]), 64, PMEMOBJ_F_RELAXED);
// 					// CLFLUSHOPT_ASM(&(parity[i*8]));
// #endif
// 				}

/////////////////////////////////////////////
#ifdef PANGOLIN_PIPELINE  // not 64B aligned -> an extra flush
			if (unlikely((i & 7) != 0)) 
				pmemops_xflush(&pop->p_ops, &parity[i-i%8], 64, PMEMOBJ_F_RELAXED);
#endif
			}
		} else {  // multiple parity, use delta parity to perform atomic ops
			int buf_len = 4096+256;  // incase buffer overflow?
			char buf[2 * buf_len];
			memset(buf, 0, 2 * buf_len);
			uint64_t *delta_parity[2] = {buf, buf + buf_len};
			if (newd == NULL) {  // append update
				gen_delta_parity(ec, data, size, vec_i, NULL, delta_parity);  // gen delta parity with SIMD
				for (int ii = 0; ii < ec->p; ii++) {
					for (i = 0; i < words; i++) {
						__atomic_xor_fetch(&(parities[ii][i]), delta_parity[ii][i], __ATOMIC_RELAXED);
#ifdef PANGOLIN_PIPELINE
						if (((i+1) & 7) == 0) pmemops_xflush(&pop->p_ops, &parities[ii][i-7], 64, PMEMOBJ_F_RELAXED);
#endif
					}
				}
			} else {  // normal update
				gen_delta_parity(ec, data, size, vec_i, newd, delta_parity);
				for (int ii = 0; ii < ec->p; ii++) {
					for (i = 0; i < words; i++) {
						__atomic_xor_fetch(&(parities[ii][i]), delta_parity[ii][i], __ATOMIC_RELAXED);
#ifdef PANGOLIN_PIPELINE
						if (((i+1) & 7) == 0) pmemops_xflush(&pop->p_ops, &parities[ii][i-7], 64, PMEMOBJ_F_RELAXED);
#endif
					}
				}
			}
#ifdef PANGOLIN_PIPELINE   // not 64B aligned -> an extra flush
			if (unlikely((i & 7) != 0)) {  // not 64B aligned -> an extra flush for atomic
				for (int ii = 0; ii < ec->p; ii++) 
					pmemops_xflush(&pop->p_ops, &parities[ii][i-i%8], 64, PMEMOBJ_F_RELAXED);
			}
#endif
			// atomic_update_parity_inplace(ec, parities, delta_parity, size);  // should be used to simplify codes
		}
	}
#endif

#ifdef PANGOLIN_RCLOCKHLE
	__atomic_clear(&ort->zone_rclock[zid][rclock],
		__ATOMIC_RELEASE | __ATOMIC_HLE_RELEASE);
#else
	os_rwlock_unlock_wrap(&ort->zone_rclock[zid][rclock]);
#endif

	return 0;
}

#endif

/**
 * pipeline data version of `pangolin_update_column_parity_multi` (update data and gen delta firstly)
 * @param delta: the obj_delta in obuf, may be delta_data or delta_parity respect to PANGOLIN_PIPELINE_DATA or PANGOLIN_PIPELINE_DATA_2M
*/
static int
pangolin_update_column_parity_multi_pd(PMEMobjpool *pop, uint64_t *data, uint64_t **delta,
	uint64_t *parity, size_t size, uint32_t zid, uint32_t rclock, uint32_t vec_i)
{
	ASSERT(ALIGNED_8(size));  // TODO: meta may have 8B small update, so coding must be full functional
	ASSERT(ALIGNED_8(parity));

	struct obuf_runtime *ort = obuf_get_runtime();
	struct ec_runtime *ec = ec_get_runtime();
	
	uint64_t *delta_parities[2];
	if (ec->p == 1) delta_parities[0] = delta[1];
	else for (int ii = 0; ii < ec->p; ii++) delta_parities[ii] = delta[ii];
	
	uint64_t *delta_data = delta[1];

	uint64_t *parities[2];  // parity on PM
	if (ort->last_zone_id == zid) {
		parities[0] = parity;
		if (ec->p != 1) parities[1] = (char *)parity + ort->last_zone_row_size;
		else parities[1] = NULL;
	} else {
		parities[0] = parity;
		if (ec->p != 1) parities[1] = (char *)parity + ort->zone_row_size;
		else parities[1] = NULL;
	}

#ifdef PANGOLIN_RCLOCKHLE // HLE instead of wrlock
	while (__atomic_exchange_n(&ort->zone_rclock[zid][rclock], 1, __ATOMIC_ACQUIRE | __ATOMIC_HLE_ACQUIRE));
#else
	if (size >= ort->rclock_threshold) os_rwlock_wrlock_wrap(&ort->zone_rclock[zid][rclock]);
	else os_rwlock_rdlock_wrap(&ort->zone_rclock[zid][rclock]);

	if (size >= ort->rclock_threshold) {
#endif
		/* SIMD coding */
#ifndef PANGOLIN_PIPELINE_DATA_2M
		// delta is delta_data
		encode_update_pipeline(ec, delta_data, parities, size, vec_i, PANGOLIN_EC_TEMPORAL);
#else  // delta is delta_parity, no coding, just merge
		for (int ii = 0; ii < ec->p; ii++) {
			void *array[3] = {delta_parities[ii], parities[ii], parities[ii]};
#ifdef PANGOLIN_PIPELINE
			xor_gen_avx512_pipeline_pgl(3, size, array);
#else
			xor_gen_avx512_pgl(3, size, array);
#endif
		}
#endif

#ifndef PANGOLIN_RCLOCKHLE
	} else {
		/* atomic coding */
		size_t words = size >> 3;
		size_t i = 0;
		if (ec->p == 1) {  // delta_data == delta_parity -> xor + flush
			for (i = 0; i < words; i++) {
				__atomic_xor_fetch(&parity[i], delta_data[i], __ATOMIC_RELAXED);
#ifdef PANGOLIN_PIPELINE
				if (unlikely(((i+1) & 7) == 0)) pmemops_xflush(&pop->p_ops, &parity[i-7], 64, PMEMOBJ_F_RELAXED);  // flush cache line
#endif
			}
#ifdef PANGOLIN_PIPELINE
			if (unlikely((i & 7) != 0)) pmemops_xflush(&pop->p_ops, &parity[i-i%8], 64, PMEMOBJ_F_RELAXED);
#endif
		} else {  // multiple parity, delta data -> delta parity, to perform atomic ops
			// gen delta_parity
	#ifndef PANGOLIN_PIPELINE_DATA_2M
			encode_update(ec, delta_data, delta_parities, size, vec_i, PANGOLIN_EC_TEMPORAL);  // since delta_data is the last delta_parities, inplace works
	#endif
			// merge delta_parity to parities
			for (int ii = 0; ii < ec->p; ii++) {
				for (i = 0; i < words; i++) {
					__atomic_xor_fetch(&(parities[ii][i]), delta_parities[ii][i], __ATOMIC_RELAXED);
#ifdef PANGOLIN_PIPELINE
					if (((i+1) & 7) == 0) pmemops_xflush(&pop->p_ops, &parities[ii][i-7], 64, PMEMOBJ_F_RELAXED);
#endif
				}
			}
#ifdef PANGOLIN_PIPELINE
			if (unlikely((i & 7) != 0)) {  // not 64B aligned -> an extra flush for atomic
				for (int ii = 0; ii < ec->p; ii++) pmemops_xflush(&pop->p_ops, &parities[ii][i-i%8], 64, PMEMOBJ_F_RELAXED);
			}
#endif
		}
	}
#endif

#ifdef PANGOLIN_RCLOCKHLE
	__atomic_clear(&ort->zone_rclock[zid][rclock],
		__ATOMIC_RELEASE | __ATOMIC_HLE_RELEASE);
#else
	os_rwlock_unlock_wrap(&ort->zone_rclock[zid][rclock]);
#endif

	return 0;
}

int
pangolin_atomic_xor(PMEMobjpool *pop, void *src1, void *src2, void *dst,
	size_t size)
{
#ifdef PANGOLIN_PARITY
	ASSERT(ALIGNED_8(src1));
	ASSERT(ALIGNED_8(src2));
	ASSERT(ALIGNED_8(dst));
	ASSERT(ALIGNED_8(size));

	uint64_t *src1w = (uint64_t *)src1;
	uint64_t *src2w = (uint64_t *)src2;
	uint64_t *dstw = (uint64_t *)dst;
	size_t words = size >> 3;

	for (size_t i = 0; i < words; i++)
		__atomic_xor_fetch(&dstw[i], src1w[i] ^ src2w[i],
			__ATOMIC_RELAXED);

	pmemops_persist(&pop->p_ops, dst, size);
#endif
	return 0;
}

int
pangolin_avx_xor(PMEMobjpool *pop, void *src1, void *src2, void *dst,
	size_t size)
{
#ifdef PANGOLIN_PARITY
	ASSERT(ALIGNED_32(src1));
	ASSERT(ALIGNED_32(src2));
	ASSERT(ALIGNED_32(dst));
	ASSERT(ALIGNED_32(size));

	void *array[4] = {src1, src2, dst, dst};
	xor_gen_avx_pgl(4, size, array);

	pmemops_persist(&pop->p_ops, dst, size);
#endif
	return 0;
}

/*
 * get parity addr based on data addr
*/
// void *
// pangolin_parity_addr(PMEMobjpool *pop, void *data, void *newd, size_t size)
// {
// #ifdef PANGOLIN_PARITY

// #endif
// 	return NULL
// }

/*
 * only called by update with redolog
*/
int
pangolin_update_data(PMEMobjpool *pop, void *src, void *dst, size_t size)
{	
	pmemops_memcpy(&pop->p_ops, dst, src, size,
		PMEMOBJ_F_MEM_WC | PMEMOBJ_F_MEM_NODRAIN);
	return 0;
}

/*
 * pangolin_update_parity -- update a range of parity as a result of data change
 *
 * Content of [data : data + size) will change to [newd : newd + size), as a
 * result of a store or memcpy-kind of operation.
 *
 * newd may be delta in some variants
 * Address of data determines the affected parity region. If newd is not NULL,
 * parity = parity XOR (data XOR newd); otherwise parity = parity XOR data.
 */
int
pangolin_update_parity(PMEMobjpool *pop, void *data, void *newd, size_t size) 
{
	return pangolin_update_parity_x(pop, data, newd, size, 0);
}
/*
 * data=dst, newd=delta
 * controlled by PANGOLIN_PIPELINE_DATA, PANGOLIN_PIPELINE, PANGOLIN_RCLOCKHLE
 * the delta is 1dim ptr
*/
int
pangolin_update_parity_x(PMEMobjpool *pop, void *data, void *newd_or_delta, size_t size, uint8_t newd_is_delta)
{
	uint64_t *delta[2] = {NULL, NULL};
	void *newd = NULL;
	if (newd_is_delta) {  // delta
		delta[0] = (uint64_t *) newd_or_delta;
		delta[1] = delta[0] + size/8;
		newd = (void *) delta[0];
	} else {
		newd = newd_or_delta;
	}

#ifdef PANGOLIN_PARITY
	/* size should always be 8-byte (or more) aligned */
	ASSERT(ALIGNED_8(data));
	ASSERT(ALIGNED_8(newd));
	ASSERT(ALIGNED_8(size));

	ASSERT(OBJ_PTR_FROM_POOL(pop, data));

	uint64_t offset;
	uint16_t vec_i;
	void *parity_ptr;
	uint32_t zid = 0;

	struct obuf_runtime *ort = obuf_get_runtime();
	// calculate parity position by the address info of ort
	void *heap_start = (char *)pop + pop->heap_offset;
	/* offset to the start of zone0 */
	offset = (uint64_t)data - (uint64_t)heap_start -
		sizeof(struct heap_header);  // the global offset of data relative to heap

// #ifndef PANGOLIN_PIPELINE_DATA_2M
	/*
	 * compute offset / ZONE_MAX_SIZE
	 * ZONE_MAX_SIZE is 16 GB so the while loop will not take many rounds.
	 */
	while (offset >= ZONE_MAX_SIZE) {
		zid += 1;
		offset -= ZONE_MAX_SIZE;
	}
	struct zone *z = ZID_TO_ZONE(heap_start, zid);  // the address of libpmemobj's zone get from zid
	uint64_t row_size;
	uint64_t parity_start;  // parity offset
	if (zid != ort->last_zone_id) {
		row_size = ort->zone_row_size;
		parity_start = ort->zone_parity;
	} else {
		row_size = ort->last_zone_row_size;
		parity_start = ort->last_zone_parity;
	}
	/* offset into the zone's chunks */
	offset -= sizeof(struct zone);
	/* offset into the resident data chunk group */
	vec_i = 0;  // needed when updating multi-parity
	while (offset >= row_size) {
		offset -= row_size;
		vec_i += 1;
	}

// #else
// // read the previous computed results in obuf (skip the loop in computation)
// 	offset = obuf->p_offset;
// 	vec_i = obuf->p_vec_i;
// 	zid = obuf->p_zid;
// 	struct zone *z = ZID_TO_ZONE(heap_start, zid);
// 	uint64_t row_size;
// 	uint64_t parity_start;  // parity offset
// 	if (zid != ort->last_zone_id) {
// 		row_size = ort->zone_row_size;
// 		parity_start = ort->zone_parity;
// 	} else {
// 		row_size = ort->last_zone_row_size;
// 		parity_start = ort->last_zone_parity;
// 	}
// #endif

	parity_ptr = (char *)z + parity_start + offset;

	uint32_t rclock = (uint32_t)(offset >> RCLOCKSHIFT);

	/* off: offset within a page; len: bytes to update within a page */
	
	size_t parity_size = size;

	uint64_t *parity, off, len;
	while (size > 0) {
		// ASSERT(offset < row_size);
		ASSERT(ALIGNED_8(offset));

		/*
		 * PGL-OPT: Updating parity per page causes some overhead if
		 * offset + size is not page-aligned. Try increasing RCLOCKSIZE
		 * see the perferformance changes.
		 */
		off = offset & ((uint64_t)RCLOCKSIZE - 1);
		len = (off + size < RCLOCKSIZE) ? size : RCLOCKSIZE - off;

		parity = (uint64_t *)((char *)z + parity_start + offset);  // iterator

		ASSERT(OBJ_PTR_FROM_POOL(pop, parity));

		// update parity! calculate parity position, rclock is lock id
		// pangolin_update_column_parity(pop, data, newd, parity, len, zid, rclock);  // xor version
		
#ifndef PANGOLIN_PIPELINE_DATA
		pangolin_update_column_parity_multi(pop, data, newd, parity, len, zid, rclock, vec_i);  // multi-parity version
#else
		if (newd_is_delta) 
			pangolin_update_column_parity_multi_pd(pop, data, delta, parity, len, zid, rclock, vec_i);  // pipeline data version (newd is delta)
		else
			pangolin_update_column_parity_multi(pop, data, newd, parity, len, zid, rclock, vec_i);
#endif

		rclock += 1;
		size -= len;
		offset += len;
		if (offset == row_size) {
			rclock = 0;
			offset = 0;
		}

		data = (char *)data + len;
		if (newd != NULL)
			newd = (char *)newd + len;
	}

#ifndef PANGOLIN_PIPELINE
	// call clwb for dirty parity, and because of outside sfence, sfence (drain) will execute after obuf_tx_heap_commit() */
	pmemops_xflush(&pop->p_ops, parity_ptr, parity_size, PMEMOBJ_F_RELAXED);
	// for (int i = 0; i < parity_size; i+=64) {
	// 	CLFLUSHOPT_ASM((char*)parity_ptr+i);
	// }

	// flush other parity rows
	struct ec_runtime *ec = ec_get_runtime();
	if (ec->p != 1) {
		for (int i = 1; i < ec->p; i++) {
			if (zid != ort->last_zone_id)
				pmemops_xflush(&pop->p_ops, 
							(char *)parity_ptr + i*ort->zone_row_size,
							parity_size, PMEMOBJ_F_RELAXED);
			else
				pmemops_xflush(&pop->p_ops, 
							(char *)parity_ptr + i*ort->last_zone_row_size,
							parity_size, PMEMOBJ_F_RELAXED);
		}
	}
#endif

#endif

	return 0;
}

#ifdef PANGOLIN_PARITY
/*
 * pangolin_clear_lane_ulogs -- free a lane's overflow ulogs
 */
static int
pangolin_clear_lane_ulogs(PMEMobjpool *pop, struct ulog *ulog)
{
	ASSERT(OBJ_OFF_FROM_LANES(pop, OBJ_PTR_TO_OFF(pop, ulog)));

	/*
	 * Each ulog_off belongs to the base ulog in a lane or an extended ulog.
	 * (the next field of struct ulog). Its value indicates where the next
	 * extended struct ulog exists (allocated memory block ofset). It will
	 * be given to pfree() to free the extended allocation, and pfree() will
	 * write zero to ulog_off.
	 */
	uint64_t *ulog_off;
	VEC(, uint64_t *) ulog_offs;
	VEC_INIT(&ulog_offs);

	while (ulog != NULL && ulog->next != 0) {
		int err = VEC_PUSH_BACK(&ulog_offs, &ulog->next);
		ASSERTeq(err, 0);
		ulog = ulog_next(ulog, &pop->p_ops);
	}

	int freed = 0;
	VEC_FOREACH_REVERSE(ulog_off, &ulog_offs) {
		pfree(pop, ulog_off);
		freed += 1;
	}

	VEC_DELETE(&ulog_offs);

	return freed;
}

/*
 * pangolin_clear_ulogs -- clear all transaction lanes and ulog entries
 *
 * This function should run before corruption recovery, to clear all lanes' logs
 * that overflow into the heap area. Logs do not affect parity computing (i.e.,
 * as if they were full of 0s). Thus, before corruption recovery, these overflow
 * logs should be freed and zeroed.
 */
static int
pangolin_clear_ulogs(PMEMobjpool *pop)
{
	/* free and zero-fill redo logs allocated in the zone region */
	for (uint64_t i = 0; i < pop->nlanes; i++) {
		struct ulog *ulog;
		struct lane *lane = &pop->lanes_desc.lane[i];

		/*
		 * Ulogs at lane->layout->internal do not overflow.
		 * See operation_new() in lane_init().
		 */

		/* Metadata redo */
		ulog = (struct ulog *)&lane->layout->external;
		if (pangolin_clear_lane_ulogs(pop, ulog) > 0) {
			pangolin_rebuild_lane_context(lane->external);
		}

		/* Pangolin's redo (though named undo) */
		ulog = (struct ulog *)&lane->layout->undo;
		if (pangolin_clear_lane_ulogs(pop, ulog) > 0) {
			pangolin_rebuild_lane_context(lane->undo);
		}
	}

	return 0;
}

/*
 * pangolin_repair_pop -- repair pool header corruption
 */
static int
pangolin_repair_pop(PMEMobjpool *pop, int hwe)
{
	int err = mprotect(pop, sizeof(struct pmemobjpool),
			PROT_READ | PROT_WRITE);
	if (err)
		return err;

	void *poprep = (char *)pop + MREP_SIZE;
	memcpy(pop, poprep, sizeof(struct pmemobjpool));
	pmemops_memcpy(&pop->p_ops, pop, poprep, sizeof(struct pmemobjpool), 0);

	return 0;
}

/*
 * pangolin_repair_metadata -- repair metadata corruption
 *
 * Parameter hwe = 1 indicates hardware error (single page).
 */
static int
pangolin_repair_metadata(PMEMobjpool *pop, void *ptr, int hwe)
{
	int err = mprotect(ptr, Pagesize, PROT_READ | PROT_WRITE);
	if (err)
		return err;

	enum ptoloc ploc = pangolin_ptoloc(pop, ptr);

	void *repaddr = NULL;
	if (ploc == PTO_LANE_HEAP)
		repaddr = (char *)ptr - MREP_SIZE;
	else if (ploc == PTO_ZONE_META)
		repaddr = (char *)ptr + ZONE_META_SIZE;

	if (repaddr) {
		pmemops_memcpy(&pop->p_ops, ptr, repaddr, Pagesize, 0);
	} else {
		err = 1;
		LOG_ERR("cannot locate metadata replication for %p!", ptr);
	}

	return err;
}

/*
 * pangolin_repair_corruption -- repair corrupted data
 *
 * Parameter hwe = 1 indicates a hardware error (single page).
 */
int
pangolin_repair_corruption(void *ptr, size_t size, int hwe, struct objbuf *obuf)
{
	struct obuf_runtime *ort = obuf_get_runtime();
	PMEMobjpool *pop = ort->pop;

	if (__atomic_exchange_n(&ort->freeze, 1, __ATOMIC_RELAXED))
		FATAL("another online reparing or scrubbing is in progress");

	uint64_t actv_txs = 0;
	struct obuf_tx *otx = obuf_get_tx();
	if (otx->stage != PANGOLIN_TX_NONE) {
		if (otx->stage != PANGOLIN_TX_WORK)
			FATAL("cannot recover outside PANGOLIN_TX_WORK stage");
		actv_txs = 1;
	}

	/* wait for all active transactions to complete */
	while (__atomic_load_n(&ort->actv_txs, __ATOMIC_RELAXED) != actv_txs);

	if ((uintptr_t)pop <= (uintptr_t)ptr &&
			(uintptr_t)ptr < (uintptr_t)pop + OBJ_LANES_OFFSET) {
		/* metadata, use replication to repair */
		int err = pangolin_repair_pop(pop, hwe);
		if (err)
			FATAL("cannot repair pool header errors");
		LOG_ERR("pool header error repaired at %p offset %zu!", ptr,
			(uintptr_t)ptr - (uintptr_t)pop);

		if (!__atomic_exchange_n(&ort->freeze, 0, __ATOMIC_RELAXED))
			FATAL("failed to clear the freeze flag");

		return 0;
	}

	pangolin_clear_ulogs(pop);

	/*
	 * This can be a SIGBUS/SIGSEGV not caused by Pmem. We need a better way
	 * to handle them.
	 */
	if (!OBJ_PTR_FROM_POOL(pop, ptr))
		FATAL("unexpected corruption location");

	uint64_t offset = OBJ_PTR_TO_OFF(pop, ptr);
	if (offset < ZONE_OFFSET) {
		/* metadata, use replication to repair */
		int err = pangolin_repair_metadata(pop, ptr, hwe);
		if (err)
			FATAL("cannot repair heap metadata errors");
		LOG_ERR("heap metadata error repaired at %p offset %zu!", ptr,
			(uintptr_t)ptr - (uintptr_t)pop);

		if (!__atomic_exchange_n(&ort->freeze, 0, __ATOMIC_RELAXED))
			FATAL("failed to clear the freeze flag");

		return err;
	}

	/* offset to the start of zone0 */
	offset -= ZONE_OFFSET;

	/*
	 * compute offset / ZONE_MAX_SIZE
	 * ZONE_MAX_SIZE is 16 GB so the while loop will not take many rounds.
	 */
	uint32_t zid = 0;
	while (offset >= ZONE_MAX_SIZE) {
		zid += 1;
		offset -= ZONE_MAX_SIZE;
	}

	if (offset < sizeof(struct zone)) {
		/* metadata, use replication to repair */
		int err = pangolin_repair_metadata(pop, ptr, hwe);
		if (err)
			FATAL("cannot repair zone metadata errors");
		LOG_ERR("zone metadata error repaired at %p offset %zu!", ptr,
			(uintptr_t)ptr - (uintptr_t)pop);

		if (!__atomic_exchange_n(&ort->freeze, 0, __ATOMIC_RELAXED))
			FATAL("failed to clear the freeze flag");

		return err;
	}

	void *heap_start = (char *)pop + pop->heap_offset;
	struct zone *z = ZID_TO_ZONE(heap_start, zid);  // ptr->offset(all)->zid->zone

	uint32_t rows, badrow = 0;
	uint64_t row_size, parity_start;  // get addr of row_size and parity_start
	if (zid != ort->last_zone_id) {
		rows = ort->zone_rows;
		row_size = ort->zone_row_size;
		parity_start = ort->zone_parity;
	} else {
		rows = ort->last_zone_rows;
		row_size = ort->last_zone_row_size;
		parity_start = ort->last_zone_parity;
	}

	if (size > row_size)
		FATAL("corruption size %zu > row_size %zu", size, row_size);

	/* offset into the zone's chunks */
	offset -= sizeof(struct zone);
	/* offset into the resident data chunk group */
	while (offset >= row_size) {  // which row is bad
		badrow += 1;
		offset -= row_size;
	}

	char *first_row = (char *)z + sizeof(struct zone); // meta
	char *parity_row = (char *)z + parity_start;

	/* an object may wrap around a row */
	size_t size1 = offset + size <= row_size ? size : row_size - offset;
	size_t size2 = size1 < size ? size - size1 : 0;

	/* Align for using SSE/AVX instructions. */
	char *repbuf = util_aligned_malloc(CACHELINE_SIZE, size);
	if (!repbuf)
		FATAL("util_aligned_malloc() failed for repair buffer");

	char *dataptr = first_row + offset;
	memcpy(repbuf, parity_row + offset, size1);
	for (uint32_t r = 0; r < rows - 1; r++) {
		if (!ALIGNED_16(dataptr) || !ALIGNED_16(repbuf))
			FATAL("incompatible data alignment for xor_gen_sse");

		/* skipping the bad row and parity row */
		if (r != badrow) {
			void *array[3] = {dataptr, repbuf, repbuf};
			xor_gen_sse_pgl(3, size1, array);
		}
		dataptr += row_size;
	}

	if (size2 != 0) {
		if (badrow == rows - 1)
			FATAL("corruption size error");

		badrow += 1;
		dataptr = first_row;
		void *xorptr = repbuf + size1;

		if (!ALIGNED_16(dataptr) || !ALIGNED_16(xorptr))
			FATAL("incompatible data alignment for xor_gen_sse");

		memcpy(xorptr, parity_row, size2);
		for (uint32_t r = 0; r < rows - 1; r++) {
			if (r != badrow) {
				void *array[3] = {dataptr, xorptr, xorptr};
				xor_gen_sse_pgl(3, size2, array);
			}
			dataptr += row_size;
		}
	}

	int repaired = 0;
	if (hwe) { /* hardware error */
		ASSERTeq(size, Pagesize);
		/* Using mprotect to emulate media errors. */
		if (mprotect(ptr, Pagesize, PROT_READ | PROT_WRITE) != 0)
			FATAL("cannot repair media errors");
		pmemops_memcpy(&pop->p_ops, ptr, repbuf, size, 0);
		/*
		 * TODO: We better run a whole-pool scrubbing now to ensure
		 * everything is fine.
		 */
		repaired = 1;
		LOG_ERR("media page error repaired at %p offset %zu", ptr,
			(uintptr_t)ptr - (uintptr_t)pop);
	} else { /* software corruption error */
		/* Verify data integrity using the repbuf. */
		struct objhdr *ohdr = (struct objhdr *)repbuf;
		void *uobj = (char *)ohdr + OHDR_SIZE;
		size_t user_size = USIZE(uobj);
		if (user_size > size)
			FATAL("repaired object size error");
		uint32_t csum = pangolin_adler32(CSUM0, uobj, user_size);

		if (csum == ohdr->csum) {
			repaired = 1;
			pmemops_memcpy(&pop->p_ops, ptr, repbuf, size, 0);
			if (obuf)
				memcpy(REAL(obuf->uobj), repbuf, size);
		}
	}

	util_aligned_free(repbuf);

	if (!__atomic_exchange_n(&ort->freeze, 0, __ATOMIC_RELAXED))
		FATAL("failed to clear the freeze flag");

	return repaired;
}

static void
signal_hdl(int sig, siginfo_t *siginfo, void *ptr)
{
	LOG_ERR("Catched SIGBUS/SIGSEGV when accessing %p!", siginfo->si_addr);
	void *page = (void *)((uint64_t)siginfo->si_addr & ~(Pagesize - 1));
	pangolin_repair_page(page);
}

/*
 * pangolin_register_sighdl -- register SIGBUS/SIGSEGV handler
 */
int
pangolin_register_sighdl(void)
{
	// struct sigaction sigbus_act;
	// struct sigaction sigsegv_act;

	// memset(&sigbus_act, 0, sizeof(sigbus_act));
	// sigbus_act.sa_sigaction = signal_hdl;
	// sigbus_act.sa_flags = SA_SIGINFO;

	// if (sigaction(SIGSEGV, &sigbus_act, 0))
	// 	FATAL("sigaction error!");

	// memset(&sigsegv_act, 0, sizeof(sigsegv_act));
	// sigsegv_act.sa_sigaction = signal_hdl;
	// sigsegv_act.sa_flags = SA_SIGINFO;

	// if (sigaction(SIGSEGV, &sigsegv_act, 0))
	// 	FATAL("sigaction error!");

	return 0;
}

#endif

/*
 * pgl_poison_obj_page -- emulate a bad page with mprotect
 */
void
pgl_poison_obj_page(PMEMoid oid)
{
	char *pobj = pmemobj_direct(oid);
	void *objpage = (void *)((uint64_t)pobj & ~(Pagesize - 1));

	mprotect(objpage, Pagesize, PROT_NONE);
}

/*
 * pgl_scribble_obj -- scribble an Pmem object's data for recovery testing
 */
void
pgl_scribble_obj(PMEMoid oid)
{
	char *pobj = pmemobj_direct(oid);
	size_t user_size = USIZE(pobj);

	for (size_t byte = 0; byte < user_size; byte++)
		pobj[byte] ^= (char)0xA3;
}

/* --------------------------- Pangolin Checksum --------------------------- */

/*
 * crc32_base -- compute a crc32 checksum
 */
static inline uint32_t
crc32_base(uint32_t init, void *data, size_t len)
{
	uint8_t *ptr = (uint8_t *)data;
	uint64_t acc = init; /* accumulator, crc32 value in lower 32b */

	while (len >= 8) {
		/* 64b quad words */
		asm volatile(
			"crc32q (%1), %0"
			: "=r" (acc)
			: "r"  (ptr), "0" (acc));
		ptr += 8;
		len -= 8;
	}

	while (len > 0) {
		/* trailing bytes */
		asm volatile(
			"crc32b (%1), %0"
			: "=r" (acc)
			: "r"  (ptr), "0" (acc));
		ptr++;
		len--;
	}

	return (uint32_t)acc;
}

#ifdef PANGOLIN_CHECKSUM
extern uint32_t crc32_gzip_refl_by8(uint32_t init, void *data, size_t len);
#endif

/*
 * pangolin_crc32 -- compute a crc32 checksum
 */
uint32_t
pangolin_crc32(uint32_t init, void *data, size_t len)
{
#ifdef PANGOLIN_CHECKSUM
	return crc32_gzip_refl_by8(init, data, len);
#else
	return crc32_base(init, data, len);
#endif
}

/*
 * adler32_base -- compute a adler32 checksum
 */
static inline uint32_t
adler32_base(uint32_t init, void *data, size_t len)
{
	uint8_t *end, *next = data;
	uint64_t lo = init & 0xffff;
	uint64_t hi = init >> 16;

	while (len > MAX_ADLER_BUF) {
		end = next + MAX_ADLER_BUF;
		for (; next < end; next++) {
			lo += *next;
			hi += lo;
		}

		lo = lo % ADLER_MOD;
		hi = hi % ADLER_MOD;
		len -= MAX_ADLER_BUF;
	}

	end = next + len;
	for (; next < end; next++) {
		lo += *next;
		hi += lo;
	}

	lo = lo % ADLER_MOD;
	hi = hi % ADLER_MOD;

	return (uint32_t)(hi << 16 | lo);
}

#ifdef PANGOLIN_CHECKSUM
extern uint32_t
adler32_sse(uint32_t init, void *data, size_t len);

extern uint32_t
adler32_avx2_4(uint32_t init, void *data, size_t len);

extern uint32_t
adler32_patch_sse(uint32_t csum, void *data, uint64_t dlen, uint64_t off,
	void *range, uint64_t rlen);

extern uint32_t
adler32_patch_avx2(uint32_t csum, void *data, uint64_t dlen, uint64_t off,
	void *range, uint64_t rlen);
#endif

/*
 * pangolin_adler32 -- compute an adler32 checksum
 */
uint32_t
pangolin_adler32(uint32_t init, void *data, size_t len)
{
#ifdef PANGOLIN_CHECKSUM
	// return adler32_sse(init, data, len);
	return adler32_avx2_4(init, data, len);  // use avx2
#else
	return adler32_base(init, data, len);
#endif
}

/*
 * adler32_patch_base -- update an adler32 checksum only using a range of
 * changed bytes, scalar version
 */
static inline uint32_t
adler32_patch_base(uint32_t csum, void *data, uint64_t dlen, uint64_t off,
	void *range, uint64_t rlen)
{
	uint8_t	*pr = range, *endpr;
	uint8_t *pd = (uint8_t *)data + off;
	uint32_t buflen = MAX_PATCH_BUF;
	uint64_t diff = 0;
	uint64_t hi = csum >> 16;
	uint64_t lo = csum & 0xffff;

	dlen = (dlen - off) % ADLER_MOD ? (dlen - off) % ADLER_MOD : ADLER_MOD;

	while (rlen > buflen) {
		for (endpr = pr + buflen; pr < endpr; pd++, pr++) {
			diff = (uint64_t)(*pd) + ADLER_MOD - (uint64_t)(*pr);
			lo += diff;
			hi += dlen * diff;
			dlen = dlen - 1 ? dlen - 1 : ADLER_MOD;
		}

		rlen -= buflen;
		lo = lo % ADLER_MOD;
		hi = hi % ADLER_MOD;
	}

	for (endpr = pr + rlen; pr < endpr; pd++, pr++) {
		diff = (uint64_t)(*pd) - (uint64_t)(*pr) + ADLER_MOD;
		lo += diff;
		hi += dlen * diff;
		dlen = dlen - 1 ? dlen - 1 : ADLER_MOD;
	}

	lo = lo % ADLER_MOD;
	hi = hi % ADLER_MOD;

	return (uint32_t)(hi << 16 | lo);
}

/*
 * pangolin_adler32_patch -- update an adler32 checksum only using a range of
 * changed bytes
 *
 * New data is referenced by (void *)data and old data is in (void *)range.
 * This should generate the same result as pangolin_adler32(CSUM0, data, dlen).
 *
 * csum: current checksum that is based on the old data
 * off: byte offset, relative to data, of the modified range
 * range: a buffer that contains old data for the modified range
 * rlen: size of the modified range
 */
uint32_t
pangolin_adler32_patch(uint32_t csum, void *data, uint64_t dlen, uint64_t off,
	void *range, uint64_t rlen)
{
#if 0//def PANGOLIN_CHECKSUM
	return adler32_patch_avx2(csum, data, dlen, off, range, rlen);
#else
	return adler32_patch_base(csum, data, dlen, off, range, rlen);
#endif
}

int
pangolin_update_zone_csum(PMEMobjpool *pop, int64_t zid, void *src, size_t len)
{
#ifdef PANGOLIN_CHECKSUM
	uint32_t zone_id = (uint32_t)zid;
	void *heap_start = (char *)pop + pop->heap_offset;
	if (zid < 0) {
		/* offset to the start of zone0 */
		uint64_t offset = (uint64_t)src - (uint64_t)heap_start -
			sizeof(struct heap_header);
		// zone_id = (uint32_t)(offset / ZONE_MAX_SIZE);
		zone_id = 0;
		while (offset >= ZONE_MAX_SIZE) {
			zone_id += 1;
			offset -= ZONE_MAX_SIZE;
		}
	}

	struct zone *z = ZID_TO_ZONE(heap_start, zone_id);
	struct zone_header *zhdr = &z->header;
	void *rep = (char *)src + ZONE_META_SIZE;

	uint32_t newcsum = pangolin_adler32_patch(zhdr->checksum,
		&zhdr->chkstart, ZONE_META_SIZE - sizeof(zhdr->checksum),
		(uintptr_t)src - (uintptr_t)&zhdr->chkstart, rep, len);

	zhdr->checksum = newcsum;
	pmemops_persist(&pop->p_ops, zhdr, sizeof(zhdr->checksum));

	rep = (char *)&zhdr->checksum + ZONE_META_SIZE;
	pmemops_memcpy(&pop->p_ops, rep, &newcsum, sizeof(newcsum),
		PMEMOBJ_F_RELAXED);
#endif

	return 0;
}

/* --------------------------- Pangolin Recovery --------------------------- */

int
pangolin_compare_zone_metarep(PMEMobjpool *pop, uint32_t zid)
{
#ifdef PANGOLIN_METAREP
	void *heap_start = (char *)pop + pop->heap_offset;
	struct zone *z = ZID_TO_ZONE(heap_start, zid);
	void *zone_meta = &z->header;
	void *zone_mrep = &z->rephdr;

	return memcmp(zone_meta, zone_mrep, ZONE_META_SIZE);
#else
	return 0;
#endif
}

/*
 * pangolin_check_zone -- check zone integrity and repair
 */
static int
pangolin_check_zone(PMEMobjpool *pop, uint32_t zid, int repair)
{
	int err = 0;

	void *heap_start = (char *)pop + pop->heap_offset;
	struct zone *z = ZID_TO_ZONE(heap_start, zid);

	if (!z) {
		LOG_ERR("invalid zone pointer");
		return 1;
	}


#ifdef PANGOLIN_CHECKSUM
	struct zone_header *zhdr = &z->header;
	uint32_t checksum = pangolin_adler32(CSUM0, &zhdr->chkstart,
		ZONE_META_SIZE - sizeof(zhdr->checksum));
	if (zhdr->magic == ZONE_HEADER_MAGIC && zhdr->checksum != checksum) {
		err += 1;
		LOG_ERR("zone %u metadata checksum error: stored 0x%x, "
			"calculated 0x%x", zid, zhdr->checksum, checksum);
		return err;
	}
#endif

#ifdef PANGOLIN_METAREP
	void *zone_meta = &z->header;
	void *zone_mrep = &z->rephdr;
	if (memcmp(zone_meta, zone_mrep, ZONE_META_SIZE) != 0) {
		err += 1;
		LOG_ERR("zone metadata replications do not match");
		return err;
	}
#endif

#ifdef PANGOLIN_PARITY
	pangolin_clear_ulogs(pop);

	struct obuf_runtime *ort = obuf_get_runtime();

	uint32_t rows = ort->zone_rows;
	uint32_t chunks = (uint32_t)(ort->zone_row_size / CHUNKSIZE);  // how many chunks in a row, chunks are vertically oriented
	if (zid == ort->last_zone_id) {
		rows = ort->last_zone_rows;
		chunks = (uint32_t)(ort->last_zone_row_size / CHUNKSIZE);
	}

	/* Verify per chunk-column. */
	uint32_t data_size = CHUNKSIZE;  // the granularity of the row to be checked is 256KB
	uint32_t data_words = data_size >> 3;
	/* Align for using SSE/AVX instructions. */
	uint64_t *xors = util_aligned_malloc(CACHELINE_SIZE, data_size);  // 256KB
	if (!xors) {
		obuf_destroy_runtime(ort->pop);
		FATAL("util_aligned_malloc() failed");
	}
	memset(xors, 0, data_size);

	uint32_t c, r, i;
	for (c = 0; c < chunks && !err; c++) {
		void *data;
		for (r = 0; r < rows; r++) {  // xor all data into xor
			data = &z->chunks[r * chunks + c];
			void *array[3] = {data, xors, xors};
			xor_gen_sse_pgl(3, data_size, array);
		}

		data = &z->chunks[c];
		for (i = 0; i < data_words; i++) {
			if (xors[i] != 0) {  // check 64bit/8B
				err += 1;
				LOG_ERR("chunk column xor error - zone %u "
					"[%u rows %u cols] column %u word %u "
					"xorval 0x%lx",
					zid, rows, chunks, c, i, xors[i]);
				goto check;
			}
		}
	}

check:
	if (err) {
		uint64_t *word, wordoff;
		LOG_ERR("inspect words of chunk column %u", c);
		for (r = 0; r < rows; r++) {
			word = (uint64_t *)&z->chunks[r * chunks + c];
			wordoff = OBJ_PTR_TO_OFF(pop, &word[i]);
			LOG_ERR("row %u word[%u] offset 0x%016lx value 0x%lx",
				r, i, wordoff, word[i]);
		}
	}

	util_aligned_free(xors);
#endif

	return err;
}

/*
 * pangolin_check_heap -- check heap integrity and repair
 */
static int
pangolin_check_heap(PMEMobjpool *pop, int repair)
{
	int err = 0;

	/*
	 * We only check if the replicas equal. Checksum can be verified by heap
	 * check function and we cam merge with the function.
	 * heap_check((char *)pop + pop->heap_offset, pop->heap_size);
	 */
	void *hdr = (char *)pop + pop->heap_offset;
	if (pangolin_check_metarep(pop, hdr, sizeof(struct heap_header) != 0)) {
		err += 1;
		LOG_ERR("heap header check failed");
	}

	struct obuf_runtime *ort = obuf_get_runtime();

	for (uint32_t zid = 0; zid < ort->nzones; zid++) {
		if (pangolin_check_zone(pop, zid, repair) != 0) {
			err += 1;
			LOG_ERR("zone %d check failed", zid);
		}
	}

	return err;
}

/*
 * pangolin_check_dscp -- check pool descriptor
 */
static int
pangolin_check_dscp(PMEMobjpool *pop, int repair)
{
	int err = 0;

#ifdef PANGOLIN_METAREP
	/*
	 * PGL-TODO: We only check if the replicas equal. Checksum is verified
	 * in obj_descr_check() and we should merge with the function.
	 *
	 * Range for (struct pool_hdr) was protected by RANGE_NONE.
	 */
	void *dscp = (void *)((uintptr_t)pop + sizeof(struct pool_hdr));
	/*
	 * Do not use pangolin_check_metarep() because pop's replica address is
	 * greater than itself.
	 */
	void *dscprep = (void *)((uintptr_t)dscp + MREP_SIZE);
	if (memcmp(dscp, dscprep, OBJ_DSC_P_SIZE) != 0) {
		err += 1;
		LOG_ERR("pool descriptor check failed");
	}
#endif

	return err;
}

/*
 * pangolin_check_lanes -- check lanes integrity and repair
 * struct lane_layout: internal, external, undo
 */
static int
pangolin_check_lanes(PMEMobjpool *pop, int repair)
{
	int err = 0;

	/*
	 * We have to free all the allocated pmem as redo logs and zero-fill
	 * them for erasure-coding consistency.
	 */
	for (uint64_t i = 0; i < pop->nlanes && err == 0; ++i) {
		struct lane_layout *layout = (struct lane_layout *)(
			(char *)pop + pop->lanes_offset +
			sizeof(struct lane_layout) * i);

		void *internal = &layout->internal;
		size_t intsize = sizeof(struct ULOG(LANE_REDO_INTERNAL_SIZE));
		if (pangolin_check_metarep(pop, internal, intsize) != 0) {
			err += 1;
			LOG_ERR("lane %zu internal check failed", i);
		}

		void *external = &layout->external;
		size_t extsize = sizeof(struct ULOG(LANE_REDO_EXTERNAL_SIZE));
		if (pangolin_check_metarep(pop, external, extsize) != 0) {
			err += 1;
			LOG_ERR("lane %zu external check failed", i);
		}

		void *redo = &layout->undo; /* 'undo' region used as redo */
		size_t redosize = sizeof(struct ULOG(LANE_UNDO_SIZE));
		if (pangolin_check_metarep(pop, redo, redosize) != 0) {
			err += 1;
			LOG_ERR("lane %zu redo check failed", i);
		}
	}

	return err;
}

/*
 * pangolin_check_pool -- check pool integrity and repair
 */
int
pangolin_check_pool(PMEMobjpool *pop, int repair)
{
	int err = 0;

	err += pangolin_check_dscp(pop, repair);  // replicated dscp
	err += pangolin_check_heap(pop, repair);
	err += pangolin_check_lanes(pop, repair); // replicated lane_layout

	return err;
}

/*
 * pangolin_check_objs -- check user object integrity and repair
 */
int
pangolin_check_objs(PMEMobjpool *pop, int repair)
{
#ifdef PANGOLIN_CHECKSUM
	PMEMoid oid;
	struct obuf_runtime *ort = obuf_get_runtime();
	POBJ_FOREACH(pop, oid) {
		void *pobj = pmemobj_direct(oid);  // pointer to data area
		uint32_t csum = pangolin_adler32(CSUM0, pobj, USIZE(pobj));
		if (OHDR(pobj)->csum != csum) {
			LOG_ERR("object checksum error - oid.off 0x%lx, "
				"stored checksum 0x%x, computed checksum 0x%x",
				oid.off, OHDR(pobj)->csum, csum);
			return 1;
		}
	}
#endif
	return 0;
}

/*
 * pangolin_check_obj_one -- check one object integrity
 * @param pobj: pointer to the object, `pobj = pmemobj_direct(oid)`
*/
int
pangolin_check_obj_one(uint64_t *pobj)
{
#ifdef PANGOLIN_CHECKSUM
	uint32_t csum = pangolin_adler32(CSUM0, pobj, USIZE(pobj));
	if (OHDR(pobj)->csum != csum) {
		LOG_ERR("object checksum error (pangolin_check_obj_one)");
		return 1;
	}
#endif
	return 0;
}

/* --------------------------- Pangolin Scrub --------------------------- */

void
pangolin_scrub_task(struct obuf_runtime *ort, enum scrub_trigger trigger)
{
	static uint64_t nr_txs = 0;
	static uint64_t nr_timed = 0;

	/* wait for acquiring the freeze flag */
	while (__atomic_exchange_n(&ort->freeze, 1, __ATOMIC_RELAXED));
	/* wait for all active transactions to complete */
	while (__atomic_load_n(&ort->actv_txs, __ATOMIC_RELAXED) != 0);

	if (trigger == SCRUB_TXS) {
		nr_txs += 1;
		LOG_BLU(1, "transaction-based scrubbing #%lu started", nr_txs);
	} else {
		nr_timed += 1;
		LOG_BLU(1, "time-based scrubbing #%lu started", nr_timed);
	}

	/* actual scrubbing task */
	if (pangolin_check_objs(ort->pop, 1) != 0) {
		obuf_destroy_runtime(ort->pop);
		FATAL("pangolin_check_objs() failed");
	}

	if (trigger == SCRUB_TXS) {
		LOG_BLU(1, "transaction-based scrubbing #%lu finished", nr_txs);
	} else {
		LOG_BLU(1, "time-based scrubbing #%lu finished", nr_timed);
	}

	if (!__atomic_exchange_n(&ort->freeze, 0, __ATOMIC_RELAXED))
		FATAL("failed to clear the freeze flag");
}

static void *
pangolin_scrub_thread()
{
	struct epoll_event event;
	struct obuf_runtime *ort = obuf_get_runtime();
	while (1) {
		memset(&event, 0, sizeof(event));
		ssize_t nr_events = epoll_wait(ort->scrub_efd, &event, 1, -1);
		if (nr_events != 1) {
			obuf_destroy_runtime(ort->pop);
			FATAL("epoll_wait received more than 1 event");
		}

		/*
		 * Make this thread cancellable at pthread_testcancel() so it
		 * can be stopped when destroying pangolin runtime.
		 */
		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
		pthread_testcancel();
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);

		if (event.data.fd != ort->scrub_tfd) {
			obuf_destroy_runtime(ort->pop);
			FATAL("epoll_wait received unrecognized event");
		}

		uint64_t data;
		if (read(ort->scrub_tfd, &data, sizeof(data)) != sizeof(data)) {
			obuf_destroy_runtime(ort->pop);
			FATAL("invalid read(ort->scrub_tfd) size");
		}

		pangolin_scrub_task(ort, SCRUB_TIMED);
	}
}

static int
pangolin_start_scrub_thread(struct obuf_runtime *ort)
{
	/*
	 * The minimum scrubbing interval is 1s. If both timing parameters are
	 * zero, do not create time-based scrub thread.
	 */
	if (ort->scrub_sec == 0 && ort->scrub_msec == 0)
		return 0;

	struct itimerspec interval =
	{
		/* seconds, nano-seconds */
		.it_interval = {ort->scrub_sec, ort->scrub_msec * 1000000},
		.it_value    = {ort->scrub_sec, ort->scrub_msec * 1000000}
	};

	int scrub_tfd = timerfd_create(CLOCK_MONOTONIC, 0);
	if (scrub_tfd == -1) {
		LOG_ERR("timerfd_create() failed");
		return -1;
	}

	if (timerfd_settime(scrub_tfd, 0, &interval, NULL) < 0) {
		LOG_ERR("timerfd_settime() failed");
		close(scrub_tfd);
		return -1;
	}

	int scrub_efd = epoll_create1(0);
	if (scrub_efd == -1) {
		LOG_ERR("epoll_create1() failed");
		close(scrub_tfd);
		return -1;
	}

	struct epoll_event event;
	event.events = EPOLLIN;
	event.data.fd = scrub_tfd;
	if (epoll_ctl(scrub_efd, EPOLL_CTL_ADD, scrub_tfd, &event) == -1) {
		LOG_ERR("epoll_ctl() failed");
		close(scrub_tfd);
		close(scrub_efd);
		return -1;
	}

	if (pthread_create(&ort->scrub_tid, NULL, pangolin_scrub_thread, NULL)
		!= 0) {
		LOG_ERR("pthread_create() failed");
		close(scrub_tfd);
		close(scrub_efd);
		return -1;
	}

	ort->scrub_tid_valid = 1;
	ort->scrub_tfd = scrub_tfd;
	ort->scrub_efd = scrub_efd;

	return 0;
}

int
pangolin_setup_scrub(PMEMobjpool *pop, struct obuf_runtime *ort)
{
	/* zeroing the three parameters disables scrubbing */
	ort->scrub_txs = 0;
	ort->scrub_sec = 0;
	ort->scrub_msec = 0;

	char *env_scrub_txs = os_getenv("PANGOLIN_SCRUB_TXS");
	if (env_scrub_txs) {
		uint64_t scrub_txs = strtoul(env_scrub_txs, NULL, 0);
		if (scrub_txs < 100) {
			LOG_ERR("PANGOLIN_SCRUB_TXS %lu < 100", scrub_txs);
			return -1;
		}
		ort->scrub_txs = scrub_txs;
		LOG_BLU(0, "scrub enabled per %lu transactions", scrub_txs);
	}

	char *env_scrub_sec = os_getenv("PANGOLIN_SCRUB_SEC");
	if (env_scrub_sec) {
		time_t scrub_sec = strtol(env_scrub_sec, NULL, 0);
		if (scrub_sec < 1) {
			LOG_ERR("PANGOLIN_SCRUB_SEC %ld too small", scrub_sec);
			return -1;
		}
		ort->scrub_sec = scrub_sec;
	}

	char *env_scrub_msec = os_getenv("PANGOLIN_SCRUB_MSEC");
	if (env_scrub_msec) {
		time_t scrub_msec = strtol(env_scrub_msec, NULL, 0);
		if (scrub_msec < 0) {
			LOG_ERR("PANGOLIN_SCRUB_MSEC %ld negative", scrub_msec);
			return -1;
		}
		ort->scrub_msec = scrub_msec;
	}

	ort->scrub_tfd = -1;
	ort->scrub_efd = -1;
	ort->scrub_tid_valid = 0;

	if (ort->scrub_sec != 0 || ort->scrub_msec != 0) {
		if (pangolin_start_scrub_thread(ort) != 0) {
			LOG_ERR("pangolin_init_scrub_thread() failed");
			return -1;
		}
		LOG_BLU(0, "scrub enabled per %ld.%ld/1000 seconds",
			ort->scrub_sec, ort->scrub_msec);
	}

	return 0;
}

#define CHUNK_HDR_SIZE 320

/*
 * DEPRECATED!
 * multi-thread scan zone worker
 * some simple OPTs: 1) directly avx512 cmp with parity, 2) mv alloc to external, 3) rm some parts e.g. check meta
*/
// void *
// pangolin_scan_worker_obj(void * args_thread)
// {	
// 	// load params
// 	thread_param * args = (thread_param *) args_thread;
// 	uint32_t chunks = args->chunks;
// 	uint32_t rows = args->rows;
// 	uint64_t * xors_local = args->xors_local;
// 	struct objhdr * data_buf_local = args->data_buf_local;
// 	struct zone *z = args->z;
// 	size_t obj_size = args->obj_size;

// 	uint32_t c_start = args->c_start;
// 	uint32_t c_end = args->c_end;

// 	// scan objsize as chunksize for cache friendly
// 	uint32_t num_obj_per_chunk = CHUNKSIZE / obj_size;
// 	uint32_t c = 0;
// 	for (c = c_start; c < c_end; c++) {  // scan this chunk level stripe
// 		uint32_t i_obj = 0;  // scan the i_th obj in this chunk
// 		for (i_obj = 0; i_obj < num_obj_per_chunk; i_obj++) {  // scan obj
// 			uint32_t r = 0;
// 			memset(xors_local, 0, obj_size);
// 			for (r = 0; r < rows-1; r++) {  // encoding all data rows
// 				void *data = &z->chunks[r * chunks + c];  // chunk raw pointer including obj's header
// 				struct objhdr * obj = (struct objhdr *) ((uintptr_t) data + CHUNK_HDR_SIZE + i_obj * obj_size);
// 				if (obj->size!=obj_size) break;  // not filled

// 				memcpy(data_buf_local, obj, obj_size);
// 				uint32_t csum_old = 0;
// 				uint32_t csum = 0;
// 				csum_old = data_buf_local->csum;
				
// 				void * obj_data = REAL_REVERSE(data_buf_local);
// 				size_t obj_usize = USIZE(obj_data);
// 				csum = pangolin_adler32(CSUM0, obj_data, obj_usize); // csum validation
// 				if (csum_old != csum) continue;   // @TODO: recover broken data
				
// 				void *array[3] = {data_buf_local, xors_local, xors_local};  // do encoding in objsize granularity
// 				xor_gen_avx512(3, obj_size, array);
// 			}  // finish encoding
			
// 			// compare with stored parity
// 			uintptr_t parity = &z->chunks[(rows-1)*chunks + c];
// 			if (memcmp(xors_local, parity + CHUNK_HDR_SIZE + i_obj * obj_size, obj_size)) continue;
// 			// TODO: recover broken data
// 		}

// 	}
// 	return NULL;
// }

/**
 * inject ramdom error in data
 * 
 */
#define ERROR_RATE 1/1000000000 // 10e−9 to 10e−4
void error_injection(char * data, size_t len) {
	uint32_t * p = (uint32_t *)data;
	// TODO

}

/*
 * multi-thread scan zone worker
 * some simple OPTs: 1) directly avx512 cmp with parity, 2) mv alloc to external, 3) rm some parts e.g. check meta
 * TODO: maybe read PM directly is faster? any cache friendly write?
*/
void *
pangolin_scan_worker_chunk(void * args_thread)
{	
	// load params
	thread_param * args = (thread_param *) args_thread;
	uint32_t chunks = args->chunks;
	uint32_t rows = args->rows;
	// uint64_t * xors_local = args->xors_local;
	uint64_t * data_buf_local = args->data_buf_local;
	struct zone *z = args->z;
	size_t obj_size = args->obj_size;
	size_t num_parity = args->num_parity;
	uint64_t ** parity_local = args->parity_local;

	uint32_t c_start = args->c_start;
	uint32_t c_end = args->c_end;

	uint32_t c;
	uint32_t r;
	struct objhdr * obj = NULL;

	for (c = c_start; c < c_end; c++) {  // scan this chunk level stripe
		uint8_t flag_no_csum = 0;  // // not filled -> no re-checksum
		
		// read to DRAM buffer, check csum, gen new parity
		for (r = 0; r < rows - num_parity; r++) {
			void *data = &z->chunks[r * chunks + c];  // chunk raw pointer, including obj's header
			memcpy(data_buf_local, data, CHUNKSIZE);  // read PM data to DRAM buffer
			/* scan this chunk and valid csum */
			for (uintptr_t ptr_offset = CHUNK_HDR_SIZE; flag_no_csum!=1 && ptr_offset < CHUNKSIZE - obj_size; ptr_offset += obj_size) {
				uint32_t csum_old = 0;
				uint32_t csum = 0;
				obj = (struct objhdr *) ((uintptr_t) data_buf_local + ptr_offset);
				if (obj->size != obj_size) {  // not filled
					flag_no_csum = 1;
					break;
				}
				csum_old = obj->csum;
				
				void * obj_data = REAL_REVERSE(obj);
				size_t obj_usize = USIZE(obj_data);
				csum = pangolin_adler32(CSUM0, obj_data, obj_usize);
				if (csum_old != csum) {
					// TODO: recover broken data (checksum)
				}
				
			}
			struct ec_runtime *ec = ec_get_runtime();
			encode_update(ec, data_buf_local, parity_local, CHUNKSIZE, r, PANGOLIN_EC_TEMPORAL);
		}


		// compare coded parity and stored parity
		for (int i = 0; i < num_parity; i++) {
			void *parity = &z->chunks[(rows-num_parity+i)*chunks + c];
				if (memcmp(parity_local[i], parity, CHUNKSIZE)) {
					// TODO: recover broken parity
				}
			}
		// clean parity_local
		for (int i = 0; i < num_parity; i++) memset(parity_local[i], 0, CHUNKSIZE);
	}
	return NULL;
}

/*
 * Scan a zone with zid. 
 * refer to pangolin_check_zone()
 * TODO: only scan, assume no errors, no write back
*/
static int
pangolin_scan_zone(PMEMobjpool *pop, uint32_t zid, int repair, uint32_t num_thread, uint32_t num_parity)
{
#ifdef PANGOLIN_PARITY
	void *heap_start = (char *)pop + pop->heap_offset;
	struct zone *z = ZID_TO_ZONE(heap_start, zid);
	if (!z) return 1;
	pangolin_clear_ulogs(pop);  // clean overflow logs
	struct obuf_runtime *ort = obuf_get_runtime();

	uint32_t rows = ort->zone_rows;
	uint32_t chunks = (uint32_t)(ort->zone_row_size / CHUNKSIZE);
	if (zid == ort->last_zone_id) {
		rows = ort->last_zone_rows;
		chunks = (uint32_t)(ort->last_zone_row_size / CHUNKSIZE);
	}

	void * tmp = &z->chunks[0];
	size_t obj_size = ((struct objhdr *) ((uintptr_t) tmp + CHUNK_HDR_SIZE))->size;

	/*
	 * assign different chunks to different physical threads
	*/
	pthread_t *threads = (pthread_t *) malloc(num_thread * sizeof(pthread_t));
    for (int i = 0; i < num_thread; i++) {
        pthread_t t = 0;
        threads[i] = t;
    }
    thread_param *args_array = (thread_param *) malloc(num_thread * sizeof(thread_param));
	for (int i = 0; i < num_thread; i++) {
		thread_param *args = &args_array[i];
		// split chunks for different thread
		args->c_start = i * chunks / num_thread;
		if (i != num_thread - 1)
			args->c_end = (i+1) * chunks / num_thread;
		else
			args->c_end = chunks;
		args->chunks = chunks;
		args->rows = rows;
		args->z = z;
		args->obj_size = obj_size;
		args->data_buf_local = util_aligned_malloc(CACHELINE_SIZE, CHUNKSIZE);  // read all data to DRAM buffer
		memset(args->data_buf_local, 0, CHUNKSIZE * (rows-num_parity));
		args->num_parity = num_parity;
		// args->xors_local = util_aligned_malloc(CACHELINE_SIZE, CHUNKSIZE);
		args->parity_local = util_aligned_malloc(sizeof(uint64_t *), num_parity);
		for (int j = 0; i < num_parity; i++) {
			args->parity_local[j] = util_aligned_malloc(CACHELINE_SIZE, CHUNKSIZE);
			memset(args->parity_local[j], 0, CHUNKSIZE);
		}
		pthread_create(&threads[i], NULL, pangolin_scan_worker_chunk, args);  
	}
	for (int i = 0; i < num_thread; i++) {
        pthread_join(threads[i], NULL);
    }

	// free
	// for (int i = 0; i < num_thread; i++) {
	// 	thread_param *args = &args_array[i];
	// 	args->xors_local = util_aligned_malloc(CACHELINE_SIZE, CHUNKSIZE);;
	// 	args->data_buf_local = util_aligned_malloc(CACHELINE_SIZE, CHUNKSIZE);
	// }
	free(args_array);

#endif
	return 0;
}

/*
 * refet to pangolin_check_heap() and pangolin_check_pool()
 * scan whole heap for crash recovery
 * TODO: only support basic scan now! no reconstruct support
*/
int
pangolin_scan(PMEMobjpool *pop, int repair, size_t num_thread, size_t num_parity)
{
	struct obuf_runtime *ort = obuf_get_runtime();
	// TODO: now only zone-level parallel, can extend to cross-zone
	for (uint32_t zid = 0; zid < ort->nzones; zid++) {
		if (pangolin_scan_zone(pop, zid, repair, num_thread, num_parity)) {
			return 1;
		}
	}
	return 0;
}