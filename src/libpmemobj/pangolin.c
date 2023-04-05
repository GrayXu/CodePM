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
xor_gen_sse(int vects, size_t len, void **array);

extern int
xor_gen_avx(int vects, size_t len, void **array);

extern int
xor_gen_avx512(int vects, size_t len, void **array);

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
		os_rwlock_wrlock(&ort->zone_rclock[zid][rclock]);
	else
		os_rwlock_rdlock(&ort->zone_rclock[zid][rclock]);

	if (size >= ort->rclock_threshold) {
#endif
		/*
		 * xor_gen_sse() requires 16-byte alignment
		 * xor_gen_avx() and xor_gen_avx512() require 32-byte alignment
		 */
		ASSERT(ALIGNED_32(data));
		ASSERT(ALIGNED_32(parity));
		if (newd == NULL) {
			void *array[3] = {data, parity, parity};
			xor_gen_sse(3, size, array);
		} else {
			ASSERT(ALIGNED_16(newd));
			void *array[4] = {data, newd, parity, parity};
			xor_gen_sse(4, size, array);
		}
#ifndef PANGOLIN_RCLOCKHLE
	} else {
		size_t words = size >> 3;
		if (newd == NULL) {
			for (size_t i = 0; i < words; i++)
				__atomic_xor_fetch(&parity[i], data[i],
					__ATOMIC_RELAXED);
		} else {
			for (size_t i = 0; i < words; i++)
				__atomic_xor_fetch(&parity[i],
					data[i] ^ newd[i], __ATOMIC_RELAXED);
		}
	}
#endif

#ifdef PANGOLIN_RCLOCKHLE
	__atomic_clear(&ort->zone_rclock[zid][rclock],
		__ATOMIC_RELEASE | __ATOMIC_HLE_RELEASE);
#else
	os_rwlock_unlock(&ort->zone_rclock[zid][rclock]);
#endif

	return 0;
}
#endif

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
	xor_gen_avx(4, size, array);

	pmemops_persist(&pop->p_ops, dst, size);
#endif
	return 0;
}

/*
 * pangolin_update_parity -- update a range of parity as a result of data change
 *
 * Content of [data : data + size) will change to [newd : newd + size), as a
 * result of a store or memcpy-kind of operation.
 *
 * Address of data determines the affected parity region. If newd is not NULL,
 * parity = parity XOR (data XOR newd); otherwise parity = parity XOR data.
 */
int
pangolin_update_parity(PMEMobjpool *pop, void *data, void *newd, size_t size)
{
#ifdef PANGOLIN_PARITY
	/* size should always be 8-byte (or more) aligned */
	ASSERT(ALIGNED_8(data));
	ASSERT(ALIGNED_8(size));
	ASSERT(ALIGNED_8(newd));

	ASSERT(OBJ_PTR_FROM_POOL(pop, data));
	struct obuf_runtime *ort = obuf_get_runtime();

	void *heap_start = (char *)pop + pop->heap_offset;
	/* offset to the start of zone0 */
	uint64_t offset = (uint64_t)data - (uint64_t)heap_start -
		sizeof(struct heap_header);
	/*
	 * compute offset / ZONE_MAX_SIZE
	 * ZONE_MAX_SIZE is 16 GB so the while loop will not take many rounds.
	 */
	uint32_t zid = 0;
	while (offset >= ZONE_MAX_SIZE) {
		zid += 1;
		offset -= ZONE_MAX_SIZE;
	}
	struct zone *z = ZID_TO_ZONE(heap_start, zid);

	uint64_t row_size, parity_start;
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
	while (offset >= row_size)
		offset -= row_size;

	uint32_t rclock = (uint32_t)(offset >> RCLOCKSHIFT);

	size_t parity_size = size;
	void *parity_ptr = (char *)z + parity_start + offset;
	/* off: offset within a page; len: bytes to update within a page */
	uint64_t *parity, off, len;
	while (size > 0) {
		ASSERT(offset < row_size);
		ASSERT(ALIGNED_8(offset));

		/*
		 * PGL-OPT: Updating parity per page causes some overhead if
		 * offset + size is not page-aligned. Try increasing RCLOCKSIZE
		 * see the perferformance changes.
		 */
		off = offset & ((uint64_t)RCLOCKSIZE - 1);
		len = (off + size < RCLOCKSIZE) ? size : RCLOCKSIZE - off;

		parity = (uint64_t *)((char *)z + parity_start + offset);
		ASSERT(OBJ_PTR_FROM_POOL(pop, parity));
		pangolin_update_column_parity(pop, data, newd, parity,
			len, zid, rclock);

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

	/* sfence (drain) will execute after obuf_tx_heap_commit() */
	pmemops_xpersist(&pop->p_ops, parity_ptr, parity_size,
		PMEMOBJ_F_RELAXED);
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
	struct zone *z = ZID_TO_ZONE(heap_start, zid);

	uint32_t rows, badrow = 0;
	uint64_t row_size, parity_start;
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
	while (offset >= row_size) {
		badrow += 1;
		offset -= row_size;
	}

	char *first_row = (char *)z + sizeof(struct zone);
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
			xor_gen_sse(3, size1, array);
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
				xor_gen_sse(3, size2, array);
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
	struct sigaction sigbus_act;
	struct sigaction sigsegv_act;

	memset(&sigbus_act, 0, sizeof(sigbus_act));
	sigbus_act.sa_sigaction = signal_hdl;
	sigbus_act.sa_flags = SA_SIGINFO;

	if (sigaction(SIGSEGV, &sigbus_act, 0))
		FATAL("sigaction error!");

	memset(&sigsegv_act, 0, sizeof(sigsegv_act));
	sigsegv_act.sa_sigaction = signal_hdl;
	sigsegv_act.sa_flags = SA_SIGINFO;

	if (sigaction(SIGSEGV, &sigsegv_act, 0))
		FATAL("sigaction error!");

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
	return adler32_sse(init, data, len);
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
	uint32_t chunks = (uint32_t)(ort->zone_row_size / CHUNKSIZE);
	if (zid == ort->last_zone_id) {
		rows = ort->last_zone_rows;
		chunks = (uint32_t)(ort->last_zone_row_size / CHUNKSIZE);
	}

	/* Verify per chunk-column. */
	uint32_t data_size = CHUNKSIZE;
	uint32_t data_words = data_size >> 3;
	/* Align for using SSE/AVX instructions. */
	uint64_t *xors = util_aligned_malloc(CACHELINE_SIZE, data_size);
	if (!xors) {
		obuf_destroy_runtime(ort->pop);
		FATAL("util_aligned_malloc() failed");
	}
	memset(xors, 0, data_size);

	uint32_t c, r, i;
	for (c = 0; c < chunks && !err; c++) {
		void *data;
		for (r = 0; r < rows; r++) {
			data = &z->chunks[r * chunks + c];
			void *array[3] = {data, xors, xors};
			xor_gen_sse(3, data_size, array);
		}

		data = &z->chunks[c];
		for (i = 0; i < data_words; i++) {
			if (xors[i] != 0) {
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

	err += pangolin_check_dscp(pop, repair);
	err += pangolin_check_heap(pop, repair);
	err += pangolin_check_lanes(pop, repair);

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
	POBJ_FOREACH(pop, oid) {
		void *pobj = pmemobj_direct(oid);
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
