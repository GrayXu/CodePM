/*
 * Copyright 2015-2018, Intel Corporation
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
 * ulog.c -- unified log implementation
 */

#include <inttypes.h>
#include <string.h>

#include "libpmemobj.h"
#include "ulog.h"
#include "obj.h"
#include "out.h"
#include "util.h"
#include "valgrind_internal.h"

#include "pangolin.h"

/*
 * Operation flag at the three most significant bits
 */
#define ULOG_OPERATION(op)		((uint64_t)(op))
#define ULOG_OPERATION_MASK		((uint64_t)(0b111ULL << 61ULL))
#define ULOG_OPERATION_FROM_OFFSET(off)	(ulog_operation_type)\
	((off) & ULOG_OPERATION_MASK)
#define ULOG_OFFSET_MASK		(~(ULOG_OPERATION_MASK))

#define CACHELINE_ALIGN(size) ALIGN_UP(size, CACHELINE_SIZE)
#define IS_CACHELINE_ALIGNED(ptr)\
	(((uintptr_t)(ptr) & (CACHELINE_SIZE - 1)) == 0)

/*
 * ulog_by_offset -- (internal) calculates the ulog pointer
 */
static struct ulog *
ulog_by_offset(size_t offset, const struct pmem_ops *p_ops)
{
	if (offset == 0)
		return NULL;

	size_t aligned_offset = CACHELINE_ALIGN(offset);

	return (struct ulog *)((char *)p_ops->base + aligned_offset);
}

/*
 * ulog_next -- retrieves the pointer to the next ulog
 */
struct ulog *
ulog_next(struct ulog *ulog, const struct pmem_ops *p_ops)
{
	return ulog_by_offset(ulog->next, p_ops);
}

/*
 * ulog_operation -- returns the type of entry operation
 */
ulog_operation_type
ulog_entry_type(const struct ulog_entry_base *entry)
{
	return ULOG_OPERATION_FROM_OFFSET(entry->offset);
}

/*
 * ulog_offset -- returns offset
 */
uint64_t
ulog_entry_offset(const struct ulog_entry_base *entry)
{
	return entry->offset & ULOG_OFFSET_MASK;
}

/*
 * ulog_entry_size -- returns the size of a ulog entry
 */
size_t
ulog_entry_size(const struct ulog_entry_base *entry)
{
	struct ulog_entry_buf *eb;

	switch (ulog_entry_type(entry)) {
		case ULOG_OPERATION_AND:
		case ULOG_OPERATION_OR:
		case ULOG_OPERATION_SET:
			return sizeof(struct ulog_entry_val);
		case ULOG_OPERATION_BUF_SET:
		case ULOG_OPERATION_BUF_CPY:
			eb = (struct ulog_entry_buf *)entry;
			return CACHELINE_ALIGN(
				sizeof(struct ulog_entry_buf) + eb->size);
		default:
			ASSERT(0);
	}

	return 0;
}

/*
 * ulog_entry_valid -- (internal) checks if a ulog entry is valid
 * Returns 1 if the range is valid, otherwise 0 is returned.
 */
static int
ulog_entry_valid(const struct ulog_entry_base *entry)
{
	if (entry->offset == 0)
		return 0;

	size_t size;
	struct ulog_entry_buf *b;

	switch (ulog_entry_type(entry)) {
		case ULOG_OPERATION_BUF_CPY:
		case ULOG_OPERATION_BUF_SET:
			size = ulog_entry_size(entry);
			b = (struct ulog_entry_buf *)entry;
			if (!util_checksum(b, size, &b->checksum, 0, 0))
				return 0;
			break;
		default:
			break;
	}

	return 1;
}

/*
 * ulog_construct -- initializes the ulog structure
 */
#ifndef PANGOLIN_LOGREP
void
ulog_construct(uint64_t offset, size_t capacity, int flush,
	const struct pmem_ops *p_ops)
#else
void
ulog_construct(uint64_t offset, size_t capacity, int flush, int extended,
	const struct pmem_ops *p_ops)
#endif
{
	struct ulog *ulog = ulog_by_offset(offset, p_ops);
	ASSERTne(ulog, NULL);

	VALGRIND_ADD_TO_TX(ulog, SIZEOF_ULOG(capacity));

#ifdef PANGOLIN_LOGREP
	size_t main_size = capacity;
	PMEMobjpool *pop = (PMEMobjpool *)p_ops->base;
	if (extended) {
		/* this is an extended ulog construction */
		main_size = pop->tx_params->cache_size;
		if (capacity < main_size * 2) {
			FATAL("capacity %zu < main_size * 2 %zu",
				capacity, main_size * 2);
		}
	}
	ulog->capacity = main_size;
#else
	ulog->capacity = capacity;
#endif
	ulog->checksum = 0;
	ulog->next = 0;
#ifdef PANGOLIN
	ulog->replay = 0;
#endif
	memset(ulog->unused, 0, sizeof(ulog->unused));

	if (flush) {
		pmemops_xflush(p_ops, ulog, sizeof(*ulog),
			PMEMOBJ_F_RELAXED);
		pmemops_memset(p_ops, ulog->data, 0, capacity,
			PMEMOBJ_F_MEM_NONTEMPORAL |
			PMEMOBJ_F_MEM_NODRAIN |
			PMEMOBJ_F_RELAXED);
	} else {
		/*
		 * We want to avoid replicating zeroes for every ulog of every
		 * lane, to do that, we need to use plain old memset.
		 */
		memset(ulog->data, 0, capacity);
	}

#ifdef PANGOLIN_LOGREP
	void *ulogrep = pangolin_repaddr(pop, ulog);
	size_t repsize = SIZEOF_ULOG(capacity); /* for in-lane ulogs */
	if (extended) { /* in-heap ulog */
		/*
		 * When constructing an extended ulog (allocated in-heap), its
		 * content is already zero-filled with the call to
		 * pmemops_memset() above. We only need to replicate the
		 * capacity value.
		 */
		repsize = sizeof(*ulog);
	}

	if (flush) {
		pmemops_memcpy(p_ops, ulogrep, ulog, repsize,
			PMEMOBJ_F_MEM_NONTEMPORAL |
			PMEMOBJ_F_MEM_NODRAIN |
			PMEMOBJ_F_RELAXED);
	} else {
		memcpy(ulogrep, ulog, repsize);
	}
#endif

	VALGRIND_REMOVE_FROM_TX(ulog, SIZEOF_ULOG(capacity));
}

/*
 * ulog_foreach_entry -- iterates over every existing entry in the ulog
 */
int
ulog_foreach_entry(struct ulog *ulog,
	ulog_entry_cb cb, void *arg, const struct pmem_ops *ops)
{
	struct ulog_entry_base *e;
	int ret = 0;

	for (struct ulog *r = ulog; r != NULL; r = ulog_next(r, ops)) {
		for (size_t offset = 0; offset < r->capacity; ) {
			e = (struct ulog_entry_base *)(r->data + offset);
			if (!ulog_entry_valid(e))
				return ret;

			if ((ret = cb(e, arg, ops)) != 0)
				return ret;

			offset += ulog_entry_size(e);
		}
	}

	return ret;
}

/*
 * ulog_capacity -- (internal) returns the total capacity of the ulog
 */
size_t
ulog_capacity(struct ulog *ulog, size_t ulog_base_bytes,
	const struct pmem_ops *p_ops)
{
	size_t capacity = ulog_base_bytes;

	/* skip the first one, we count it in 'ulog_base_bytes' */
	while ((ulog = ulog_next(ulog, p_ops)) != NULL) {
		capacity += ulog->capacity;
	}

	return capacity;
}

/*
 * ulog_rebuild_next_vec -- rebuilds the vector of next entries
 */
void
ulog_rebuild_next_vec(struct ulog *ulog, struct ulog_next *next,
	const struct pmem_ops *p_ops)
{
	do {
		if (ulog->next != 0)
			VEC_PUSH_BACK(next, ulog->next);
	} while ((ulog = ulog_next(ulog, p_ops)) != NULL);
}

/*
 * ulog_reserve -- reserves new capacity in the ulog
 */
int
ulog_reserve(struct ulog *ulog,
	size_t ulog_base_nbytes, size_t *new_capacity, ulog_extend_fn extend,
	struct ulog_next *next,
	const struct pmem_ops *p_ops)
{
	size_t capacity = ulog_base_nbytes;

	uint64_t offset;
	VEC_FOREACH(offset, next) {
		ulog = ulog_by_offset(offset, p_ops);
		ASSERTne(ulog, NULL);

		capacity += ulog->capacity;
	}

	while (capacity < *new_capacity) {
		if (extend(p_ops->base, &ulog->next) != 0)
			return -1;
		VEC_PUSH_BACK(next, ulog->next);
		ulog = ulog_next(ulog, p_ops);
		ASSERTne(ulog, NULL);

		capacity += ulog->capacity;
	}
	*new_capacity = capacity;

	return 0;
}

/*
 * ulog_checksum -- (internal) calculates ulog checksum
 */
static int
ulog_checksum(struct ulog *ulog, size_t ulog_base_bytes, int insert)
{
	return util_checksum(ulog, SIZEOF_ULOG(ulog_base_bytes),
		&ulog->checksum, insert, 0);
}

/*
 * ulog_store -- stores the transient src ulog in the
 *	persistent dest ulog
 *
 * The source and destination ulogs must be cacheline aligned.
 */
void
ulog_store(struct ulog *dest, struct ulog *src, size_t nbytes,
	size_t ulog_base_nbytes, struct ulog_next *next,
	const struct pmem_ops *p_ops)
{
	/*
	 * First, store all entries over the base capacity of the ulog in
	 * the next logs.
	 * Because the checksum is only in the first part, we don't have to
	 * worry about failsafety here.
	 */
	struct ulog *ulog = dest;
	size_t offset = ulog_base_nbytes;

	/*
	 * Copy at least 8 bytes more than needed. If the user always
	 * properly uses entry creation functions, this will zero-out the
	 * potential leftovers of the previous log. Since all we really need
	 * to zero is the offset, sizeof(struct redo_log_entry_base) is enough.
	 * If the nbytes is aligned, an entire cacheline needs to be addtionally
	 * zeroed.
	 * But the checksum must be calculated based solely on actual data.
	 */
	size_t checksum_nbytes = MIN(ulog_base_nbytes, nbytes);
	nbytes = CACHELINE_ALIGN(nbytes + sizeof(struct ulog_entry_base));

	size_t base_nbytes = MIN(ulog_base_nbytes, nbytes);
	size_t next_nbytes = nbytes - base_nbytes;

	size_t nlog = 0;

	while (next_nbytes > 0) {
		ulog = ulog_by_offset(VEC_ARR(next)[nlog++], p_ops);
		ASSERTne(ulog, NULL);

		size_t copy_nbytes = MIN(next_nbytes, ulog->capacity);
		next_nbytes -= copy_nbytes;

		ASSERT(IS_CACHELINE_ALIGNED(ulog->data));

		VALGRIND_ADD_TO_TX(ulog->data, copy_nbytes);
		pmemops_memcpy(p_ops,
			ulog->data,
			src->data + offset,
			copy_nbytes,
			PMEMOBJ_F_MEM_WC |
			PMEMOBJ_F_MEM_NODRAIN |
			PMEMOBJ_F_RELAXED);
#ifdef PANGOLIN_LOGREP
		struct ulog *ulogrep = pangolin_repaddr(p_ops->base, ulog);
		pmemops_memcpy(p_ops,
			ulogrep->data,
			src->data + offset,
			copy_nbytes,
			PMEMOBJ_F_MEM_WC |
			PMEMOBJ_F_MEM_NODRAIN |
			PMEMOBJ_F_RELAXED);
#endif
		VALGRIND_REMOVE_FROM_TX(ulog->data, copy_nbytes);
		offset += copy_nbytes;
	}

	if (nlog != 0)
		pmemops_drain(p_ops);

	/*
	 * Then, calculate the checksum and store the first part of the
	 * ulog.
	 */
	src->next = VEC_SIZE(next) == 0 ? 0 : VEC_FRONT(next);
	ulog_checksum(src, checksum_nbytes, 1);

#ifdef PANGOLIN_LOGREP
	void *destrep = pangolin_repaddr(p_ops->base, dest);
	pmemops_memcpy(p_ops, destrep, src,
		SIZEOF_ULOG(base_nbytes),
		PMEMOBJ_F_MEM_WC |
		PMEMOBJ_F_MEM_NODRAIN |
		PMEMOBJ_F_RELAXED);
#endif
	pmemops_memcpy(p_ops, dest, src,
		SIZEOF_ULOG(base_nbytes),
		PMEMOBJ_F_MEM_WC);
}

/*
 * ulog_entry_val_create -- creates a new log value entry in the ulog
 *
 * This function requires at least a cacheline of space to be available in the
 * ulog.
 */
struct ulog_entry_val *
ulog_entry_val_create(struct ulog *ulog, size_t offset, uint64_t *dest,
	uint64_t value, ulog_operation_type type,
	const struct pmem_ops *p_ops)
{
	struct ulog_entry_val *e =
		(struct ulog_entry_val *)(ulog->data + offset);

	struct {
		struct ulog_entry_val v;
		struct ulog_entry_base zeroes;
	} data;
	COMPILE_ERROR_ON(sizeof(data) != sizeof(data.v) + sizeof(data.zeroes));

	/*
	 * Write a little bit more to the buffer so that the next entry that
	 * resides in the log is erased. This will prevent leftovers from
	 * a previous, clobbered, log from being incorrectly applied.
	 */
	data.zeroes.offset = 0;
	data.v.base.offset = (uint64_t)(dest) - (uint64_t)p_ops->base;
	data.v.base.offset |= ULOG_OPERATION(type);
	data.v.value = value;

#ifdef PANGOLIN_LOGREP
	void *erep = pangolin_repaddr(p_ops->base, e);
	pmemops_memcpy(p_ops, erep, &data, sizeof(data),
		PMEMOBJ_F_MEM_NOFLUSH | PMEMOBJ_F_RELAXED);
#endif

	pmemops_memcpy(p_ops, e, &data, sizeof(data),
		PMEMOBJ_F_MEM_NOFLUSH | PMEMOBJ_F_RELAXED);

	return e;
}

/*
 * ulog_entry_buf_create -- atomically creates a buffer entry in the log
 */
struct ulog_entry_buf *
ulog_entry_buf_create(struct ulog *ulog, size_t offset, uint64_t *dest,
	const void *src, uint64_t size,
	ulog_operation_type type, const struct pmem_ops *p_ops)
{
	struct ulog_entry_buf *e =
		(struct ulog_entry_buf *)(ulog->data + offset);

	/*
	 * Depending on the size of the source buffer, we might need to perform
	 * up to three separate copies:
	 *	1. The first cacheline, 24b of metadata and 40b of data
	 * If there's still data to be logged:
	 *	2. The entire remainder of data data aligned down to cacheline,
	 *	for example, if there's 150b left, this step will copy only
	 *	128b.
	 * Now, we are left with between 0 to 63 bytes. If nonzero:
	 *	3. Create a stack allocated cacheline-sized buffer, fill in the
	 *	remainder of the data, and copy the entire cacheline.
	 *
	 * This is done so that we avoid a cache-miss on misaligned writes.
	 */

	struct ulog_entry_buf *b = alloca(CACHELINE_SIZE);
	b->base.offset = (uint64_t)(dest) - (uint64_t)p_ops->base;
	b->base.offset |= ULOG_OPERATION(type);
	b->size = size;
	b->checksum = 0;

	/*
	 * PGL-OPT: We might be able to avoid copies around alloca() and
	 * last_cacheline[] by leveraging the cache line alignment of an objbuf.
	 */

	size_t bdatasize = CACHELINE_SIZE - sizeof(struct ulog_entry_buf);  // should be 4, a extra trick for cl alignment
	size_t ncopy = MIN(size, bdatasize);
	memcpy(b->data, src, ncopy);
	memset(b->data + ncopy, 0, bdatasize - ncopy);

	size_t remaining_size = ncopy > size ? 0 : size - ncopy;  // size = ncopy + rcopy + lcopy

	char *srcof = (char *)src + ncopy;
	size_t rcopy = ALIGN_DOWN(remaining_size, CACHELINE_SIZE);
	size_t lcopy = remaining_size - rcopy;

	uint8_t last_cacheline[CACHELINE_SIZE];
	if (lcopy != 0) {
		memcpy(last_cacheline, srcof + rcopy, lcopy);
		memset(last_cacheline + lcopy, 0, CACHELINE_SIZE - lcopy);
	}

	if (rcopy != 0) {
		void *dest = e->data + ncopy;
		ASSERT(IS_CACHELINE_ALIGNED(dest));
#ifndef PANGOLIN_LOGFREE
		VALGRIND_ADD_TO_TX(dest, rcopy);
		pmemops_memcpy(p_ops, dest, srcof, rcopy,
			PMEMOBJ_F_MEM_NODRAIN | PMEMOBJ_F_MEM_NONTEMPORAL);
#endif
		VALGRIND_REMOVE_FROM_TX(dest, rcopy);
#ifndef PANGOLIN_LOGFREE
	#ifdef PANGOLIN_LOGREP
		void *destrep = pangolin_repaddr(p_ops->base, dest);
		pmemops_memcpy(p_ops, destrep, srcof, rcopy,
			PMEMOBJ_F_MEM_NODRAIN | PMEMOBJ_F_MEM_NONTEMPORAL);
	#endif
#endif
	}

	if (lcopy != 0) {
		void *dest = e->data + ncopy + rcopy;
		ASSERT(IS_CACHELINE_ALIGNED(dest));

		VALGRIND_ADD_TO_TX(dest, CACHELINE_SIZE);
#ifndef PANGOLIN_LOGFREE
		pmemops_memcpy(p_ops, dest, last_cacheline, CACHELINE_SIZE,
			PMEMOBJ_F_MEM_NODRAIN | PMEMOBJ_F_MEM_NONTEMPORAL);
#endif
		VALGRIND_REMOVE_FROM_TX(dest, CACHELINE_SIZE);
#ifndef PANGOLIN_LOGFREE
	#ifdef PANGOLIN_LOGREP
		void *destrep = pangolin_repaddr(p_ops->base, dest);
		pmemops_memcpy(p_ops, destrep, last_cacheline, CACHELINE_SIZE,
			PMEMOBJ_F_MEM_NODRAIN | PMEMOBJ_F_MEM_NONTEMPORAL);
	#endif
#endif
	}

	b->checksum = util_checksum_seq(b, CACHELINE_SIZE, 0);
	if (rcopy != 0)
		b->checksum = util_checksum_seq(srcof, rcopy, b->checksum);
	if (lcopy != 0)
		b->checksum = util_checksum_seq(last_cacheline,
			CACHELINE_SIZE, b->checksum);

	ASSERT(IS_CACHELINE_ALIGNED(e));

	VALGRIND_ADD_TO_TX(e, CACHELINE_SIZE);
#ifndef PANGOLIN_LOGFREE
	pmemops_memcpy(p_ops, e, b, CACHELINE_SIZE,
		PMEMOBJ_F_MEM_NODRAIN | PMEMOBJ_F_MEM_NONTEMPORAL);
#endif
	VALGRIND_REMOVE_FROM_TX(e, CACHELINE_SIZE);

#ifndef PANGOLIN_LOGFREE
#ifdef PANGOLIN_LOGREP
	void *erep = pangolin_repaddr(p_ops->base, e);
	pmemops_memcpy(p_ops, erep, b, CACHELINE_SIZE,
		PMEMOBJ_F_MEM_NODRAIN | PMEMOBJ_F_MEM_NONTEMPORAL);
#endif
#endif

#ifndef PANGOLIN
	/*
	 * An objbuf transaction does not have to drain (sfence) every log
	 * entry, because it uses redo logging and it is fine to drain after all
	 * log entries are copied to pmem. We call pmemops_drain() after
	 * obuf_tx_log_commit().
	 */
#ifndef PANGOLIN_LOGFREE
	// no log, no fence!
	pmemops_drain(p_ops);
#endif
#endif

	ASSERT(ulog_entry_valid(&e->base));

	return e;
}

/*
 * ulog_entry_apply -- applies modifications of a single ulog entry
 */
void
ulog_entry_apply(const struct ulog_entry_base *e, int persist,
	const struct pmem_ops *p_ops)
{
	ulog_operation_type t = ulog_entry_type(e);
	uint64_t offset = ulog_entry_offset(e);

	size_t dst_size = sizeof(uint64_t);
	uint64_t *dst = (uint64_t *)((uintptr_t)p_ops->base + offset);

	struct ulog_entry_val *ev;
	struct ulog_entry_buf *eb;

	flush_fn f = persist ? p_ops->persist : p_ops->flush;

#ifdef PANGOLIN
	/*
	 * In the current implementation of libpmemobj/pangolin:
	 *
	 * ULOG_OPERATION_AND and ULOG_OPERATION_OR only modify chunk_run's hdr
	 * or bitmap, and parity data should reflect the changes.
	 *
	 * ULOG_OPERATION_SET does not modify chunk_run's bitmap. It could
	 * modify heap metadata, log data, or user data via the following paths,
	 * and it should not affect parity.
	 *
	 *   1. Function huge_prep_operation_hdr(), to modify a zone's
	 *   chunk_headers for huge allocation/free. This is handled by metadata
	 *   replication.
	 *
	 *   2. Function palloc_set_value(). This only affects lane area, for
	 *   invalidating logs. This is handled by metadata replication.
	 *
	 *   3. Function pmalloc_construct() via lane_undo_extend() or
	 *   lane_redo_extend(), to extend allocations for excessive logging.
	 *   ULOG_OPERATION_SET in this case only modifies log data, which do
	 *   not affect parity.
	 *
	 *   4. Functions pmalloc(), prealloc(), pfree() in pmalloc.c. Pangolin
	 *   only uses pfree() for clearing all logs, and ULOG_OPERATION_SET in
	 *   this case only affects log data.
	 *
	 *   5. Function obj_alloc_root() in obj.c for root object. This
	 *   modifies &pop->root_size, handled by metadata replication.
	 *
	 *   6. Functions obj_alloc_construct() and obj_free() in obj.c.
	 *   Pangolin does not use or allow them.
	 *
	 *   7. Functions in list.c. Pangolin doesn't use or allow them.
	 *
	 * ULOG_OPERATION_BUF_CPY will be used for applying user's logs during
	 * crash recovery or libpmemobj's abort (see tx_undo_entry_apply).
	 *
	 * ULOG_OPERATION_BUF_SET is not used in the current implementation.
	 */
#endif

#ifdef PANGOLIN_PARITY
	uint64_t dstval = *dst;
#endif

	switch (t) {
		case ULOG_OPERATION_AND:
			ev = (struct ulog_entry_val *)e;
			VALGRIND_ADD_TO_TX(dst, dst_size);
			*dst &= ev->value;
#ifdef PANGOLIN_PARITY
			pangolin_update_parity(p_ops->base, dst, &dstval,
				sizeof(uint64_t));
#endif
			f(p_ops->base, dst, sizeof(uint64_t),
				PMEMOBJ_F_RELAXED);
		break;
		case ULOG_OPERATION_OR:
			ev = (struct ulog_entry_val *)e;

			VALGRIND_ADD_TO_TX(dst, dst_size);
			*dst |= ev->value;
#ifdef PANGOLIN_PARITY
			pangolin_update_parity(p_ops->base, dst, &dstval,
				sizeof(uint64_t));
#endif
			f(p_ops->base, dst, sizeof(uint64_t),
				PMEMOBJ_F_RELAXED);
		break;
		case ULOG_OPERATION_SET:
			ev = (struct ulog_entry_val *)e;

			VALGRIND_ADD_TO_TX(dst, dst_size);
			*dst = ev->value;
#if defined(PANGOLIN_METAREP) || defined(PANGOLIN_LOGREP)
#ifdef PANGOLIN_CHECKSUM
			if (pangolin_ptoloc(p_ops->base, dst) == PTO_ZONE_META)
				pangolin_update_zone_csum(p_ops->base, -1, dst,
					sizeof(uint64_t));
#endif
			void *dstrep = pangolin_repaddr(p_ops->base, dst);
			pmemops_memcpy(p_ops, dstrep, dst, sizeof(uint64_t),
				PMEMOBJ_F_RELAXED);
			/*
			 * PGL-NOTE: If this is user data, we should handle
			 * parity update. But Pangolin should not have a case
			 * for this to be user data, which must go through
			 * objbuf.
			 */
#endif
			f(p_ops->base, dst, sizeof(uint64_t),
				PMEMOBJ_F_RELAXED);
		break;
		case ULOG_OPERATION_BUF_SET:
			eb = (struct ulog_entry_buf *)e;

			dst_size = eb->size;
			VALGRIND_ADD_TO_TX(dst, dst_size);
			pmemops_memset(p_ops, dst, *eb->data, eb->size,
				PMEMOBJ_F_RELAXED | PMEMOBJ_F_MEM_NODRAIN);
		break;
		case ULOG_OPERATION_BUF_CPY:
			eb = (struct ulog_entry_buf *)e;

			dst_size = eb->size;
			VALGRIND_ADD_TO_TX(dst, dst_size);
			pmemops_memcpy(p_ops, dst, eb->data, eb->size,
				PMEMOBJ_F_RELAXED | PMEMOBJ_F_MEM_NODRAIN);
		break;
		default:
			ASSERT(0);
	}
	VALGRIND_REMOVE_FROM_TX(dst, dst_size);
}

/*
 * ulog_process_entry -- (internal) processes a single ulog entry
 */
static int
ulog_process_entry(struct ulog_entry_base *e, void *arg,
	const struct pmem_ops *p_ops)
{
	ulog_entry_apply(e, 0, p_ops);

	return 0;
}

/*
 * ulog_clobber -- zeroes the metadata of the ulog
 */
void
ulog_clobber(struct ulog *dest, struct ulog_next *next,
	const struct pmem_ops *p_ops)
{
	struct ulog empty;
	memset(&empty, 0, sizeof(empty));

	if (next != NULL)
		empty.next = VEC_SIZE(next) == 0 ? 0 : VEC_FRONT(next);
	else
		empty.next = dest->next;

#ifdef PANGOLIN_LOGREP
	void *destrep = pangolin_repaddr(p_ops->base, dest);
	pmemops_memcpy(p_ops, destrep, &empty, sizeof(empty),
		PMEMOBJ_F_MEM_WC | PMEMOBJ_F_MEM_NODRAIN | PMEMOBJ_F_RELAXED);
#endif
	pmemops_memcpy(p_ops, dest, &empty, sizeof(empty),
		PMEMOBJ_F_MEM_WC);
}

/*
 * ulog_clobber_data -- zeroes out 'nbytes' of data in the logs
 */
void
ulog_clobber_data(struct ulog *dest,
	size_t nbytes, size_t ulog_base_nbytes,
	struct ulog_next *next, ulog_free_fn ulog_free,
	const struct pmem_ops *p_ops)
{
	size_t rcapacity = ulog_base_nbytes;
	size_t nlog = 0;
	ASSERTne(dest, NULL);

	for (struct ulog *r = dest; r != NULL; ) {
		size_t nzero = MIN(nbytes, rcapacity);
		VALGRIND_ADD_TO_TX(r->data, nzero);
#ifndef PANGOLIN_LOGFREE
#ifdef PANGOLIN_LOGREP
		void *datarep = pangolin_repaddr(p_ops->base, r->data);
		pmemops_memset(p_ops, datarep, 0, nzero,
			PMEMOBJ_F_MEM_WC |
			PMEMOBJ_F_MEM_NODRAIN | PMEMOBJ_F_RELAXED);
#endif
		pmemops_memset(p_ops, r->data, 0, nzero, PMEMOBJ_F_MEM_WC);
#endif
		VALGRIND_ADD_TO_TX(r->data, nzero);
		nbytes -= nzero;

		if (nbytes == 0)
			break;

		r = ulog_by_offset(VEC_ARR(next)[nlog++], p_ops);
		if (nlog > 1)
			break;

		ASSERTne(r, NULL);
		rcapacity = r->capacity;
	}

	/*
	 * To make sure that transaction logs do not occupy too much of space,
	 * all of them, expect for the first one, are freed at the end of
	 * the operation. The reasoning for this is that pmalloc() is
	 * a relatively cheap operation for transactions where many hundreds of
	 * kilobytes are being snapshot, and so, allocating and freeing the
	 * buffer for each transaction is an acceptable overhead for the average
	 * case.
	 */
	struct ulog *u = ulog_by_offset(dest->next, p_ops);
	if (u == NULL)
		return;

	VEC(, uint64_t *) logs_past_first;
	VEC_INIT(&logs_past_first);

	size_t next_offset;
	while (u != NULL && ((next_offset = u->next) != 0)) {
		if (VEC_PUSH_BACK(&logs_past_first, &u->next) != 0) {
			/* this is fine, it will just use more pmem */
			LOG(1, "unable to free transaction logs memory");
			goto out;
		}
		u = ulog_by_offset(u->next, p_ops);
	}

	uint64_t *ulog_ptr;
	VEC_FOREACH_REVERSE(ulog_ptr, &logs_past_first) {
		ulog_free(p_ops->base, ulog_ptr);
	}

out:
	VEC_DELETE(&logs_past_first);
}

/*
 * ulog_process -- process ulog entries
 */
void
ulog_process(struct ulog *ulog, ulog_check_offset_fn check,
	const struct pmem_ops *p_ops)
{
	LOG(15, "ulog %p", ulog);

#ifdef DEBUG
	if (check)
		ulog_check(ulog, check, p_ops);
#endif

	ulog_foreach_entry(ulog, ulog_process_entry, NULL, p_ops);
}

/*
 * ulog_base_nbytes -- (internal) counts the actual of number of bytes
 *	occupied by the ulog
 */
size_t
ulog_base_nbytes(struct ulog *ulog)
{
	size_t offset = 0;
	struct ulog_entry_base *e;

	for (offset = 0; offset < ulog->capacity; ) {
		e = (struct ulog_entry_base *)(ulog->data + offset);
		if (!ulog_entry_valid(e))
			break;

		offset += ulog_entry_size(e);
	}

	return offset;
}

/*
 * ulog_recovery_needed -- checks if the logs needs recovery
 */
int
ulog_recovery_needed(struct ulog *ulog, int verify_checksum)
{
	size_t nbytes = MIN(ulog_base_nbytes(ulog), ulog->capacity);
	if (nbytes == 0)
		return 0;

	if (verify_checksum && !ulog_checksum(ulog, nbytes, 0))
		return 0;

	return 1;
}

/*
 * ulog_recover -- recovery of ulog
 *
 * The ulog_recover shall be preceded by ulog_check call.
 */
void
ulog_recover(struct ulog *ulog, ulog_check_offset_fn check,
	const struct pmem_ops *p_ops)
{
	LOG(15, "ulog %p", ulog);

	if (ulog_recovery_needed(ulog, 1)) {
		ulog_process(ulog, check, p_ops);
		ulog_clobber(ulog, NULL, p_ops);
	}
}

/*
 * ulog_check_entry --
 *	(internal) checks consistency of a single ulog entry
 */
static int
ulog_check_entry(struct ulog_entry_base *e,
	void *arg, const struct pmem_ops *p_ops)
{
	uint64_t offset = ulog_entry_offset(e);
	ulog_check_offset_fn check = arg;

	if (!check(p_ops->base, offset)) {
		LOG(15, "ulog %p invalid offset %" PRIu64,
				e, e->offset);
		return -1;
	}

	return offset == 0 ? -1 : 0;
}

/*
 * ulog_check -- (internal) check consistency of ulog entries
 */
int
ulog_check(struct ulog *ulog, ulog_check_offset_fn check,
	const struct pmem_ops *p_ops)
{
	LOG(15, "ulog %p", ulog);

	return ulog_foreach_entry(ulog,
			ulog_check_entry, check, p_ops);
}
