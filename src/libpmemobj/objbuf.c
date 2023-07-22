/*
 * Copyright 2017-2018, University of California, San Diego
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
 * objbuf.c -- object buffer implementation
 */

#include <sys/mman.h>
#include <immintrin.h>
#include "mmap.h"
#include "pangolin.h"
#include "cuckoo.h"
#include "objbuf_tx.h"
#include "libpangolin.h"

/*
 * ort -- objbuf runtime information
 */
static struct obuf_runtime *ort;

/*
 * obuf_find_shared_slot -- find the index slot to search for an objbuf
 */
static struct obuf_shared_slot *
obuf_find_shared_slot(void *pobj)
{
	/*
	 * PGL-OPT: This is based on MurmurHash3 64-bit finalizer.
	 * https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
	 *
	 * It works a bit faster than modulo a prime number and seems to provide
	 * a bit less collisions. It's worth trying different hash functions for
	 * lower latency or less collisions. Perhaps some SIMD implementation
	 * can help with performance.
	 */
	uint64_t hash = (uint64_t)pobj;
	hash ^= hash >> 33;
	hash *= 0xff51afd7ed558ccd;
	hash ^= hash >> 33;
	hash *= 0xc4ceb9fe1a85ec53;
	hash ^= hash >> 33;

	uint32_t slotid = (uint32_t)(hash & SHARED_SLOT_MASK);
	struct obuf_shared_slot *slot = &ort->shared[slotid];

	return slot;
}

/*
 * obuf_lock_shared_slot -- locks a shared objbuf index slot
 */
static inline void
obuf_lock_shared_slot(struct obuf_shared_slot *slot)
{
	/* can try | __ATOMIC_HLE_ACQUIRE but it was worse last time tested */
	while (__atomic_exchange_n(&slot->lock, 1, __ATOMIC_ACQUIRE))
		_mm_pause();
}

/*
 * obuf_unlock_shared_slot -- unlocks a shared objbuf index slot
 */
static inline void
obuf_unlock_shared_slot(struct obuf_shared_slot *slot)
{
	/* can try | __ATOMIC_HLE_RELEASE but it was worse last time tested */
	__atomic_clear(&slot->lock, __ATOMIC_RELEASE);
}

/*
 * obuf_fill_objbuf -- fill data of an objbuf
 */
static int
obuf_fill_objbuf(struct objbuf *obuf, void *pobj, size_t size, int copy_obj)
{
	/* copy object data or just its header to objbuf */
	void *uobj = obuf->uobj; /* point to the start of the objbuf's object */
	memcpy(REAL(uobj), REAL(pobj), size);

#ifdef PANGOLIN_CHECKSUM
	// temporary disable verification
	// if (copy_obj) {
	// 	size_t user_size = USIZE(obuf->uobj);
	// 	uint32_t csum = pangolin_adler32(CSUM0, obuf->uobj, user_size);
	// 	if (obuf->csum != csum) {
	// 		uint64_t oidoff = OBJ_PTR_TO_OFF(ort->pop, pobj);
	// 		LOG_ERR("object checksum error - oid.off 0x%lx, "
	// 			"stored checksum 0x%x, computed checksum 0x%x",
	// 			oidoff, obuf->csum, csum);

	// 		// TODO: may fail here when logfree on skiplist and rtree
	// 		if (!pangolin_repair_corruption(REAL(pobj), size,
	// 			0, obuf)) {
	// 			FATAL("cannot repair object corruption");
	// 		} else {
	// 			LOG_ERR("checksum error repaired at %p offset "
	// 				"0x%lx size %zu!", pobj, oidoff, size);
	// 		}
	// 	}
	// }
#endif

	return 0;
}

/*
 * obuf_validate_actions -- check if the input is a valid obuf_action
 */
static int
obuf_validate_actions(enum obuf_action oact)
{
	/* ignore bits undefined in enum obuf_action */
#ifdef DEBUG
	if (oact == OBUF_ACT_EINVAL) {
		LOG_ERR("invalid objbuf action");
		return EINVAL;
	}

	int count = 0;
	/* counted actions are mutually exclusive */
	count = ((oact & OBUF_ACT_NEWOBJ) > 0) +
		((oact & OBUF_ACT_CPYOBJ) > 0);
	if (count > 1) {
		LOG_ERR("received conflicting actions 0x%08x", oact);
		return EINVAL;
	}
#endif

	return 0;
}

/*
 * obuf_insert_shared -- insert an objbuf into a shared slot
 */
struct objbuf *
obuf_insert_shared(void *pobj, struct objbuf *obuf)
{
	if (OBUF_IS_SHARED(obuf))
		return obuf;

	struct obuf_shared_slot *slot = obuf_find_shared_slot(pobj);
	/*
	 * PGL-TODO: In multi-thread applications, more than one thread may try
	 * to create a shared objbuf for the same PMEMoid. If one thread
	 * succeeds, others should use the already-created objbuf. Now we do not
	 * have this use case. If so, let the insert function return the
	 * existing objbuf.
	 */
	obuf_lock_shared_slot(slot);

	uint64_t oidoff = OBJ_PTR_TO_OFF(ort->pop, pobj);
	int err = cuckoo_insert(slot->kvs, oidoff, obuf);
	ASSERTeq(err, 0);

#ifdef PROBE
	if (err == 0) {
		slot->inserts += 1;
		slot->objbufs += 1;
	}
#endif

	obuf->state = OBUF_SET_SHARED(obuf);

	obuf_unlock_shared_slot(slot);

	return obuf;
}

// TODO: actually can be simplified, because the indexing of delta does not need two-stage
#define DELTAP_SIZE 1024*1024*1024ULL+1024*1024*512ULL
static char* deltaP_buffer_e = NULL;  // ending addr for delta
static char* deltaP_buffer_p = NULL;
void
init_deltaP_buffer()
{
	deltaP_buffer_p = Malloc(DELTAP_SIZE);
	deltaP_buffer_e = deltaP_buffer_p + DELTAP_SIZE;
}
// malloc a addr pointer
void *
malloc_from_deltaP_buffer(size_t size)
{
	if (deltaP_buffer_p + size >= deltaP_buffer_e) {
		deltaP_buffer_p = deltaP_buffer_e - DELTAP_SIZE;
		printf("deltaP buffer overflow\n");  // pointer cannot be reused
		exit(-6657);
	}
	void* ret = deltaP_buffer_p;
	deltaP_buffer_p += size;
	return ret;
}
///////////////////////////////////////////
#define DELTA_BUFFER_SIZE 1024*1024*512ULL
static char* delta_buffer_e = NULL;
static char* delta_buffer_p = NULL;

void
init_delta_buffer()
{
	delta_buffer_p = Malloc(DELTA_BUFFER_SIZE);
	delta_buffer_e = delta_buffer_p + DELTA_BUFFER_SIZE;
}

// void *
// malloc_from_delta_buffer(size_t size)
// {
// 	if (delta_buffer_p + size >= delta_buffer_e) delta_buffer_p = delta_buffer_e;
// 	void* ret = delta_buffer_p;
// 	delta_buffer_p += size;
// 	return ret;
// }

// reusing delta buffer
void *
malloc_aligned_from_delta_buffer(size_t size)
{
	if (delta_buffer_p + size >= delta_buffer_e) {
		delta_buffer_p = delta_buffer_e - DELTA_BUFFER_SIZE;  // buffer usage is allower to re-use
	}
	size_t disp = CACHELINE_SIZE - (((uint64_t) delta_buffer_p) & CLMASK);
	delta_buffer_p += disp;
	void* ret = delta_buffer_p;
	delta_buffer_p += size;
	return ret;
}

/*
 * obuf_create_objbuf -- create an objbuf
 *
 * Pointer pobj must point to the start of a pmem object.
 */
struct objbuf *
obuf_create_objbuf(PMEMobjpool *pop, void *pobj, struct obuf_pool *obp,
	enum obuf_action oact)
{
	ASSERT(ALIGNED_CL(OHDR(pobj)));

	if (obuf_validate_actions(oact) != 0)
		FATAL("%s: illegal objbuf actions", __func__);

	/* data_size: sizeof(objbuf incl. uobj) - OBUF_META_SIZE */
	size_t data_size = OHDR(pobj)->size & ALLOC_HDR_FLAGS_MASK;
	ASSERT(ALIGNED_CL(data_size));

	struct objbuf *obuf;
	size_t obuf_size = OBUF_META_SIZE + data_size;
#ifdef DEBUG
	/*
	 * In DEBUG mode, we always use Malloc() to allocate objbuf so that
	 * Asan can detect buffer overflows.
	 */
	int inpool = 0;
#else
	int inpool = obp && (obp->size + obuf_size <= OBUF_POOL_CAP);
#endif
	
	inpool = obp && (obp->size + obuf_size <= OBUF_POOL_CAP);
	
	size_t disp = 0; /* displacement for cache-line alignment */

	if (inpool) {  // use obufpool
		obuf = (struct objbuf *)(obp->base + obp->size);
		ASSERT(ALIGNED_CL(obuf));
		obp->size += obuf_size;
	} else {
		/*
		 * make objbufs cache-line aligned
		 *
		 * Note: the latency of Malloc() can be a critical part for the
		 * overall performance of using objbufs. Therefore, we highly
		 * prefer a low-latency, high-concurrent implementation.
		 * Malloc() by default uses glibc's malloc(). Alternatives are
		 * worth tring, such as jemalloc, tcmalloc, or rpmalloc etc.
		 *
		 * Do not use util_aligned_malloc() or posix_memalign()
		 * because they show bad latency numbers on our test machines.
		 */
		obuf = Malloc(obuf_size + CACHELINE_SIZE);
		ASSERTne(obuf, NULL);
		if (!ALIGNED_CL(obuf)) {
			uint64_t obufbase = (uint64_t)obuf;
			disp = CACHELINE_SIZE - (obufbase & CLMASK);
			obuf = (struct objbuf *)(obufbase + disp);
		}
		ASSERT(ALIGNED_CL(obuf));
#ifdef DEBUG
		uint64_t oidoff = OBJ_PTR_TO_OFF(pop, pobj);
		LOG_BLU(1, "Malloc objbuf %p for oidoff 0x%lx", obuf, oidoff);
#endif
	}

	/* clear objbuf content depending on oact */
	size_t zeros = (oact & OBUF_ACT_CPYOBJ) ? OBUF_META_SIZE : obuf_size;
	memset(obuf, 0, zeros);

	// TODO: thread delta buffer alloc
#ifdef PANGOLIN_PIPELINE_DATA
	if (obuf->obj_delta == NULL) {
		// assign them delta buffer (TODO: not thread-safe!!!)
		// char *tmp = Malloc(data_size * 2 * sizeof(char));
		// char *tmp = malloc_aligned_from_delta_buffer(data_size * 2 * sizeof(char));
		// char *tmp = delta_buffer_p;  // fix delta buffer addr
		obuf->obj_delta = delta_buffer_p;

		// obuf->obj_delta = malloc_aligned_from_delta_buffer(sizeof(uint64_t *) * 2);
		// obuf->obj_delta = malloc_from_deltaP_buffer(sizeof(uint64_t *) * 2);
		// obuf->obj_delta[1] = (uint64_t *) (tmp + data_size);

		// fix all addresses. avoid logical overflow, but may have worse locality? but save some pointers
		// obuf->obj_delta = deltaP_buffer_p;
		// obuf->obj_delta[0] = (uint64_t *) tmp;
		// obuf->obj_delta[1] = (uint64_t *) (tmp + 1024*1024*1024ULL);

		// fix buffer address, but ptr address will change. may have better locality? but save some pointers
		// obuf->obj_delta = malloc_from_deltaP_buffer(sizeof(uint64_t *) * 2);
		// obuf->obj_delta[0] = (uint64_t *) tmp;
		// obuf->obj_delta[1] = (uint64_t *) (tmp + data_size);
	}
#endif

	/*
	 * data size to be copied from pmem
	 *
	 * PGL-OPT: There are unused fields in objbuf's metadata, and probably
	 * we can use them to store the object header, so we can set fill_size
	 * to 0 for actions except OBUF_ACT_CPYOBJ.
	 *
	 * PGL-TODO: OBUF_ACT_NEWOBJ may not need to copy the header if function
	 * alloc_prep_block() can write_header() to objbuf.
	 */
	int copy_obj = oact & OBUF_ACT_CPYOBJ;
	size_t fill_size = copy_obj ? data_size : OHDR_SIZE;

	obuf_fill_objbuf(obuf, pobj, fill_size, copy_obj);

	/* set objbuf metadata */
	obuf->canary = OBUF_CANARY;
	ASSERT(disp < CACHELINE_SIZE);
	obuf->disp = (uint16_t)disp;
	obuf->txid = 0;
	obuf->pop = pop;
	obuf->pobj = pobj;

	/* set objbuf data state; OBUF_CPYOBJ and OBUF_NEWOBJ are exclusive */
	obuf->state = OBUF_CPYOBJ; /* OBUF_ACT_CPYOBJ */
	obuf->state = (oact & OBUF_ACT_NEWOBJ) ? OBUF_NEWOBJ : obuf->state;

	obuf->state = (inpool) ? OBUF_SET_INPOOL(obuf) : obuf->state;

	return obuf;
}

/*
 * obuf_free_objbuf -- free an objbuf occupied memory
 */
void
obuf_free_objbuf(struct objbuf *obuf)
{	
#ifdef PANGOLIN_PIPELINE_DATA
	// free delta
	// Free(obuf->obj_delta[0]);
	// Free(obuf->obj_delta);
#endif

	if (OBUF_IS_INPOOL(obuf))
		return;

	void *obufbase = (char *)obuf - obuf->disp;

	Free(obufbase);

#ifdef PROBE
	uint64_t oidoff = OBJ_PTR_TO_OFF(obuf->pop, obuf->pobj);
	LOG_BLU(1, "free objbuf %p for oidoff 0x%lx", obuf, oidoff);
#endif
}

/*
 * cuckoo_free_objbuf -- cuckoo hashmap's call-back function
 */
static void
cuckoo_free_objbuf(uint64_t key, void *value)
{
	uint64_t oidoff = key;
	struct objbuf *obuf = (struct objbuf *)value;

	ASSERTeq(obuf->txid, 0);
	ASSERT(OBJ_OFF_IS_VALID(obuf->pop, oidoff));

	obuf_free_objbuf(obuf);
}

/*
 * obuf_find_shared -- find a shared objbuf
 */
struct objbuf *
obuf_find_shared(void *pobj)
{
	/*
	 * Generally, a forged oid pointing to an arbitrary heap location does
	 * not work with libpangolin, because that can not tell the object size.
	 * Now libpangolin only opens oids that point to the start of objects.
	 *
	 * Most pmemobj applications use pmemobj_direct(oid) when oid points to
	 * the beginning of a pmem object. However it's also possible to forge a
	 * valid oid that points to the middle of a pmem object.
	 */
	struct obuf_shared_slot *slot = obuf_find_shared_slot(pobj);

	obuf_lock_shared_slot(slot);

	uint64_t oidoff = OBJ_PTR_TO_OFF(ort->pop, pobj);
	struct objbuf *obuf = cuckoo_get(slot->kvs, oidoff);

	ASSERT(((obuf == NULL) || OBUF_IS_SHARED(obuf)));

	obuf_unlock_shared_slot(slot);

	return obuf;
}

/*
 * pgl_open -- open a pmem object by its PMEMoid
 *
 * User is responsible for checking if this object is already openned or not.
 */
void *
pgl_open(PMEMobjpool *pop, PMEMoid oid)
{
	void *pobj = (char *)ort->pop + oid.off;
	struct objbuf *obuf = obuf_create_objbuf(ort->pop, pobj, NULL,
			OBUF_ACT_CPYOBJ);

	return obuf->uobj;
}

/*
 * pgl_open_shared -- open a pmem object by its PMEMoid in shared mode
 */
void *
pgl_open_shared(PMEMoid oid)
{
	if (OBJ_OID_IS_NULL(oid))
		return NULL;

	/* PGL-TODO: Need to use a map for multiple pools. */
	PMEMobjpool *pop = ort->pop;
	void *pobj = (char *)pop + oid.off;

	struct objbuf *obuf = obuf_find_shared(pobj);

	/*
	 * Shared objbuf is created on open (unlike thread-local ones). This is
	 * for handling objbuf easily, i.e. there is no need to redirect a
	 * pointer from pmem to dram.
	 *
	 * This approach may degrade performance for read-only workloads due to
	 * the objbuf creation and memcpy, and consume extra DRAM resources.
	 * But the penalty only becomes significant when the workload opens a
	 * lot of shared objbufs.
	 */
	if (obuf == NULL) {
		obuf = obuf_create_objbuf(pop, pobj, NULL, OBUF_ACT_CPYOBJ);
		ASSERTne(obuf, NULL);
		obuf_insert_shared(pobj, obuf);
	}

	if (!OBUF_IS_SHARED(obuf))
		FATAL("%s: objbuf is not in shared state", __func__);

	if (!OBUF_IS_CPYOBJ(obuf) && !OBUF_IS_NEWOBJ(obuf))
		FATAL("%s: objbuf state error", __func__);

	return obuf->uobj;
}

/*
 * pgl_close -- close an objbuf instance without commit
 */
int
pgl_close(void *uobj)
{
	if (uobj == NULL)
		return 0;

	struct objbuf *obuf = OBUF(uobj);
	ASSERT(!OBUF_IS_SHARED(obuf));

	if (obuf->txid > 0) {
		LOG_ERR("attempt to close an object of a transaction");
		return EINVAL;
	}

	obuf_free_objbuf(obuf);

	return 0;
}

/*
 * pgl_close_shared -- close a shared objbuf instance
 *
 * Caller of this function should avoid "double-free" because this function
 * cannot determine if the objbuf enclosing uobj is already freed.
 */
int
pgl_close_shared(PMEMobjpool *pop, void *uobj)
{
	if (uobj == NULL || OBJ_PTR_FROM_POOL(pop, uobj))
		return 0;

	struct objbuf *obuf = OBUF(uobj);
	ASSERT(OBUF_IS_SHARED(obuf));

	if (obuf->txid > 0) {
		LOG_ERR("attempt to close an object of a transaction");
		return EINVAL;
	}

	void *pobj = obuf->pobj;
	struct obuf_shared_slot *slot = obuf_find_shared_slot(pobj);

	obuf_lock_shared_slot(slot);

	uint64_t oidoff = OBJ_PTR_TO_OFF(obuf->pop, pobj);
	void *obufret = cuckoo_remove(slot->kvs, oidoff);
	if (obufret != obuf) {
		LOG_ERR("removed objbuf %p != expected %p", obufret, obuf);
		return EINVAL;
	}

#ifdef PROBE
	if (slot->objbufs == 0)
		LOG_ERR("objbuf deleting error: about to delete shared objbuf "
			"%p, objbuf->pobj %p; counter slot->objbufs has "
			"already reached zero", obuf, obuf->pobj);
	slot->removes += 1;
	slot->objbufs -= 1;
#endif

	obuf_free_objbuf(obuf);

	obuf_unlock_shared_slot(slot);

	return 0;
}

/*
 * pgl_close_all_shared -- close all shared objbufs
 *
 * This is only for testing, not thread-safe.
 */
void
pgl_close_all_shared(void)
{
	for (size_t i = 0; i < SHARED_NSLOTS; i++) {
		struct obuf_shared_slot *slot = &ort->shared[i];
		cuckoo_foreach_cb(slot->kvs, cuckoo_free_objbuf, 1);
	}
}


/*
 * pgl_oid -- get the given object's PMEMoid
 */
PMEMoid
pgl_oid(void *uobj)
{
	if (uobj == NULL)
		return OID_NULL;

	/*
	 * PGL-OPT: For now this is only called for an object in transaction,
	 * i.e. uobj in objbuf. Calling this function can be fatal if uobj
	 * points to pmem.
	 */

	void *pobj = OBUF(uobj)->pobj;
	PMEMobjpool *pop = OBUF(uobj)->pop;

	if (! OBJ_PTR_FROM_POOL(pop, pobj))
		return OID_NULL;

	PMEMoid oid = {pop->uuid_lo, OBJ_PTR_TO_OFF(pop, pobj)};

	return oid;
}

/*
 * obuf_create_index -- create objbuf index slots
 */
static int
obuf_create_index(struct obuf_runtime *ort)
{
	/* index for shared (between all open pools) objbufs */
	ort->shared =
		Zalloc(SHARED_NSLOTS * sizeof(struct obuf_shared_slot));
	ASSERTne(ort->shared, NULL);

	for (size_t i = 0; i < SHARED_NSLOTS; i++) {
		struct obuf_shared_slot *slot = &ort->shared[i];
		slot->kvs = cuckoo_new();
		ASSERTne(slot->kvs, NULL);
	}

	ort->tlocal =
		Zalloc(TLOCAL_NSLOTS * sizeof(struct obuf_tlocal_slot));
	ASSERTne(ort->tlocal, NULL);

	for (size_t i = 0; i < TLOCAL_NSLOTS; i++) {
		struct obuf_tlocal_slot *slot = &ort->tlocal[i];
		slot->kvs = cuckoo_new();
		ASSERTne(slot->kvs, NULL);
	}

	return 0;
}

/*
 * obuf_destroy_index -- destroy objbuf lookup table
 */
static int
obuf_destroy_index(struct obuf_runtime *ort)
{
	for (size_t i = 0; i < SHARED_NSLOTS; i++) {
		struct obuf_shared_slot *slot = &ort->shared[i];
		cuckoo_foreach_cb(slot->kvs, cuckoo_free_objbuf, 1);
		cuckoo_delete(slot->kvs);
	}
	Free(ort->shared);

	for (size_t i = 0; i < TLOCAL_NSLOTS; i++) {
		struct obuf_tlocal_slot *slot = &ort->tlocal[i];
		cuckoo_foreach_cb(slot->kvs, cuckoo_free_objbuf, 1);
		cuckoo_delete(slot->kvs);
	}
	Free(ort->tlocal);

	return 0;
}

/*
 * obuf_create_obufpool -- provision thread-local objbuf pool
 */
static int
obuf_create_obufpool(struct obuf_runtime *ort)
{
	for (size_t i = 0; i < TLOCAL_NSLOTS; i++) {
		struct obuf_tlocal_slot *slot = &ort->tlocal[i];
		struct obuf_pool *obp = &slot->obp;

		obp->base = util_map(-1, OBUF_POOL_CAP,
			MAP_SHARED | MAP_ANONYMOUS, 0, Pagesize, NULL);

		/* PGL-OPT: Should unmap already-mapped on failue. */
		if (obp->base == MAP_FAILED)
			FATAL("%s: create objbuf pool %zu failed", __func__, i);

		ASSERT(ALIGNED_4K(obp->base));

		obp->size = 0;
	}

	return 0;
}

/*
 * obuf_destroy_obufpool -- destroy thread-local objbuf pool
 */
static int
obuf_destroy_obufpool(struct obuf_runtime *ort)
{
	for (size_t i = 0; i < TLOCAL_NSLOTS; i++) {
		struct obuf_tlocal_slot *slot = &ort->tlocal[i];
		struct obuf_pool *obp = &slot->obp;
		int err = util_unmap(obp->base, OBUF_POOL_CAP);
		ASSERTeq(err, 0);
	}

	return 0;
}

/*
 * obuf_get_runtime -- get objbuf runtime data structure
 */
struct obuf_runtime *
obuf_get_runtime(void)
{
	return ort;
}

/*
 * obuf_create_runtime -- create objbuf runtime data structure **for Pangolin**
 */
int
obuf_create_runtime(PMEMobjpool *pop)
{
	COMPILE_ERROR_ON(PAGESIZE != Pagesize);
	COMPILE_ERROR_ON(!ALIGNED_4K(OBUF_POOL_CAP));
	COMPILE_ERROR_ON(TLOCAL_NSLOTS != OBJ_NLANES);
	COMPILE_ERROR_ON((1 << CLSHIFT) != CACHELINE_SIZE);
	COMPILE_ERROR_ON(OBUF_META_SIZE != CACHELINE_SIZE);
	COMPILE_ERROR_ON(OHDR_SIZE != ALLOC_HDR_COMPACT_SIZE);
	COMPILE_ERROR_ON((SHARED_NSLOTS & SHARED_SLOT_MASK) != 0);

	ASSERTeq(ort, NULL);
	init_delta_buffer();
	init_deltaP_buffer();
	/*
	 * PGL-TODO: This only supports a single pool or poolset. Need to
	 * implement an array or hashmap for multiple pools, indexed by
	 * pop->uuid_lo.
	 */
	ort = Zalloc(sizeof(struct obuf_runtime));  // one obuf_runtime binds one PMEMobjpool *pop
	ASSERTne(ort, NULL);

	ort->pop = pop;

	uint32_t nzones = 1;
	/* compute the number of zones, starting with the size of all zones */
	uint64_t zone_size = pop->heap_size - sizeof(struct heap_header);
	while (zone_size >= ZONE_MAX_SIZE) {
		nzones += 1;
		zone_size -= ZONE_MAX_SIZE;
	}
	ort->nzones = nzones;

#ifdef PANGOLIN_PARITY
	/* one chunk array will be parity (default: 9+1) */
	uint32_t zone_rows = 10;
	uint32_t last_zone_rows = 10;
	uint32_t parity_rows = 1;

	char *env_zone_rows = os_getenv("PANGOLIN_ZONE_ROWS");  // the num of rows in one zone
	char *env_zone_parity = os_getenv("PANGOLIN_ZONE_ROWS_PARITY");  // the num of parity rows in one zone
	
	if (env_zone_rows) {
		int64_t rows = strtol(env_zone_rows, NULL, 0);
		if (rows < 4 || rows > 1000) {
			Free(ort);
			FATAL("invalid PANGOLIN_ZONE_ROWS %ld!", rows);
		}
		/* PGL-TODO: should save this setting in pmem */
		zone_rows = (uint32_t)rows;
		last_zone_rows = (uint32_t)rows;
	}
	if (env_zone_parity) {
		int64_t rows = strtol(env_zone_parity, NULL, 0);
		if (rows < 1 || rows > 2) {  // TODO: only support p=1,2
			Free(ort);
			FATAL("invalid PANGOLIN_ZONE_ROWS_PARITY %ld!", rows);
		}
		parity_rows = (uint32_t)rows;
	}
	init_ec_runtime(zone_rows-parity_rows, parity_rows, 4096);  // init isa-l ec

	/* first compute parameters for a max-sized zone */
	uint64_t zone_chunks = MAX_CHUNK;
	/* #chunks of an chunk array, remainder is wasted */
	uint64_t zone_row_chunks = zone_chunks / zone_rows;
	/* #bytes of an chunk array */
	uint64_t zone_row_size = zone_row_chunks * CHUNKSIZE;  // track by offset directly
	ort->zone_rows = zone_rows;
	ort->zone_rows_parity = parity_rows;
	ort->zone_row_size = zone_row_size;

	/* parity offset into the zone */
	if (parity_rows == 1) {  // only 1 xor parity
		ort->zone_parity = sizeof(struct zone) + (zone_rows - 1) * zone_row_size;
		ort->zone_parity_1 = 0;
	} else {  // 2 parity RS code
		ort->zone_parity = sizeof(struct zone) + (zone_rows - 2) * zone_row_size;
		ort->zone_parity_1 = sizeof(struct zone) + (zone_rows - 1) * zone_row_size;
	}

	/* initialize zone's range-column locks */
#ifdef PANGOLIN_RCLOCKHLE
	ort->zone_rclock = Zalloc(nzones * sizeof(int *));
#else
	ort->zone_rclock = Zalloc(nzones * sizeof(os_rwlock_t *));
#endif

	/* initialize full zone's parity locks */
	uint32_t zid = 0;
	uint64_t zone_rclocks = zone_row_chunks * (CHUNKSIZE / RCLOCKSIZE);
	for (zid = 0; zid < nzones - 1; zid++) {
		ort->zone_rclock[zid] =
			Zalloc(zone_rclocks * sizeof(os_rwlock_t));
#ifndef PANGOLIN_RCLOCKHLE
		for (uint64_t rclock = 0; rclock < zone_rclocks; rclock++)
			os_rwlock_init(&ort->zone_rclock[zid][rclock]);
#endif
	}

	zone_chunks = (zone_size - sizeof(struct zone)) / CHUNKSIZE;
	zone_row_chunks = zone_chunks / last_zone_rows;  // the last row is shorter, but coding param unchanged
	zone_row_size = zone_row_chunks * CHUNKSIZE;

	ort->last_zone_id = nzones - 1;
	ort->last_zone_rows = last_zone_rows;
	ort->last_zone_rows_parity = parity_rows;
	ort->last_zone_row_size = zone_row_size;

	// parity addr in the last zone (same as normal filled zone)
	if (parity_rows == 1) {
		ort->last_zone_parity = sizeof(struct zone) + (last_zone_rows - 1) * ort->last_zone_row_size;
		ort->last_zone_parity_1 = 0;
	} else {
		ort->last_zone_parity = sizeof(struct zone) + (last_zone_rows - 2) * ort->last_zone_row_size;
		ort->last_zone_parity_1 = sizeof(struct zone) + (last_zone_rows - 1) * ort->last_zone_row_size;
	}

	zone_rclocks = zone_row_chunks * (CHUNKSIZE / RCLOCKSIZE);
	ort->zone_rclock[zid] = Zalloc(zone_rclocks * sizeof(os_rwlock_t));

#ifndef PANGOLIN_RCLOCKHLE
	for (uint64_t rclock = 0; rclock < zone_rclocks; rclock++)
		os_rwlock_init(&ort->zone_rclock[zid][rclock]);
#endif


	// ort->rclock_threshold = 512;  // a trade off by pangolin default (8KB in paper)
#ifdef PANGOLIN_FORCE_ATOMIC_XOR
	ort->rclock_threshold = 8192;  // a super big threshold to force atomic xor
#else
	ort->rclock_threshold = 16;  // a super small threshold to force SIMD xor
#endif

	
	char *env_rclock_threshold = os_getenv("PANGOLIN_RCLOCK_THRESHOLD");
	if (env_rclock_threshold) {
		size_t rcth = strtoul(env_rclock_threshold, NULL, 0);
		if (rcth < CACHELINE_SIZE || rcth > RCLOCKSIZE) {
			Free(ort);
			FATAL("invalid PANGOLIN_RCLOCK_THRESHOLD %ld", rcth);
		}
		ort->rclock_threshold = rcth;
	}

#endif /* PANGOLIN_PARITY */

	obuf_create_index(ort);
	obuf_create_obufpool(ort);

	ort->get_verify_csum = 0;

#ifdef PANGOLIN_CHECKSUM
	char *env_get_verify_csum = os_getenv("PANGOLIN_GET_VERIFY_CSUM");
	if (env_get_verify_csum) {
		if (strtoul(env_get_verify_csum, NULL, 0) > 0)
			ort->get_verify_csum = 1;
	}

	if (pangolin_setup_scrub(pop, ort) != 0) {
		obuf_destroy_runtime(pop);
		FATAL("pangolin_setup_scrub() failed");
	}
#endif

	ort->freeze = 0;
	ort->actv_txs = 0;
	ort->past_txs = 0;

#ifdef PANGOLIN_PARITY
	pangolin_register_sighdl();
#endif

#ifdef PROBE
	ort->new_objs = 0;
	ort->new_size = 0;
	ort->del_objs = 0;
	ort->del_size = 0;
	ort->mod_objs = 0;
	ort->mod_size = 0;
	ort->vul_size = 0;
	ort->get_size = 0;
	ort->get_size_max = 0;
#endif

	return 0;
}

/*
 * pangolin_update_stats -- temporary function
 */
void
pangolin_update_stats(void)
{
#ifdef PROBE
	if (ort->get_size > ort->get_size_max)
		ort->get_size_max = ort->get_size;
#endif
}

/*
 * pgl_print_stats -- print statistics collected by probes
 */
void
pgl_print_stats(void)
{
	/* PGL-OPT: This only prints statistics for shared objbufs. */
#ifdef PROBE
	size_t inserts = 0, removes = 0, objbufs = 0;
	for (uint32_t i = 0; i < SHARED_NSLOTS; i++) {
		struct obuf_shared_slot *slot = &ort->shared[i];
		inserts += slot->inserts;
		removes += slot->removes;
		objbufs += slot->objbufs;
	}

	/*
	 * A non-zero objbufs indicates some shared objbufs exist but they are
	 * not closed by user. These are memory leaks and could impact the
	 * application's performance.
	 */

	LOG_BLU(0, "total shared objbuf inserts: %lu, removes: %lu",
		inserts, removes);
	if (objbufs > 0)
		LOG_YLW(0, "active shared objbufs: %lu", objbufs);
	else
		LOG_BLU(0, "active shared objbufs: %lu", objbufs);

	uint64_t past_txs = ort->past_txs;
	uint64_t new_objs = ort->new_objs;
	uint64_t new_size = ort->new_size;
	uint64_t del_objs = ort->del_objs;
	uint64_t del_size = ort->del_size;
	uint64_t mod_objs = ort->mod_objs;
	uint64_t mod_size = ort->mod_size;
	uint64_t vul_size = ort->vul_size;
	uint64_t get_size = ort->get_size;

	LOG_BLU(0, "zone row size: %lu, range column locks: %lu",
		ort->zone_row_size, ort->zone_row_size / RCLOCKSIZE);

	if (past_txs == 0)
		return;

	float avg_new_objs = (float)new_objs / (float)past_txs;
	float avg_new_size = (float)new_size / (float)past_txs;
	float avg_del_objs = (float)del_objs / (float)past_txs;
	float avg_del_size = (float)del_size / (float)past_txs;
	float avg_mod_objs = (float)mod_objs / (float)past_txs;
	float avg_mod_size = (float)mod_size / (float)past_txs;

	float vul_size_kb = (float)vul_size / 1024;
	float vul_size_mb = vul_size_kb / 1024;
	float vul_size_gb = vul_size_mb / 1024;

	float get_size_kb = (float)get_size / 1024;
	float get_size_mb = get_size_kb / 1024;
	float get_size_gb = get_size_mb / 1024;

	if (get_size > ort->get_size_max)
		ort->get_size_max = get_size;
	uint64_t get_size_max = ort->get_size_max;

	float get_size_max_kb = (float)get_size_max / 1024;
	float get_size_max_mb = get_size_max_kb / 1024;
	float get_size_max_gb = get_size_max_mb / 1024;

	LOG_BLU(0, "past transactions: %zu", past_txs);
	LOG_BLU(0, "avg. allocated size: %8.2f, objs: %8.2f", avg_new_size,
		avg_new_objs);
	LOG_BLU(0, "avg. modified  size: %8.2f, objs: %8.2f", avg_mod_size,
		avg_mod_objs);
	LOG_BLU(0, "avg. dealloca. size: %8.2f, objs: %8.2f", avg_del_size,
		avg_del_objs);
	LOG_BLU(0, "total vul size: %10zu, %6.2f KB, %6.2f MB, %6.2f GB",
		vul_size, vul_size_kb, vul_size_mb, vul_size_gb);
	LOG_BLU(0, "total get size: %10zu, %6.2f KB, %6.2f MB, %6.2f GB",
		get_size, get_size_kb, get_size_mb, get_size_gb);
	LOG_BLU(0, "max get size:   %10zu, %6.2f KB, %6.2f MB, %6.2f GB",
		get_size_max, get_size_max_kb, get_size_max_mb,
		get_size_max_gb);
#endif
}

/*
 * pgl_reset_stats -- reset pangolin statistics
 */
void
pgl_reset_stats(void)
{
	/* PGL-OPT: This only resets statistics for shared objbufs. */
#ifdef PROBE
	for (uint32_t i = 0; i < SHARED_NSLOTS; i++) {
		struct obuf_shared_slot *slot = &ort->shared[i];
		slot->inserts = 0;
		slot->removes = 0;
		/* object counts should not reset */
	}

	ort->new_objs = 0;
	ort->new_size = 0;
	ort->del_objs = 0;
	ort->del_size = 0;
	ort->mod_objs = 0;
	ort->mod_size = 0;
	ort->vul_size = 0;
	ort->get_size = 0;
#endif
}

/*
 * obuf_destroy_runtime -- destroy objbuf runtime data structures
 */
int
obuf_destroy_runtime(PMEMobjpool *pop)
{
	ASSERTne(ort, NULL);

	obuf_destroy_obufpool(ort);
	obuf_destroy_index(ort);

#ifdef PANGOLIN_PARITY
	for (uint32_t zid = 0; zid <= ort->last_zone_id; zid++) {
#ifndef PANGOLIN_RCLOCKHLE
		uint64_t row_size;
		if (zid == ort->last_zone_id)
			row_size = ort->last_zone_row_size;
		else
			row_size = ort->zone_row_size;

		uint32_t rclocks = (uint32_t)(row_size >> RCLOCKSHIFT);

		for (uint32_t rclock = 0; rclock < rclocks; rclock++)
			os_rwlock_destroy(&ort->zone_rclock[zid][rclock]);
#endif

		Free(ort->zone_rclock[zid]);
	}

	Free(ort->zone_rclock);
#endif

	if (ort->scrub_tid_valid) {
		pthread_cancel(ort->scrub_tid);
		pthread_join(ort->scrub_tid, NULL);
		close(ort->scrub_tfd);
		close(ort->scrub_efd);
	}

	Free(ort);
	ort = NULL;

	LOG(3, "objbuf runtime destroyed");

	return 0;
}

void
timer_get(struct timespec *time)
{
	os_clock_gettime(CLOCK_MONOTONIC, time);
}

void
timer_diff(struct timespec *d, struct timespec *t1, struct timespec *t2)
{
	long long nsecs = (t2->tv_sec - t1->tv_sec) * NSECPSEC + t2->tv_nsec -
		t1->tv_nsec;
	d->tv_sec = nsecs / NSECPSEC;
	d->tv_nsec = nsecs % NSECPSEC;
}

double
timer_get_secs(struct timespec *t)
{
	return (double)t->tv_sec + (double)t->tv_nsec / NSECPSEC;
}

long
timer_get_nsecs(struct timespec *t)
{
	long ret = t->tv_nsec;
	ret += t->tv_sec * NSECPSEC;

	return ret;
}

#ifdef DEBUG
struct objbuf *
obuf(void *uobj)
{
	return OBUF(uobj);
}

size_t
memdiff(char *src1, char *src2, size_t size)
{
	size_t diffs = 0;
	for (size_t i = 0; i < size; i++) {
		if (src1[i] != src2[i]) {
			if (diffs < 5)
				LOG_ERR("Diff at %zu: 0x%x - 0x%x", i,
					src1[i], src2[i]);
			diffs += 1;
		}
	}
	LOG_ERR("Found %zu different bytes between two sources", diffs);

	return diffs;
}
#endif
