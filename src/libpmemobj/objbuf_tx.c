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
 * objbuf_tx.c -- objbuf transaction functions
 */
#include <stddef.h>
#include "cuckoo.h"
#include "pangolin.h"
#include "objbuf_tx.h"
#include "libpangolin.h"

#ifdef PANGOLIN_PIPELINE_DATA_2M
#include "pangolin_ec.h"
#endif

#define LARGE_SIZE_THRESHOLD (1024 * 1024)
#define MAX_ACTVOBJS 2048 // 32 is enough for all cases except for hashmap_tx rebuilding QAQ

extern int
xor_gen_sse_pgl(int vects, size_t len, void **array);
extern int
xor_gen_avx512_pgl(int vects, size_t len, void **array);

/*
 * dram_delta := pm_dst + dram_src & pm_dst := dram_src
 * must be called by swap_delta_avx512(3, len, {pm_dst, dram_src, dram_delta})
*/ 
extern int
swap_delta_avx512(int vects, size_t len, void **array);

/*
 * This data structure has similar purpose as tx_range_def, but the offset value
 * is relative to the object it belongs to.
 */
struct obj_range {
	struct objbuf *obuf;
	uint64_t offset;
	uint64_t size;
/*	uint64_t flags; */
};

/*
 * obuf_get_tx -- returns current objbuf transaction struct
 *
 * Refer to tx.c/get_tx() for implementation.
 */
struct obuf_tx *
obuf_get_tx(void)
{
	static __thread struct obuf_tx otx;
	return &otx;
}

/*
 * pgl_tx_stage -- return the current thread's transaction stage
 */
enum pgl_tx_stage
pgl_tx_stage(void)
{
	return obuf_get_tx()->stage;
}

/*
 * pgl_tx_process -- processes current pangolin transaction stage
 */
void
pgl_tx_process(void)
{
	struct obuf_tx *otx = obuf_get_tx();

	ASSERT(otx->txid > 0);

	switch (otx->stage) {
	case PANGOLIN_TX_NONE:
		break;
	case PANGOLIN_TX_WORK:
		pgl_tx_commit();
		break;
	case PANGOLIN_TX_ONABORT:
		LOG_ERR("transaction abort not implemented");
	case PANGOLIN_TX_POSTCOMMIT:
		otx->stage = PANGOLIN_TX_FINALLY;
		break;
	case PANGOLIN_TX_FINALLY:
		otx->stage = PANGOLIN_TX_NONE;
		break;
	default:
		ASSERT(0);
	}
}

/*
 * obuf_tx_action_add -- reserve space and add a new obuf tx action
 */
static struct pobj_action *
obuf_tx_action_add(struct obuf_tx *otx)
{
	size_t entries_size = (VEC_SIZE(&otx->actions) + 1) *
		sizeof(struct ulog_entry_val);
	if (operation_reserve(otx->lane->external, entries_size) != 0)
		return NULL;

	VEC_INC_BACK(&otx->actions);

	struct pobj_action *action = &VEC_BACK(&otx->actions);

	return action;
}

/*
 * obuf_tx_abort -- abort a transaction
 */
static void
obuf_tx_abort(const char *err_msg)
{
	if (err_msg) {
		LOG_ERR("%s", err_msg);
	}

	struct obuf_tx *otx = obuf_get_tx();

	otx->obp->size = 0;
	/* Clear the memory actions. */
	VEC_CLEAR(&otx->actions);
}

/*
 * pgl_tx_begin -- start a new pangolin transaction
 */
int
pgl_tx_begin(PMEMobjpool *pop)
{
	struct obuf_tx *otx = obuf_get_tx();

	if (otx->stage == PANGOLIN_TX_WORK) {
		ASSERTne(otx->lane, NULL);

		otx->level += 1;
		if (otx->pop != pop)
			obuf_tx_abort("nested transaction for different pool");

		return 0;
	}

	if (otx->stage != PANGOLIN_TX_NONE)
		obuf_tx_abort("starting a new transaction at a wrong stage");

	struct obuf_runtime *ort = obuf_get_runtime();

#ifdef PANGOLIN_PARITY
	/*
	 * Wait for the pool-wide freeze flag to clear.
	 * PGL-TODO: perhaps better let this thread sleep
	 */
	while (__atomic_load_n(&ort->freeze, __ATOMIC_RELAXED));

	__atomic_add_fetch(&ort->actv_txs, 1, __ATOMIC_RELAXED);

	/* In case the freeze flag is set after we increment actv_txs... */
	if (__atomic_load_n(&ort->freeze, __ATOMIC_RELAXED)) {
		/* a recovery or scrubbing thread is pending */
		otx->frozen = 1; /* thread-local variable */
		__atomic_sub_fetch(&ort->actv_txs, 1, __ATOMIC_RELAXED);
		return 1;
	}
#endif

	uint32_t laneid = lane_hold(pop, &otx->lane);
	operation_start(otx->lane->undo);
	VEC_INIT(&otx->actions);

	otx->pop = pop;
	otx->actvobjs = 0;
	otx->frozen = 0;
	/* laneid starts from 0; valid txid starts from 1 */
	otx->txid = laneid + 1;
	otx->level = 0;
	otx->ort = ort;
	otx->ost = &otx->ort->tlocal[laneid];
	otx->obp = &otx->ost->obp;

	SLIST_INIT(&otx->txobjs);
	otx->stage = PANGOLIN_TX_WORK;
	otx->first_snapshot = 1;

	return 0;
}

/*
 * pgl_commit -- commit a single object
 */
int
pgl_commit(void *uobj)
{
	struct obuf_tx *otx = obuf_get_tx();
	/* TODO: We may actually allow this as a nested transaction. */
	if (otx->stage != PANGOLIN_TX_NONE)
		obuf_tx_abort("should not call pgl_commit in a transaction");

	struct objbuf *obuf = OBUF(uobj);

	/* TODO: Check return values and handle aborts. */
	pgl_tx_begin(obuf->pop);
	pgl_tx_add(uobj);
	pgl_tx_commit();
	return pgl_tx_end();
}

/*
 * obuf_tx_log_range -- redo-log a (modified) range of an object
 *
 * offset relative to REAL(obuf->uobj)
 *
 * PGL-OPT: Make the offset and size in terms of cache lines.
 * Redo logging for obuf data during update.
 */
#ifndef PANGOLIN_LOGFREE
static int
obuf_tx_log_range(struct objbuf *obuf, uint64_t offset, size_t size)
{
	ASSERTne(obuf, NULL);

	struct obuf_tx *otx = obuf_get_tx();

	if (size > PMEMOBJ_MAX_ALLOC_SIZE) {
		LOG_ERR("size to log too large");
		return EINVAL;
	}

	/*
	 * PGL-OPT: In pmemobj_tx_add_common() it checks for overlapping ranges.
	 * Pangolin does not need to do so because objbufs hold changes. But we
	 * may perform range-combining for efficient write-backs.
	 */

	/*
	 * Pangolin uses lane->layout->undo with redo semantics. If we are
	 * creating the first redo-log entry, setup an internal redo log action
	 * to validate the entire redo log chain when the internal redo log is
	 * processed by palloc_publish in pgl_tx_commit.
	 */
	if (otx->first_snapshot) {
#ifdef PANGOLIN
		/* This action is in DRAM before palloc_publish processes it. */
		struct pobj_action *action = obuf_tx_action_add(otx);
		if (action == NULL)
			return -1;

		struct ulog *loghead = (struct ulog *)&otx->lane->layout->undo;
		palloc_set_value(&otx->pop->heap, action, &loghead->replay, 1);
#endif

		otx->first_snapshot = 0;
	}

	/*
	 * Depending on the size of the block, either allocate an
	 * entire new object or use cache.
	 */

	void *src = (char *)REAL(obuf->uobj) + offset;
	void *dest = (char *)REAL(obuf->pobj) + offset;

	int ret = operation_add_buffer(otx->lane->undo, dest, src, size,
		ULOG_OPERATION_BUF_CPY);

#ifdef PROBE
	otx->ort->mod_size += size;
	/*
	 * PGL-TODO: Note the "size" here is allocated size, not user-defined
	 * object size. The minimum allocation size is 128 B. For objects
	 * smaller than that, adding 128 to mod_size may not be appropriate.
	 *
	 * Data structure  |  node size   | allocation size
	 * ctree           |   56         |  128
	 * rbtree          |   80         |  128
	 * btree           |  304         |  320
	 * skiplist        |  408         |  448
	 * rtree           | 4128 or 4136 | 4160
	 */
#endif

	if (ret != 0) {
		LOG_ERR("failed to create range redo log");
		return EINVAL;
	}

	return ret;
}
#endif

/*
 * pgl_tx_add_common -- add an object to transaction
 */
static void *
pgl_tx_add_common(void *uobj, int shared)
{
	if (uobj == NULL)
		return NULL;

	struct obuf_tx *otx = obuf_get_tx();
	ASSERTne(otx->txid, 0);

	struct objbuf *obuf = OBUF(uobj);

	if (OBJ_PTR_FROM_POOL(otx->pop, uobj)) {
		uint64_t oidoff = OBJ_PTR_TO_OFF(otx->pop, uobj);
		obuf = cuckoo_get(otx->ost->kvs, oidoff);
		if (obuf == NULL) {
			struct obuf_pool *obp = shared ? NULL : otx->obp;
			obuf = obuf_create_objbuf(otx->pop, uobj, obp,
				OBUF_ACT_CPYOBJ);
			if (obuf == NULL)
				obuf_tx_abort("create objbuf failed");
#ifdef PANGOLIN_PIPELINE_DATA
			if (obuf->obj_delta == NULL) obuf_tx_abort("objbuf obj_delta is empty");
#endif
		}
	}

	if (obuf->txid == otx->txid) {
#ifdef DEBUG
		uint64_t oidoff = OBJ_PTR_TO_OFF(obuf->pop, obuf->pobj);
		struct objbuf *exist = cuckoo_get(otx->ost->kvs, oidoff);
		if (shared)
			/* obuf must NOT be in this thread-local index */
			ASSERTeq(exist, NULL);
		else
			/* obuf must be in this thread-local index */
			ASSERTeq(exist, obuf);
#endif
		goto out;
	}

	if (obuf->txid == 0)
		obuf->txid = otx->txid;

	if (obuf->txid != otx->txid)
		obuf_tx_abort("objbuf already in another transaction");

	if (shared) {
		obuf_insert_shared(obuf->pobj, obuf);
	} else {
#ifdef PROBE
		otx->ort->vul_size += USIZE(obuf->uobj);
#endif
		uint64_t oidoff = OBJ_PTR_TO_OFF(obuf->pop, obuf->pobj);
		int err = cuckoo_insert(otx->ost->kvs, oidoff, obuf);
		if (err != 0)
			obuf_tx_abort("objbuf insertion error");
	}

#ifdef PROBE
	otx->ort->mod_objs += 1;
#endif

	/*
	 * Insert to head, and later perform commits from head. The object order
	 * should not matter. If the order matters, store a pointer to the tail
	 * object and SLIST_INSERT_AFTER. Or use TAILQ to insert at tail, but
	 * that requires two pointers in an objbuf's header.
	 */
	SLIST_INSERT_HEAD(&otx->txobjs, obuf, txobj);

	otx->actvobjs += 1;

out:
	return obuf->uobj;
}

/*
 * pgl_tx_alloc_common -- allocate and optionally open a pmem object
 *
 * The allocation will not be published before the transaction commits. If
 * retptr is not NULL, its return value holds a pointer for the allocated and
 * zero-filled object in objbuf. Otherwise, the allocated object is not opened
 * and its header will be written to pmem on commit.
 */
static PMEMoid
pgl_tx_alloc_common(size_t size, uint64_t type_num, uintptr_t *retptr,
	int shared)
{
	if (size == 0 || size > PMEMOBJ_MAX_ALLOC_SIZE)
		obuf_tx_abort("allocation size error");

	struct obuf_tx *otx = obuf_get_tx();
	if (otx->stage != PANGOLIN_TX_WORK)
		obuf_tx_abort("pgl_tx_alloc_common w/o a transaction\n");

	PMEMobjpool *pop = otx->pop;

	struct pobj_action *action = obuf_tx_action_add(otx);
	if (action == NULL)
		obuf_tx_abort("obuf_tx_action_add() failed\n");

	/*
	 * PGL-TODO: palloc_reserve() writes object allocation header to pmem
	 * but it does not modify allocation metadata. Be aware of this pmem
	 * change for abort and recovery processes.
	 */
	// a known BUG of PMDK: `palloc` cannot guarantee object alignment! 16 B offset
	if (palloc_reserve(&pop->heap, size, NULL, NULL, type_num, 0,
		CLASS_ID_FROM_FLAG(0ULL), action) != 0) {
		LOG_ERR("palloc_reserve() failed");
		goto abort;
	}

	PMEMoid retoid = OID_NULL;
	retoid.off = action->heap.offset;
	retoid.pool_uuid_lo = pop->uuid_lo;

	/* setup objbuf */
	void *pobj = (char *)pop + retoid.off;
	ASSERTeq(pobj, pmemobj_direct(retoid));

	/*
	 * PGL-TODO: It still creates an objbuf even if retptr is not specified.
	 * Perhaps we do not need to do this if the user doesn't intend to open
	 * the allocated pmem object.
	 */
	struct obuf_pool *obp = shared ? NULL : otx->obp;
	struct objbuf *obuf = obuf_create_objbuf(otx->pop, pobj, obp,
		OBUF_ACT_NEWOBJ);
	ASSERTeq(obuf->csum, CSUM0);
	ASSERT(OBUF_IS_NEWOBJ(obuf));

	if (pgl_tx_add_common(obuf->uobj, shared) == NULL) {
		LOG_ERR("pgl_tx_add_common() failed");
		goto abort;
	}

	if (retptr != NULL)
		*retptr = (uintptr_t)obuf->uobj;

#ifdef PROBE
	otx->ort->new_objs += 1;
	otx->ort->new_size += size;
#endif

	return retoid;

abort:
	VEC_POP_BACK(&otx->actions);
	LOG_ERR("object allocation failed");

	return OID_NULL;
}

/*
 * pgl_tx_alloc -- allocate a zero-filled object as part of a transaction
 */
PMEMoid
pgl_tx_alloc(size_t size, uint64_t type_num)
{
	/*
	 * PGL-TODO: It still creates an objbuf in OBUF_NEWOBJ state.
	 * Perhaps we should not open it if user does not intend to.
	 */
	// uintptr_t * addr = NULL;
	return pgl_tx_alloc_common(size, type_num, NULL, 0);
}

/*
 * pgl_tx_alloc_open -- allocate and open a thread-local object
 */
void *
pgl_tx_alloc_open(size_t size, uint64_t type_num)
{
	uintptr_t uobj = 0;

	pgl_tx_alloc_common(size, type_num, &uobj, 0);

	return (void *)uobj;
}

/*
 * pgl_tx_alloc_open_shared -- allocate and open a shared object
 */
void *
pgl_tx_alloc_open_shared(size_t size, uint64_t type_num)
{
	uintptr_t uobj = 0;

	pgl_tx_alloc_common(size, type_num, &uobj, 1);

	return (void *)uobj;
}

/*
 * pgl_tx_free -- frees an existing object
 */
int
pgl_tx_free(PMEMoid oid)
{
	if (OBJ_OID_IS_NULL(oid))
		return 0;

	struct obuf_tx *otx = obuf_get_tx();
	PMEMobjpool *pop = otx->pop;

	if (pop->uuid_lo != oid.pool_uuid_lo)
		obuf_tx_abort("invalid pool uuid");

	ASSERT(OBJ_OID_IS_VALID(pop, oid));

	/* remove its objbuf if opened */
	void *pobj = (char *)pop + oid.off;
	ASSERTeq(pobj, pmemobj_direct(oid));

#ifdef PROBE
	otx->ort->del_objs += 1;
	otx->ort->del_size += palloc_usable_size(&pop->heap, oid.off);
#endif

	struct pobj_action *action;
	struct objbuf *obuf = cuckoo_get(otx->ost->kvs, oid.off);

	if (obuf != NULL) { /* a thread-local objbuf */
		ASSERTeq(obuf->txid, otx->txid);
		void *obufret = cuckoo_remove(otx->ost->kvs, oid.off);
		ASSERTeq(obufret, obuf);
	} else {
		obuf = obuf_find_shared(pobj);
		if (obuf != NULL) /* a shared objbuf */
			ASSERT((obuf->txid == 0) || (obuf->txid == otx->txid));
	}

	if (obuf != NULL && obuf->txid == otx->txid) {
		/*
		 * Mark the objbuf to be freed at commit, but does not remove it
		 * from the objbuf list now. If its space matters, we may need
		 * to remove and free it. But that may require a doubly linked
		 * list for efficient removal.
		 */
		obuf->state = OBUF_SET_TXFREE(obuf);

		VEC_FOREACH_BY_PTR(action, &otx->actions) {
			if (action->type == POBJ_ACTION_TYPE_HEAP &&
			    action->heap.offset == oid.off) {
				palloc_cancel(&pop->heap, action, 1);
				VEC_ERASE_BY_PTR(&otx->actions, action);
				/*
				 * Getting here means this object is a new
				 * allocation of this transaction and it is not
				 * persistent yet. Thus, we do not need to free
				 * any persistent object.
				 */
				return 0;
			}
		}
	}

	action = obuf_tx_action_add(otx);
	if (action == NULL)
		obuf_tx_abort("obuf_tx_action_add");

	palloc_defer_free(&pop->heap, oid.off, action);

	return 0;
}

/*
 * pgl_tx_open_common -- open an object and add it to a transaction
 */
static void *
pgl_tx_open_common(PMEMoid oid, int shared)
{
	if (OBJ_OID_IS_NULL(oid))
		return NULL;

	struct obuf_tx *otx = obuf_get_tx();

	if (otx->txid == 0)
		obuf_tx_abort("no active transaction for openning an object\n");

	ASSERTeq(oid.pool_uuid_lo, otx->pop->uuid_lo);
	void *uobj = (char *)otx->pop + oid.off;

	uobj = pgl_tx_add_common(uobj, shared);
	if (uobj == NULL)
		obuf_tx_abort("pgl_tx_add_common");

	ASSERTne(OBUF(uobj)->txid, 0);

	return uobj;
}

/*
 * pgl_tx_open -- open a thread-local object and add it to a transaction
 */
void *
pgl_tx_open(PMEMoid oid)
{
	return pgl_tx_open_common(oid, 0);
}

/*
 * pgl_tx_open_shared -- open a shared object and add it to a transaction
 */
void *
pgl_tx_open_shared(PMEMoid oid)
{
	return pgl_tx_open_common(oid, 1);
}

/*
 * pgl_get -- get a pmem object by its PMEMoid in thread-local mode
 *
 * This function is context sensitive. If the calling thread is in the middle of
 * a transaction, it searches for the thread's local buffer hashmap aimming to
 * find an already-open objbuf for the PMEMoid. If the calling thread is not in
 * a transaction, or it is in a transaction but no open objbuf is found, this
 * function returns a pointer to the pmem object.
 *
 * The purpose of this function is to avoid data copy for read-only workloads.
 */
void *
pgl_get(PMEMoid oid)
{
	if (OBJ_OID_IS_NULL(oid))
		return NULL;

	struct obuf_tx *otx = obuf_get_tx();

	if (otx->ost != NULL) {
		struct objbuf *obuf = cuckoo_get(otx->ost->kvs, oid.off);
		if (obuf != NULL)
			return obuf->uobj;
	}

	struct obuf_runtime *ort = obuf_get_runtime();
	void *uobj = (char *)ort->pop + oid.off;

#ifdef PROBE
	ort->vul_size += USIZE(uobj);
	ort->get_size += USIZE(uobj);
#endif

#ifdef PANGOLIN_CHECKSUM
	if (obuf_get_runtime()->get_verify_csum) {
		size_t user_size = USIZE(uobj);
		size_t size = user_size + OHDR_SIZE; // size to repair
		uint64_t oidoff = OBJ_PTR_TO_OFF(ort->pop, uobj);
		uint32_t csum = pangolin_adler32(CSUM0, uobj, user_size);
		if (OHDR(uobj)->csum != csum) {
			LOG_ERR("object checksum error - oid.off 0x%lx, "
				"stored checksum 0x%x, computed checksum 0x%x",
				oidoff, OHDR(uobj)->csum, csum);
			if (!pangolin_repair_corruption(REAL(uobj), size,
				0, NULL)) {
				FATAL("cannot repair object corruption");
			} else {
				LOG_ERR("checksum error repaired at %p offset "
					"0x%lx size %zu!", uobj, oidoff, size);
			}
		}
	}
#endif

	ASSERTne(uobj, NULL);

	return uobj;
}

/*
 * pgl_tx_add -- add a thread-local object to transaction
 */
void *
pgl_tx_add(void *uobj)
{
	return pgl_tx_add_common(uobj, 0);
}

/*
 * pgl_tx_add_shared -- add a shared object to transaction
 */
void *
pgl_tx_add_shared(void *uobj)
{
	return pgl_tx_add_common(uobj, 1);
}

/*
 * pgl_tx_add_range_common -- add a range of an object to transaction
 */
static void *
pgl_tx_add_range_common(void *uobj, uint64_t hoff, size_t size, int shared)
{
	if (uobj == NULL)
		return NULL;

	uobj = pgl_tx_add_common(uobj, shared);
	if (uobj == NULL)
		obuf_tx_abort("adding object to transaction failed");

	struct objbuf *obuf = OBUF(uobj);

	if (OBUF_IS_NEWOBJ(obuf) || OBUF_IS_ADDALL(obuf))
		return uobj;

	size_t user_size = USIZE(uobj);
	uint64_t start = hoff, end = hoff + size - 1;
	if (start >= user_size || end >= user_size)
		obuf_tx_abort("adding invalid object range to transaction");

	if (start > UINT32_MAX || end > UINT32_MAX) { /* obj size > 4GB */
		/* PGL-OPT: not really an error, just to note this case */
		LOG(1, "adding a very large object or range to transaction");

		/* PGL-OPT: for such a large range now just add all to tx */
		obuf->state = OBUF_SET_ADDALL(obuf);
		return uobj;
	}

	/* range offset relative to REAL(uobj) */
	uint64_t s8 = (start + OHDR_SIZE) >> 3;
	uint64_t e8 = (end + OHDR_SIZE) >> 3;
	uint64_t s32 = s8 >> 2;
	uint64_t e32 = e8 >> 2;

	int range_set = 0;
	/* PGL-OPT: Need to consider overlapping ranges. */
	if (s8 < UINT8_MAX && e8 < UINT8_MAX) {
		int free_slot = OBUF_SRANGES;
		for (int i = 0; i < OBUF_SRANGES; i++) {
			uint8_t srs = obuf->srange[2 * i];
			uint8_t sre = obuf->srange[2 * i + 1];
			if (s8 + 1 == srs && e8 + 1 == sre) {
				/* already noted */
				range_set = 1;
				break;
			}
			if (srs == 0 && sre == 0)
				free_slot = i;
		}
		if (!range_set && free_slot < OBUF_SRANGES) {
			/* valid 8-byte range starts from 1 */
			obuf->srange[2 * free_slot] = (uint8_t)(s8 + 1);
			obuf->srange[2 * free_slot + 1] = (uint8_t)(e8 + 1);
			range_set = 1;
		}
	} else if (s32 < UINT32_MAX && e32 < UINT32_MAX) {
		if (obuf->lranges == 0) {
			/* valid 32-byte range starts from 1 */
			obuf->lrange[0] = (uint32_t)(s32 + 1);
			obuf->lrange[1] = (uint32_t)(e32 + 1);
			range_set = 1;
		}
	} else {
		/* PGL-OPT: not an error; need to improve */
		LOG(1, "range not fit: start %lu, end %lu, end - start = %lu",
			start, end, end - start);
	}

	if (!range_set) { /* range store exhaused; add whole object */
		/* PGL-OPT: not an error; need to improve */
		LOG(1, "range not set: start %lu, end %lu, "
			"sranges 0x%016lx, lranges = 0x%016lx",
			start, end, obuf->sranges, obuf->lranges);
		obuf->sranges = 0;
		obuf->lranges = 0;
		/* set all range to prevent later reusing of ranges */
		obuf->state = OBUF_SET_ADDALL(obuf);
	}

	return uobj;
}

/*
 * pgl_tx_add_range -- add a range of a thread-local object to transaction
 */
void *
pgl_tx_add_range(void *uobj, uint64_t hoff, size_t size)
{
	return pgl_tx_add_range_common(uobj, hoff, size, 0);
}

/*
 * pgl_tx_add_range_shared -- add a range of a shared object to transaction
 */
void *
pgl_tx_add_range_shared(void *uobj, uint64_t hoff, size_t size)
{
	return pgl_tx_add_range_common(uobj, hoff, size, 1);
}

/*
 * obuf_heap_obj_update -- update a whole pmem object (like write a full new object)
 */
static int
obuf_heap_obj_update(PMEMobjpool *pop, struct objbuf *obuf)
{
	/*
	 * This real_size is the allocated memory block size for the user's
	 * object. libpmemobj does not store the user's requested object size.
	 */
	uint64_t *src = REAL(obuf->uobj);
	uint64_t *dst = REAL(obuf->pobj);
	/* size contains compact object header */
	size_t size = obuf->size & ALLOC_HDR_FLAGS_MASK;

	// 64B aligned!
	ASSERT(ALIGNED_CL(src));
	ASSERT(ALIGNED_CL(dst));
	ASSERT(ALIGNED_CL(size));

	if (size > LARGE_SIZE_THRESHOLD)
		/* not an error, but this case needs optimizations */
		LOG(1, "updating large size %lu", size);

#ifdef PANGOLIN_CHECKSUM
	/* could include the object header and its offset in the checksum */
	obuf->csum = pangolin_adler32(CSUM0, obuf->uobj, size - OHDR_SIZE);  // update checksum!
#endif

#ifdef PANGOLIN_PARITY
	int err = pangolin_update_parity(pop, dst, src, size);  // update parity!
	if (err != 0) {
		LOG_ERR("pangolin parity update failed");
		return EINVAL;
	}
#endif

#ifdef PANGOLIN_LOGFREE
#ifndef PANGOLIN_LOGFREE_NOFENCE
	pmemops_drain(&pop->p_ops); // as a memory fence
#endif
#endif

	pmemops_memcpy(&pop->p_ops, dst, src, size,
		PMEMOBJ_F_MEM_WC | PMEMOBJ_F_MEM_NODRAIN);  // NT(WC)+NODRAIN

	return 0;
}

#ifdef PANGOLIN_CHECKSUM
/*
 * obuf_update_checksum -- update an objbuf's checksum with a modified range
 *
 * off: byte offset, relative to REAL(obuf->uobj), of a modified range
 * size: size of the modified range
 */
static void
obuf_update_checksum(struct objbuf *obuf, uint64_t off, uint64_t size)
{
	if (OBUF_IS_CSDONE(obuf))
		return;
	/*
	 * Adjust off and size if they belong to the first cache line, because
	 * the object header is not invloved in checksum calculation. Value of
	 * off and size are cache line-aligned so off == 0 means it covers the
	 * first cache line.
	 */
	off = (off == 0) ? 0 : off - OHDR_SIZE;
	size = (off == 0) ? size - (OHDR_SIZE - off) : size;

	/*
	 * Updating an existing checksum with range data can be slower than
	 * re-calculating the new checksum using the whole data range, if the
	 * value of (dlen - rlen) is less than a threshold.
	 */
	uint64_t user_size = USIZE(obuf->uobj);
	if (user_size - size < 1024) {
		obuf->csum = pangolin_adler32(CSUM0, obuf->uobj, user_size);
		obuf->state = OBUF_SET_CSDONE(obuf);
	} else {
		obuf->csum = pangolin_adler32_patch(obuf->csum, obuf->uobj,
			USIZE(obuf->uobj), off, (char *)obuf->pobj + off, size);
	}
}
#endif

// #ifndef PANGOLIN_LOGFREE
/*
 * obuf_heap_range_update -- update a range of a pmem object
 *
 * off is relative to the start of REAL(obuf->uobj).
 *
 * PGL-OPT: Possibly merge code with obuf_heap_obj_update(). 
 *
 */
static int
obuf_heap_range_update(PMEMobjpool *pop, struct objbuf *obuf,
	uint64_t off, size_t size)
{
	uint64_t *src = (uint64_t *)((uint64_t)REAL(obuf->uobj) + off);
	uint64_t *dst = (uint64_t *)((uint64_t)REAL(obuf->pobj) + off);

	ASSERT(ALIGNED_CL(src));
	ASSERT(ALIGNED_CL(dst));
	ASSERT(ALIGNED_CL(size));

#ifdef PANGOLIN_CHECKSUM
	/* The first cache line is updated last because it contains checksum. */
	if (OBUF_IS_NEWOBJ(obuf) && (off == 0)) {
		/*
		 * For a new object, wait until the first cache line (updated
		 * last) to compute a checksum for the whole object.
		 */
		size_t user_size = USIZE(obuf->uobj);
		obuf->csum = pangolin_adler32(CSUM0, obuf->uobj, user_size);
	} else if (!OBUF_IS_NEWOBJ(obuf)) {
		/* For an existing object, use incremental updating method. */
		obuf_update_checksum(obuf, off, size);
	}
#endif

#ifdef PANGOLIN_PARITY
	int err = pangolin_update_parity(pop, dst, src, size);
	if (err != 0) {
		LOG_ERR("pangolin parity update failed");
		return EINVAL;
	}
#endif

	pangolin_update_data(pop, src, dst, size);

	return 0;
}

/**
 * only update csum part
*/
static inline int
obuf_heap_range_update_csum(PMEMobjpool *pop, struct objbuf *obuf,
	uint64_t off, size_t size) {
	uint64_t *src = (uint64_t *)((uint64_t)REAL(obuf->uobj) + off);
	uint64_t *dst = (uint64_t *)((uint64_t)REAL(obuf->pobj) + off);

	ASSERT(ALIGNED_CL(src));
	ASSERT(ALIGNED_CL(dst));
	ASSERT(ALIGNED_CL(size));

#ifdef PANGOLIN_CHECKSUM
	/* The first cache line is updated last because it contains checksum. */
	if (OBUF_IS_NEWOBJ(obuf) && (off == 0)) {
		/*
		 * For a new object, wait until the first cache line (updated
		 * last) to compute a checksum for the whole object.
		 */
		size_t user_size = USIZE(obuf->uobj);
		obuf->csum = pangolin_adler32(CSUM0, obuf->uobj, user_size);
	} else if (!OBUF_IS_NEWOBJ(obuf)) {
		/* For an existing object, use incremental updating method. */
		obuf_update_checksum(obuf, off, size);
	}
#endif
	return 0;
}

/**
 * only update parity part
 * like obuf_heap_range_update_csum()
*/
static inline int
obuf_heap_range_update_parity(PMEMobjpool *pop, struct objbuf *obuf,
	uint64_t off, size_t size)
{
#ifdef PANGOLIN_PIPELINE_DATA
	uint64_t *src = obuf->obj_delta + size/8;
#else
	uint64_t *src = (uint64_t *)((uint64_t)REAL(obuf->uobj) + off);
#endif
	uint64_t *dst = (uint64_t *)((uint64_t)REAL(obuf->pobj) + off);
	int err;
	ASSERT(ALIGNED_CL(src));
	ASSERT(ALIGNED_CL(dst));
	ASSERT(ALIGNED_CL(size));

#ifdef PANGOLIN_PARITY
	#ifdef PANGOLIN_PIPELINE_DATA
		err = pangolin_update_parity_x(pop, dst, obuf->obj_delta, size, 1);
	#else
		err = pangolin_update_parity(pop, dst, src, size);
	#endif
	if (err != 0) {
		LOG_ERR("pangolin parity update failed");
		return EINVAL;
	}
#endif
	return 0;
}
/**
 * only update data part 
*/
static inline int
obuf_heap_range_update_data(PMEMobjpool *pop, struct objbuf *obuf,
	uint64_t off, size_t size)
{
	uint64_t *src = (uint64_t *)((uint64_t)REAL(obuf->uobj) + off);
	uint64_t *dst = (uint64_t *)((uint64_t)REAL(obuf->pobj) + off);
	ASSERT(ALIGNED_CL(src));
	ASSERT(ALIGNED_CL(dst));
	ASSERT(ALIGNED_CL(size));
	pmemops_memcpy(&pop->p_ops, dst, src, size,
		PMEMOBJ_F_MEM_WC | PMEMOBJ_F_MEM_NODRAIN);
	return 0;
}

// src is dram new data, dst is pm old data
static inline int swap_delta_avx512_debug(uint64_t *src, uint64_t *dst, uint64_t *delta, size_t size, const struct pmem_ops *p_ops) {
	ASSERT(ALIGNED_CL(size));
	void *array[3] = {dst, src, delta};
	swap_delta_avx512(3, size, array);

	// flatten
	// memcpy(delta, dst, size);  // read old data to delta
	// pmemops_memcpy(p_ops, dst, src, size, PMEMOBJ_F_MEM_WC | PMEMOBJ_F_MEM_NODRAIN); // nt-store new data
	// // gen delta data
	// void *array[3] = {src, delta, delta};
	// xor_gen_avx512_pgl(3, size, array);
	return 0;
}

#ifdef PANGOLIN_PIPELINE_DATA
/**
 * update data part and store delta data in src
*/
static inline int
obuf_heap_range_update_data_ddata(PMEMobjpool *pop, struct objbuf *obuf,
	uint64_t off, size_t size)
{
	uint64_t *src = (uint64_t *)((uint64_t)REAL(obuf->uobj) + off);
	uint64_t *dst = (uint64_t *)((uint64_t)REAL(obuf->pobj) + off);
	ASSERT(ALIGNED_CL(size));

	uint64_t *delta[2] = {obuf->obj_delta, obuf->obj_delta + size/8};

	// pmemops_memcpy(&pop->p_ops, dst, src, size,
	// 	PMEMOBJ_F_MEM_WC | PMEMOBJ_F_MEM_NODRAIN);

	// void *array[3] = {dst, src, src};
	// swap_delta_avx512(3, size, array);
	swap_delta_avx512_debug(src, dst, delta[1], size, &pop->p_ops);

#ifdef PANGOLIN_PIPELINE_DATA_2M
	// delta_data -> delta_parity
	struct ec_runtime *ec = ec_get_runtime();
	if (ec->p != 1) {  // delta_data -> delta_parity
		uint16_t vec_i;
		uint64_t offset = 0;
		struct obuf_runtime *ort = obuf_get_runtime();
		void *heap_start = (char *)pop + pop->heap_offset;
		offset = (uint64_t)dst - (uint64_t)heap_start - sizeof(struct heap_header);
		uint32_t zid = 0;
		while (offset >= ZONE_MAX_SIZE) {
			zid += 1;
			offset -= ZONE_MAX_SIZE;
		}
		struct zone *z = ZID_TO_ZONE(heap_start, zid);
		uint64_t row_size;
		uint64_t parity_start;
		if (zid != ort->last_zone_id) {
			row_size = ort->zone_row_size;
		} else {
			row_size = ort->last_zone_row_size;
		}
		offset -= sizeof(struct zone);
		while (offset >= row_size) {
			offset -= row_size;
			vec_i += 1;
		}
		encode_update(ec, delta[1], delta, size, vec_i, PANGOLIN_EC_TEMPORAL);
	}   // if ec->p==1, delta_data is delta_parity, so no more coding

#endif

	return 0;
}
#endif
// #endif

/*
 * obuf_heap_update -- update a heap object (range) using an objbuf
 * update content on PM: call obuf_head_obj_update series functions one by one based on the address in obuf
 * Note: no need to consider persistent write problem, it's guaranteed by the outside
 */
static int
obuf_heap_update(PMEMobjpool *pop, struct objbuf *obuf)
{
	if (obuf->sranges == 0 && obuf->lranges == 0)  // update a whole object
		return obuf_heap_obj_update(pop, obuf);

// if not update a whole object...
#ifdef PANGOLIN_CHECKSUM
	int nranges = 0;
	int cl0_skipped = 1;

	/* avoid breaking a single range into two. */
	for (int i = 0; i < OBUF_SRANGES; i++) {
		uint32_t s8 = obuf->srange[2 * i];
		uint32_t e8 = obuf->srange[2 * i + 1];

		if (s8 != 0 && e8 != 0)
			nranges++;
	}
	nranges += obuf->lranges != 0;
	if (nranges == 0)
		return 0;
#endif

	for (int i = 0; i < OBUF_SRANGES; i++) {
		uint32_t s8 = obuf->srange[2 * i];
		uint32_t e8 = obuf->srange[2 * i + 1];

		if (s8 == 0 && e8 == 0)
			continue;

		ASSERT(s8 > 0 && e8 > 0 && s8 <= e8);

		uint64_t scl = (s8 - 1) >> 3;
		uint64_t ecl = (e8 - 1) >> 3;

#ifdef PANGOLIN_CHECKSUM
		int inc_scl = scl == 0 && nranges > 1;
		cl0_skipped = cl0_skipped && (inc_scl || scl > 0);
		/* skip the first cache line with checksum */
		scl = inc_scl ? scl + 1 : scl;
#endif
		/* off is relative to the start of REAL(uobj) */
		uint64_t off = scl << CLSHIFT;
		size_t size = (ecl + 1 - scl) << CLSHIFT;

		/* size can be zero due to skipped first cache line */
		if (size > 0)
			obuf_heap_range_update(pop, obuf, off, size);
	}

	if (obuf->lranges != 0) {
		uint32_t s32 = obuf->lrange[0];
		uint32_t e32 = obuf->lrange[1];
		ASSERT(s32 > 0 && e32 > 0 && s32 <= e32);

		uint64_t scl = (s32 - 1) >> 1;
		uint64_t ecl = (e32 - 1) >> 1;

#ifdef PANGOLIN_CHECKSUM
		int inc_scl = scl == 0 && nranges > 1;
		cl0_skipped = cl0_skipped && (inc_scl || scl > 0);
		/* skip the first cache line with checksum */
		scl = inc_scl ? scl + 1 : scl;
#endif
		/* off is relative to the start of REAL(uobj) */
		uint64_t off = scl << CLSHIFT;
		size_t size = (ecl + 1 - scl) << CLSHIFT;

		/* size can be zero due to skipped first cache line */
		if (size > 0)
			obuf_heap_range_update(pop, obuf, off, size);
	}

#ifdef PANGOLIN_CHECKSUM
	/*
	 * Getting here means we have updated some ranges of an object except
	 * for its first cache line that contains a checksum.
	 */
	if (cl0_skipped)
		obuf_heap_range_update(pop, obuf, 0, CACHELINE_SIZE);
#endif

	return 0;
}

#ifdef PANGOLIN_PIPELINE_DATA
/**
 * update new checksum, flush new data to PM, and store delta-data in `src`
*/
static int
obuf_heap_update_data_checksum(PMEMobjpool *pop, struct objbuf *obuf) {
	// unsigned short update_times = 0;
#ifdef PANGOLIN_CHECKSUM
	int nranges = 0;
	int cl0_skipped = 1;
	for (int i = 0; i < OBUF_SRANGES; i++) {
		uint32_t s8 = obuf->srange[2 * i];
		uint32_t e8 = obuf->srange[2 * i + 1];
		if (s8 != 0 && e8 != 0) nranges++;
	}
	nranges += obuf->lranges != 0;
	if (nranges == 0) return 0;
#endif
	for (int i = 0; i < OBUF_SRANGES; i++) {  // srange
		uint32_t s8 = obuf->srange[2 * i];
		uint32_t e8 = obuf->srange[2 * i + 1];
		if (s8 == 0 && e8 == 0) continue;
		ASSERT(s8 > 0 && e8 > 0 && s8 <= e8);
		uint64_t scl = (s8 - 1) >> 3;
		uint64_t ecl = (e8 - 1) >> 3;
#ifdef PANGOLIN_CHECKSUM
		int inc_scl = scl == 0 && nranges > 1;
		cl0_skipped = cl0_skipped && (inc_scl || scl > 0);
		scl = inc_scl ? scl + 1 : scl;
#endif
		size_t size = (ecl + 1 - scl) << CLSHIFT;
		if (size > 0) {
			obuf_heap_range_update_csum(pop, obuf, scl << CLSHIFT, size);
			obuf_heap_range_update_data_ddata(pop, obuf, scl << CLSHIFT, size);
		}
	}
	if (obuf->lranges != 0) {  // lrange
		uint32_t s32 = obuf->lrange[0];
		uint32_t e32 = obuf->lrange[1];
		ASSERT(s32 > 0 && e32 > 0 && s32 <= e32);
		uint64_t scl = (s32 - 1) >> 1;
		uint64_t ecl = (e32 - 1) >> 1;
#ifdef PANGOLIN_CHECKSUM
		int inc_scl = scl == 0 && nranges > 1;
		cl0_skipped = cl0_skipped && (inc_scl || scl > 0);
		scl = inc_scl ? scl + 1 : scl;
#endif
		size_t size = (ecl + 1 - scl) << CLSHIFT;
		if (size > 0) {
			obuf_heap_range_update_csum(pop, obuf, scl << CLSHIFT, size);
			obuf_heap_range_update_data_ddata(pop, obuf, scl << CLSHIFT, size);
		}
	}
#ifdef PANGOLIN_CHECKSUM
	if (cl0_skipped) {
		obuf_heap_range_update_csum(pop, obuf, 0, CACHELINE_SIZE);
		obuf_heap_range_update_data_ddata(pop, obuf, 0, CACHELINE_SIZE);
	}
#endif
	return 0;
}
#endif
/*
 * obuf_heap_update_parity_checksum update parity and new checksum in obuf
 * obuf_heap_update_parity_checksum + obuf_heap_update_data = obuf_heap_update
 * update parity and gen new csum for newd
 * ONLY a wrapper of heap_range_update
*/ 
static int
obuf_heap_update_parity_checksum(PMEMobjpool *pop, struct objbuf *obuf) {
#ifdef PANGOLIN_CHECKSUM
	int nranges = 0;
	int cl0_skipped = 1;
	for (int i = 0; i < OBUF_SRANGES; i++) {
		uint32_t s8 = obuf->srange[2 * i];
		uint32_t e8 = obuf->srange[2 * i + 1];
		if (s8 != 0 && e8 != 0) nranges++;
	}
	nranges += obuf->lranges != 0;
	if (nranges == 0) return 0;
#endif
/* update parity after updating data and checksum */
	for (int i = 0; i < OBUF_SRANGES; i++) {  // srange
		uint32_t s8 = obuf->srange[2 * i];
		uint32_t e8 = obuf->srange[2 * i + 1];
		if (s8 == 0 && e8 == 0) continue;
		ASSERT(s8 > 0 && e8 > 0 && s8 <= e8);
		uint64_t scl = (s8 - 1) >> 3;
		uint64_t ecl = (e8 - 1) >> 3;
#ifdef PANGOLIN_CHECKSUM
		int inc_scl = scl == 0 && nranges > 1;
		cl0_skipped = cl0_skipped && (inc_scl || scl > 0);
		scl = inc_scl ? scl + 1 : scl;
#endif
		size_t size = (ecl + 1 - scl) << CLSHIFT;
		if (size > 0) {
			obuf_heap_range_update_csum(pop, obuf, scl << CLSHIFT, size);
			obuf_heap_range_update_parity(pop, obuf, scl << CLSHIFT, size);
			// update_times++;
		}
	}
	if (obuf->lranges != 0) {  // lrange
		uint32_t s32 = obuf->lrange[0];
		uint32_t e32 = obuf->lrange[1];
		ASSERT(s32 > 0 && e32 > 0 && s32 <= e32);
		uint64_t scl = (s32 - 1) >> 1;
		uint64_t ecl = (e32 - 1) >> 1;
#ifdef PANGOLIN_CHECKSUM
		int inc_scl = scl == 0 && nranges > 1;
		cl0_skipped = cl0_skipped && (inc_scl || scl > 0);
		scl = inc_scl ? scl + 1 : scl;
#endif
		size_t size = (ecl + 1 - scl) << CLSHIFT;
		if (size > 0) {
			obuf_heap_range_update_csum(pop, obuf, scl << CLSHIFT, size);
			obuf_heap_range_update_parity(pop, obuf, scl << CLSHIFT, size);
		}
	}
#ifdef PANGOLIN_CHECKSUM
	if (cl0_skipped) {
		obuf_heap_range_update_csum(pop, obuf, 0, CACHELINE_SIZE);
		obuf_heap_range_update_parity(pop, obuf, 0, CACHELINE_SIZE);
	}
#endif
	return 0;
}

/**
 * only update parity part, and only called by obuf_tx_heap_commit_pipeline
 * use delta to do fast encoding
*/
static int
obuf_heap_update_parity(PMEMobjpool *pop, struct objbuf *obuf) {
	// unsigned short update_times = 0;
#ifdef PANGOLIN_CHECKSUM
	int nranges = 0;
	int cl0_skipped = 1;
	for (int i = 0; i < OBUF_SRANGES; i++) {
		uint32_t s8 = obuf->srange[2 * i];
		uint32_t e8 = obuf->srange[2 * i + 1];
		if (s8 != 0 && e8 != 0) nranges++;
	}
	nranges += obuf->lranges != 0;
	if (nranges == 0) return 0;
#endif
/* update parity after updating data and checksum */
	for (int i = 0; i < OBUF_SRANGES; i++) {  // srange
		uint32_t s8 = obuf->srange[2 * i];
		uint32_t e8 = obuf->srange[2 * i + 1];
		if (s8 == 0 && e8 == 0) continue;
		ASSERT(s8 > 0 && e8 > 0 && s8 <= e8);
		uint64_t scl = (s8 - 1) >> 3;
		uint64_t ecl = (e8 - 1) >> 3;
#ifdef PANGOLIN_CHECKSUM
		int inc_scl = scl == 0 && nranges > 1;
		cl0_skipped = cl0_skipped && (inc_scl || scl > 0);
		scl = inc_scl ? scl + 1 : scl;
#endif
		size_t size = (ecl + 1 - scl) << CLSHIFT;
		if (size > 0) {
			obuf_heap_range_update_parity(pop, obuf, scl << CLSHIFT, size);
			// update_times++;
		}
	}
	if (obuf->lranges != 0) {  // lrange
		uint32_t s32 = obuf->lrange[0];
		uint32_t e32 = obuf->lrange[1];
		ASSERT(s32 > 0 && e32 > 0 && s32 <= e32);
		uint64_t scl = (s32 - 1) >> 1;
		uint64_t ecl = (e32 - 1) >> 1;
#ifdef PANGOLIN_CHECKSUM
		int inc_scl = scl == 0 && nranges > 1;
		cl0_skipped = cl0_skipped && (inc_scl || scl > 0);
		scl = inc_scl ? scl + 1 : scl;
#endif
		size_t size = (ecl + 1 - scl) << CLSHIFT;
		if (size > 0) {
			obuf_heap_range_update_parity(pop, obuf, scl << CLSHIFT, size);
		}
	}
#ifdef PANGOLIN_CHECKSUM
	if (cl0_skipped) {
		obuf_heap_range_update_parity(pop, obuf, 0, CACHELINE_SIZE);
	}
#endif
	return 0;
}

/**
 * flush new data and new csum (in OBJHDR)
*/ 
static int
obuf_heap_update_data(PMEMobjpool *pop, struct objbuf *obuf)
{
	// unsigned short update_times = 0;
#ifdef PANGOLIN_CHECKSUM
	int nranges = 0;
	int cl0_skipped = 1;

	/* avoid breaking a single range into two. */
	for (int i = 0; i < OBUF_SRANGES; i++) {
		uint32_t s8 = obuf->srange[2 * i];
		uint32_t e8 = obuf->srange[2 * i + 1];
		if (s8 != 0 && e8 != 0) nranges++;
	}
	nranges += obuf->lranges != 0;
	if (nranges == 0) return 0;
#endif
/* update data */
	for (int i = 0; i < OBUF_SRANGES; i++) {  // srange
		uint32_t s8 = obuf->srange[2 * i];
		uint32_t e8 = obuf->srange[2 * i + 1];
		if (s8 == 0 && e8 == 0) continue;
		uint64_t scl = (s8 - 1) >> 3;
		uint64_t ecl = (e8 - 1) >> 3;
#ifdef PANGOLIN_CHECKSUM
		int inc_scl = scl == 0 && nranges > 1;
		scl = inc_scl ? scl + 1 : scl;
#endif
		size_t size = (ecl + 1 - scl) << CLSHIFT;
		if (size > 0) {
			obuf_heap_range_update_data(pop, obuf, scl << CLSHIFT, size);
			// update_times++;	
		}
	}
	if (obuf->lranges != 0) {  // lrange
		uint32_t s32 = obuf->lrange[0];
		uint32_t e32 = obuf->lrange[1];
		uint64_t scl = (s32 - 1) >> 1;
		uint64_t ecl = (e32 - 1) >> 1;
#ifdef PANGOLIN_CHECKSUM
		int inc_scl = scl == 0 && nranges > 1;
		scl = inc_scl ? scl + 1 : scl;
#endif
		size_t size = (ecl + 1 - scl) << CLSHIFT;
		if (size > 0) {
			obuf_heap_range_update_data(pop, obuf, scl << CLSHIFT, size);
			// update_times++;
		}
	}
#ifdef PANGOLIN_CHECKSUM
	if (cl0_skipped) {
		obuf_heap_range_update_data(pop, obuf, 0, CACHELINE_SIZE);
		// update_times++;
	}
#endif
	// ASSERT(update_times==1);  // prevent pipe buf from being reused, no need to assert in other cases
	return 0;
}

/**
 * obuf_heap_update for LOGFREE version,
 * the core change is to isolate the update parity and update data parts, and insert a memory fence in the middle
*/
static int
obuf_heap_update_logfree(PMEMobjpool *pop, struct objbuf *obuf)
{
	if (obuf->sranges == 0 && obuf->lranges == 0)  // update a whole object
		return obuf_heap_obj_update(pop, obuf);

#ifdef PANGOLIN_CHECKSUM
	int nranges = 0;
	int cl0_skipped = 1;

	/* avoid breaking a single range into two. */
	for (int i = 0; i < OBUF_SRANGES; i++) {
		uint32_t s8 = obuf->srange[2 * i];
		uint32_t e8 = obuf->srange[2 * i + 1];

		if (s8 != 0 && e8 != 0)
			nranges++;
	}
	nranges += obuf->lranges != 0;
	if (nranges == 0)
		return 0;
#endif

/* update new parity and gen new checksum */
	for (int i = 0; i < OBUF_SRANGES; i++) {  // srange
		uint32_t s8 = obuf->srange[2 * i];
		uint32_t e8 = obuf->srange[2 * i + 1];
		if (s8 == 0 && e8 == 0) continue;
		ASSERT(s8 > 0 && e8 > 0 && s8 <= e8);
		uint64_t scl = (s8 - 1) >> 3;
		uint64_t ecl = (e8 - 1) >> 3;
#ifdef PANGOLIN_CHECKSUM
		int inc_scl = scl == 0 && nranges > 1;
		cl0_skipped = cl0_skipped && (inc_scl || scl > 0);
		scl = inc_scl ? scl + 1 : scl;
#endif
		size_t size = (ecl + 1 - scl) << CLSHIFT;
		if (size > 0) {
			obuf_heap_range_update_csum(pop, obuf, scl << CLSHIFT, size);
			obuf_heap_range_update_parity(pop, obuf, scl << CLSHIFT, size);
		}
	}
	if (obuf->lranges != 0) {  // lrange
		uint32_t s32 = obuf->lrange[0];
		uint32_t e32 = obuf->lrange[1];
		ASSERT(s32 > 0 && e32 > 0 && s32 <= e32);
		uint64_t scl = (s32 - 1) >> 1;
		uint64_t ecl = (e32 - 1) >> 1;
#ifdef PANGOLIN_CHECKSUM
		int inc_scl = scl == 0 && nranges > 1;
		cl0_skipped = cl0_skipped && (inc_scl || scl > 0);
		scl = inc_scl ? scl + 1 : scl;
#endif
		size_t size = (ecl + 1 - scl) << CLSHIFT;
		if (size > 0) {
			obuf_heap_range_update_csum(pop, obuf, scl << CLSHIFT, size);
			obuf_heap_range_update_parity(pop, obuf, scl << CLSHIFT, size);
		}
	}
#ifdef PANGOLIN_CHECKSUM
	if (cl0_skipped) {
		obuf_heap_range_update_csum(pop, obuf, 0, CACHELINE_SIZE);
		obuf_heap_range_update_parity(pop, obuf, 0, CACHELINE_SIZE);
	}
#endif

/* memory fence to split data and parity updates */
#ifndef PANGOLIN_LOGFREE_NOFENCE
	pmemops_drain(&pop->p_ops);
#endif

/* update data */
	for (int i = 0; i < OBUF_SRANGES; i++) {  // srange
		uint32_t s8 = obuf->srange[2 * i];
		uint32_t e8 = obuf->srange[2 * i + 1];
		if (s8 == 0 && e8 == 0) continue;
		uint64_t scl = (s8 - 1) >> 3;
		uint64_t ecl = (e8 - 1) >> 3;
#ifdef PANGOLIN_CHECKSUM
		int inc_scl = scl == 0 && nranges > 1;
		scl = inc_scl ? scl + 1 : scl;
#endif
		size_t size = (ecl + 1 - scl) << CLSHIFT;
		if (size > 0) obuf_heap_range_update_data(pop, obuf, scl << CLSHIFT, size);
	}
	if (obuf->lranges != 0) {  // lrange
		uint32_t s32 = obuf->lrange[0];
		uint32_t e32 = obuf->lrange[1];
		uint64_t scl = (s32 - 1) >> 1;
		uint64_t ecl = (e32 - 1) >> 1;
#ifdef PANGOLIN_CHECKSUM
		int inc_scl = scl == 0 && nranges > 1;
		scl = inc_scl ? scl + 1 : scl;
#endif
		size_t size = (ecl + 1 - scl) << CLSHIFT;
		if (size > 0) obuf_heap_range_update_data(pop, obuf, scl << CLSHIFT, size);
	}
#ifdef PANGOLIN_CHECKSUM
	if (cl0_skipped)
		obuf_heap_range_update_data(pop, obuf, 0, CACHELINE_SIZE);
#endif

	return 0;
}

/*
 * obuf_tx_log_commit -- commit a transaction in the form of redo logs
 *
 * This function roughtly follows tx_pre_commit().
 */
static int
obuf_tx_log_commit(PMEMobjpool *pop, struct obuf_tx *otx)
{
	uint32_t obj_count = 0;
	struct objbuf *obuf;

	SLIST_FOREACH(obuf, &otx->txobjs, txobj) {
		if (obuf->state & OBUF_EINVAL)
			obuf_tx_abort("invalid object buffer");
		if (obuf->canary != OBUF_CANARY)
			obuf_tx_abort("invalid canary bytes");

		obj_count += 1;

		if (OBUF_IS_TXFREE(obuf))
			continue;

		/*
		 * For new allocated objects, write to their heap location. They
		 * do not need redo logging. On transaction abort, the object
		 * space will be freed and zero-filled.
		 * 
		 * Because you can undo to all zero.
		 */
		if (OBUF_IS_NEWOBJ(obuf)) {  // new written objects don't need log, just update PM heap space
			ASSERTeq(obuf->csum, CSUM0);
			obuf_heap_obj_update(pop, obuf);
			continue;
		}

#ifndef PANGOLIN_LOGFREE
/* make redo logging entries */
		/*
		 * This real_size is the allocated memory size for the user's
		 * object. Libpmemobj doesn't store the requested size.
		 */
		size_t real_size = obuf->size & ALLOC_HDR_FLAGS_MASK;

		/*
		 * PGL-OPT: Creating a redo log entry for each small range is
		 * not efficient. Should combine them.
		 */
		if (obuf->sranges == 0 && obuf->lranges == 0) {
			if (real_size > LARGE_SIZE_THRESHOLD)
				/* not an error, but need to optimize */
				LOG(1, "logging large size %lu", real_size);

			if (obuf_tx_log_range(obuf, 0, real_size) != 0)
				obuf_tx_abort("obuf_tx_log_range");

			continue;
		}

		for (int i = 0; i < OBUF_SRANGES; i++) {
			uint8_t s8 = obuf->srange[2 * i];
			uint8_t e8 = obuf->srange[2 * i + 1];

			if (s8 == 0 && e8 == 0)
				continue;

			ASSERT(s8 > 0 && e8 > 0 && s8 <= e8);

			uint64_t start = (uint64_t)(s8 - 1) << 3;
			uint64_t end = (uint64_t)(e8 - 1) << 3;

			/* off is relative to the start of user REAL(uobj) */
			uint64_t off = start;
			size_t size = end - start + 8; /* 8-byte range */

			if (obuf_tx_log_range(obuf, off, size) != 0)
				obuf_tx_abort("obuf_tx_log_range");
		}

		if (obuf->lranges != 0) {
			uint32_t s32 = obuf->lrange[0];
			uint32_t e32 = obuf->lrange[1];

			ASSERT(s32 > 0 && e32 > 0 && s32 <= e32);

			uint64_t start = (uint64_t)(s32 - 1) << 5;
			uint64_t end = (uint64_t)(e32 - 1) << 5;

			/* off is relative to the start of user REAL(uobj) */
			uint64_t off = start;
			size_t size = end - start + 32; /* 32-byte range */

			if (obuf_tx_log_range(obuf, off, size) != 0)
				obuf_tx_abort("obuf_tx_log_range");
		}
#endif
	}

#ifndef PANGOLIN_LOGFREE
	if (obj_count != otx->actvobjs)
		obuf_tx_abort("object count error");
#endif

	return 0;
}
#ifdef PANGOLIN_PIPELINE_DATA
/**
 * update data first, then update parity, and no extra buffer!
 * for:
 * PANGOLIN_PIPELINE_DATA variants
*/
static int
obuf_tx_heap_commit_pipeline(PMEMobjpool *pop, struct obuf_tx *otx) {
	struct objbuf *obuf_list[otx->actvobjs];  // temp array to store obuf
	struct objbuf *obuf;
	for (int i = 0; i < otx->actvobjs; i++) obuf_list[i] = NULL;
	LOG(1, "commit %u objects for txid %u", otx->actvobjs, otx->txid);
/* update new csum, pipelined update new data, get delta data in dram buffer */
	for (uint32_t i = 0; i < otx->actvobjs; i++) {
		obuf = SLIST_FIRST(&otx->txobjs);
		obuf_list[i] = obuf;
		SLIST_REMOVE_HEAD(&otx->txobjs, txobj);
		if (obuf->state & OBUF_EINVAL)
			obuf_tx_abort("invalid object buffer");
		LOG(1, "commit obj at offset 0x%lx", (uint64_t)obuf->pobj - (uint64_t)pop);
		if (!(OBUF_IS_NEWOBJ(obuf) || OBUF_IS_TXFREE(obuf))) {  // not new or not free
			if (obuf->sranges == 0 && obuf->lranges == 0) {  // obj full update
				uint64_t *src = REAL(obuf->uobj);
				uint64_t *dst = REAL(obuf->pobj);
				// ASSERT(obuf->pobj!=NULL);
				size_t size = obuf->size & ALLOC_HDR_FLAGS_MASK;
				obuf->csum = pangolin_adler32(CSUM0, obuf->uobj, size - OHDR_SIZE);  // update checksum to obuf
				ASSERT(ALIGNED_CL(size));
				// gen delat data in newd and persist new data on PM
				if (src != NULL) {  // src is new_data, dst is pm_data
					// void *array[3] = {dst, src, src};
					// swap_delta_avx512(3, size, array);
					swap_delta_avx512_debug(src, dst, obuf->obj_delta + size/8, size, &pop->p_ops);
				} else {  // if src==NULL -> append update -> src is d_data -> only write PM (TODO: won't be called?)
					pmemops_memcpy(&pop->p_ops, dst, src, size, PMEMOBJ_F_MEM_WC | PMEMOBJ_F_MEM_NODRAIN);  // won't be called
				}
#ifdef PANGOLIN_PIPELINE_DATA_2M
				// delta_data -> delta_parity
				struct ec_runtime *ec = ec_get_runtime();
				
				if (ec->p != 1) {  // delta_data -> delta_parity
					uint16_t vec_i = 0;
					uint64_t offset = 0;
					struct obuf_runtime *ort = obuf_get_runtime();
					void *heap_start = (char *)pop + pop->heap_offset;
					offset = (uint64_t)src - (uint64_t)heap_start - sizeof(struct heap_header);
					uint32_t zid = 0;
					while (offset >= ZONE_MAX_SIZE) {
						zid += 1;
						offset -= ZONE_MAX_SIZE;
					}
					struct zone *z = ZID_TO_ZONE(heap_start, zid);
					uint64_t row_size;
					uint64_t parity_start;
					if (zid != ort->last_zone_id) {
						row_size = ort->zone_row_size;
					} else {
						row_size = ort->last_zone_row_size;
					}
					offset -= sizeof(struct zone);
					while (offset >= row_size) {
						offset -= row_size;
						vec_i += 1;
					}
					uint64_t *delta[2] = {obuf->obj_delta, obuf->obj_delta + size/8};
					encode_update(ec, delta[1], delta, size, vec_i, PANGOLIN_EC_TEMPORAL);
				}  // if ec->p==1, delta_data is delta_parity, so no more coding
#endif
			} else {
				obuf_heap_update_data_checksum(pop, obuf);
			}
		}
	}

/* memory fence to split parity and data updates */
#ifndef PANGOLIN_LOGFREE_NOFENCE
	pmemops_drain(&pop->p_ops);  // TODO: make it lazy!!!
#endif

/* pipelined update parity (with delta-data in `src`) */
	for (uint32_t i = 0; i < otx->actvobjs; i++) {
		obuf = obuf_list[i];
		if (!(OBUF_IS_NEWOBJ(obuf) || OBUF_IS_TXFREE(obuf))) {
			if (obuf->sranges == 0 && obuf->lranges == 0) {  // obj full update
				// ASSERT(obuf->pobj!=NULL);
				uint64_t *dst = REAL(obuf->pobj);
				size_t size = obuf->size & ALLOC_HDR_FLAGS_MASK;
				pangolin_update_parity_x(pop, dst, obuf->obj_delta, size, 1);
			} else {
				obuf_heap_update_parity(pop, obuf);
			}
		}
		
		if (OBUF_IS_SHARED(obuf)) {  // clean
			obuf->txid = 0;
			if (OBUF_IS_TXFREE(obuf)) {
				pgl_close_shared(otx->pop, obuf->uobj);
			} else {
				obuf->sranges = 0;
				obuf->lranges = 0;
#ifdef PANGOLIN_CHECKSUM
				obuf->state = OBUF_CLR_CSDONE(obuf);
#endif
				obuf->state = OBUF_CLR_ADDALL(obuf);
				obuf->state = OBUF_SET_CPYOBJ(obuf);
			}
		} else { /* thread-local objbufs */
			if (!OBUF_IS_TXFREE(obuf)) {
				uint64_t oidoff = OBJ_PTR_TO_OFF(obuf->pop, obuf->pobj);
				void *obufret = cuckoo_remove(otx->ost->kvs, oidoff);
				ASSERTeq(obufret, obuf);
			}
			obuf_free_objbuf(obuf);
			continue;
		}
	}

	return 0;
}
#endif

/**
 * modify obuf_tx_heap_commit to separate data and parity updates for multiple actvobjs
 * to reduce the number of fences in logfree, only one fence is needed
 * TODO: separating updates will slightly reduce performance and worse locality
*/
static int
obuf_tx_heap_commit_logfree(PMEMobjpool *pop, struct obuf_tx *otx) {
	struct objbuf *obuf_list[otx->actvobjs];  // temp array to store obuf
	struct objbuf *obuf;

	LOG(1, "commit %u objects for txid %u", otx->actvobjs, otx->txid);
/* Update new csum and parity */
	for (uint32_t i = 0; i < otx->actvobjs; i++) {
		obuf = SLIST_FIRST(&otx->txobjs);  // process one obuf at a time
		obuf_list[i] = obuf;
		SLIST_REMOVE_HEAD(&otx->txobjs, txobj);
		if (obuf->state & OBUF_EINVAL)
			obuf_tx_abort("invalid object buffer");
		LOG(1, "commit obj at offset 0x%lx",
			(uint64_t)obuf->pobj - (uint64_t)pop);

		if (!(OBUF_IS_NEWOBJ(obuf) || OBUF_IS_TXFREE(obuf))) {
			if (obuf->sranges == 0 && obuf->lranges == 0) {  // obj full update
				uint64_t *src = REAL(obuf->uobj);
				uint64_t *dst = REAL(obuf->pobj);
				size_t size = obuf->size & ALLOC_HDR_FLAGS_MASK;
				obuf->csum = pangolin_adler32(CSUM0, obuf->uobj, size - OHDR_SIZE);
				pangolin_update_parity(pop, dst, src, size);
			} else {
				obuf_heap_update_parity_checksum(pop, obuf);
			}
		}
	}
/* memory fence to split parity and data updates */
#ifndef PANGOLIN_LOGFREE_NOFENCE
	pmemops_drain(&pop->p_ops);
#endif

/* update data */
	for (uint32_t i = 0; i < otx->actvobjs; i++) {
		obuf = obuf_list[i];
		if (!(OBUF_IS_NEWOBJ(obuf) || OBUF_IS_TXFREE(obuf))) {
			if (obuf->sranges == 0 && obuf->lranges == 0) {  // obj full update
				uint64_t *src = REAL(obuf->uobj);
				uint64_t *dst = REAL(obuf->pobj);
				size_t size = obuf->size & ALLOC_HDR_FLAGS_MASK;
				pmemops_memcpy(&pop->p_ops, dst, src, size, PMEMOBJ_F_MEM_WC | PMEMOBJ_F_MEM_NODRAIN);  // NT(WC)+NODRAIN to update data
			} else {
				obuf_heap_update_data(pop, obuf);
			}
		}
		
		if (OBUF_IS_SHARED(obuf)) {
			obuf->txid = 0;
			if (OBUF_IS_TXFREE(obuf)) {
				pgl_close_shared(otx->pop, obuf->uobj);
			} else {
				obuf->sranges = 0;
				obuf->lranges = 0;
#ifdef PANGOLIN_CHECKSUM
				obuf->state = OBUF_CLR_CSDONE(obuf);
#endif
				obuf->state = OBUF_CLR_ADDALL(obuf);
				obuf->state = OBUF_SET_CPYOBJ(obuf);
			}
		} else { /* thread-local objbufs */
			if (!OBUF_IS_TXFREE(obuf)) {
				uint64_t oidoff = OBJ_PTR_TO_OFF(obuf->pop, obuf->pobj);
				void *obufret = cuckoo_remove(otx->ost->kvs, oidoff);
				ASSERTeq(obufret, obuf);
			}
			obuf_free_objbuf(obuf);
			continue;
		}
	}

	return 0;
}

/*
 * obuf_tx_heap_commit -- update pmem heap objects (data&parity)
 * warpper of `obuf_heap_update()`
 */
static int
obuf_tx_heap_commit(PMEMobjpool *pop, struct obuf_tx *otx)
{
	struct objbuf *obuf;

	LOG(1, "commit %u objects for txid %u", otx->actvobjs, otx->txid);
	for (uint32_t i = 0; i < otx->actvobjs; i++) {
		obuf = SLIST_FIRST(&otx->txobjs);
		SLIST_REMOVE_HEAD(&otx->txobjs, txobj);

		if (obuf->state & OBUF_EINVAL)
			obuf_tx_abort("invalid object buffer");

		LOG(1, "commit obj at offset 0x%lx",
			(uint64_t)obuf->pobj - (uint64_t)pop);

		/* new objects were written back in log commit */
		if (!(OBUF_IS_NEWOBJ(obuf) || OBUF_IS_TXFREE(obuf))) {  // NOT a new obj or TXFREE
#ifdef PANGOLIN_LOGFREE_NOFENCE
			obuf_heap_update(pop, obuf); // default is no fence version
#else
	#ifdef PANGOLIN_LOGFREE
			// logfree version, and only support parity pipeline (early flush)
			obuf_heap_update_logfree(pop, obuf);
	#else
			obuf_heap_update(pop, obuf); // THE CORE PART
	#endif
#endif
		}
		if (OBUF_IS_SHARED(obuf)) {
			obuf->txid = 0;
			if (OBUF_IS_TXFREE(obuf)) {
				pgl_close_shared(otx->pop, obuf->uobj);
			} else {
				obuf->sranges = 0;
				obuf->lranges = 0;
#ifdef PANGOLIN_CHECKSUM
				obuf->state = OBUF_CLR_CSDONE(obuf);
#endif
				obuf->state = OBUF_CLR_ADDALL(obuf);
				obuf->state = OBUF_SET_CPYOBJ(obuf);
			}
		} else { /* thread-local objbufs */
			if (!OBUF_IS_TXFREE(obuf)) {
				uint64_t oidoff =
					OBJ_PTR_TO_OFF(obuf->pop, obuf->pobj);
				void *obufret =
					cuckoo_remove(otx->ost->kvs, oidoff);
				ASSERTeq(obufret, obuf);
			}
			obuf_free_objbuf(obuf);
			continue;
		}
	}

	ASSERT(SLIST_EMPTY(&otx->txobjs));

	return 0;
}


/**
 * Invalidate redo logs. operation_finish will execute sfence.
*/
static void
obuf_tx_post_commit(struct obuf_tx *otx)
{
#ifndef PANGOLIN_LOGFREE
#ifdef PANGOLIN
	/* Invalidate redo logs. operation_finish will execute sfence. */
	struct ulog *loghead = (struct ulog *)&otx->lane->layout->undo;
#ifdef PANGOLIN_LOGREP
	pmemops_memset(&otx->pop->p_ops, (char *)&loghead->replay - MREP_SIZE,
			0, sizeof(loghead->replay),
			PMEMOBJ_F_RELAXED | PMEMOBJ_F_MEM_NODRAIN);
#endif
	pmemops_memset(&otx->pop->p_ops, &loghead->replay, 0,
			sizeof(loghead->replay),
			PMEMOBJ_F_RELAXED | PMEMOBJ_F_MEM_NODRAIN);
#endif
#endif

	operation_finish(otx->lane->undo);

	otx->obp->size = 0;
	VEC_CLEAR(&otx->actions);
}

#define OBUF_HEAP_COMMIT_ACTVOBJS_THRESHOLD 1
/**
 * pgl_tx_commit -- commit a transaction, the first entry
 * 1. obuf_tx_log_commit	+ sfence  (redo-logging)
 * 2. obuf_tx_heap_commit	+ sfence  (update data&parity, the sfence is provided by pgl_tx_commit)
 * 3. obuf_tx_post_commit			  (clean-logging)
 */
int
pgl_tx_commit()
{
	struct obuf_tx *otx = obuf_get_tx();
	PMEMobjpool *pop = otx->pop;
	ASSERTne(pop, NULL);

	if (otx->level == 0) {

		otx->stage = PANGOLIN_TX_PRECOMMIT;

		/*
		 * PGL-TODO: Add return values to the following functions and
		 * check them.
		 */

		obuf_tx_log_commit(pop, otx);  // write redo-logging

#ifndef PANGOLIN_LOGFREE
		/*
		 * sfence
		 * Note: normally obuf_tx_log_commit in pangolin needs a drain
		 * but if no log is written, obviously no need to drain
		 * if a full obj update is triggered, the drain is given to the data update part
		 * [in other words, pangolin has an extra sfence bug here]
		*/
		pmemops_drain(&pop->p_ops);
#endif

		operation_start(otx->lane->external);  // handle metadata, including VALGRIND related
		/*
		 * palloc_publish() makes pool metadata reflect new allocations,
		 * invalidates the redo range logs, and finishes operation on
		 * otx->lane->external.
		 */
		palloc_publish(&pop->heap, VEC_ARR(&otx->actions),
			VEC_SIZE(&otx->actions), otx->lane->external);

#ifndef PANGOLIN_LOGFREE
		obuf_tx_heap_commit(pop, otx);  // update PM content (data + parity)
#else
	#ifdef PANGOLIN_PIPELINE_DATA  // normal logfree
		obuf_tx_heap_commit_pipeline(pop, otx); // pipeline data version
	#else
		obuf_tx_heap_commit_logfree(pop, otx);  // log-free version heap_commit for complex ops
	#endif
	
#endif

		pmemops_drain(&pop->p_ops); /* sfence to ensure persistency of this op*/

		obuf_tx_post_commit(otx);  // invalidate redo logs

		lane_release(otx->pop);
		otx->lane = NULL;
	}

	otx->stage = PANGOLIN_TX_POSTCOMMIT;

	return 0;
}

/*
 * pgl_tx_end -- end current transaction
 */
int
pgl_tx_end()
{
	struct obuf_tx *otx = obuf_get_tx();

	if (otx->frozen)
		return 0;

	if (otx->level == 0) {
		ASSERTne(otx->txid, 0);
		otx->txid = 0;
		otx->actvobjs = 0;
		otx->stage = PANGOLIN_TX_NONE;
		VEC_DELETE(&otx->actions);
#ifdef PANGOLIN_PARITY
		struct obuf_runtime *ort = otx->ort;
		__atomic_sub_fetch(&ort->actv_txs, 1, __ATOMIC_RELAXED);
#ifdef PANGOLIN_CHECKSUM
		/*
		 * We do not need atomics for past_txs since it does not have to
		 * be accurate.
		 */
		ort->past_txs += 1;
		if (ort->scrub_txs > 0 && ort->past_txs >= ort->scrub_txs) {
#ifdef PROBE
			pangolin_update_stats();
			pgl_reset_stats();
#endif
			pangolin_scrub_task(ort, SCRUB_TXS);
			ort->past_txs = 0;
		}
#endif
#endif
		otx->pop = NULL;
		otx->ost = NULL;
		otx->obp = NULL;
	} else {
		otx->level -= 1;
		otx->stage = PANGOLIN_TX_WORK;
	}

	return 0;
}
