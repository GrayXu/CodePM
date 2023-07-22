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
 * pangolin.h -- pangolin declarations
 */

#ifndef LIBPANGOLIN_INTERNAL_H
#define LIBPANGOLIN_INTERNAL_H 1

#include "obj.h"
#include "os.h"
#include "out.h"
#include "tx.h"
#include "util.h"
#include "sys_util.h"
#include "heap_layout.h"
#include <pthread.h>

#define ALIGNED_8(x)	(((uint64_t)(x) & 0x07) == 0)
#define ALIGNED_16(x)	(((uint64_t)(x) & 0x0f) == 0)
#define ALIGNED_32(x)	(((uint64_t)(x) & 0x1f) == 0)
#define ALIGNED_64(x)	(((uint64_t)(x) & 0x3f) == 0)
#define ALIGNED_CL(x)	ALIGNED_64(x)
#define ALIGNED_4K(x)	(((uint64_t)(x) & 0xfff) == 0)

#define CLMASK		(CACHELINE_SIZE - 1)
#define CLSHIFT		(6)
/* cache line size in 8-byte words */
#define CLWORDS		(CACHELINE_SIZE / 8)

#define PAGESHIFT	(12)
#define PAGESIZE	(1 << PAGESHIFT)

#define RCLOCKSHIFT	(14)
#define RCLOCKSIZE	(1 << RCLOCKSHIFT)

/*
 * This structure must be binary identical to struct allocation_header_compact,
 * and we only support this compact header type. Every allocated object in a
 * pmemobj pool has this header as prefix. The 'extra' field only seems to store
 * a type value, which we assume can always fit 32-bit so we use the high-order
 * 32-bit as object checksum.
 *
 * size: 16b flags | 48b usable size (check ALLOC_HDR_SIZE_SHIFT)
 * This 'extra' field definition depends on little endianness (x86).
 */
#define OBJHDR\
	uint64_t size;\
	union {\
		struct {\
			uint32_t type;\
			uint32_t csum;\
		};\
		uint64_t extra;\
	}

/* compact object header */
struct objhdr {
	OBJHDR;
};

// because obj ptr has a 16B header, so the following macros are used to help
#define OHDR_SIZE	(sizeof(struct allocation_header_compact))  // 16B space, corresponding to OBJHDR
/* get the real allocated memory reference from an object pointer */
#define REAL(obj)	((void *)((uintptr_t)obj - OHDR_SIZE))
#define REAL_REVERSE(obj)	((void *)((uintptr_t)obj + OHDR_SIZE))  // +16B to data ptr
/* get the object header reference from an object pointer */
#define OHDR(obj)	((struct objhdr *)REAL(obj))
/* get the usable size from an object pointer */
#define USIZE(obj)	((OHDR(obj)->size & ALLOC_HDR_FLAGS_MASK) - OHDR_SIZE)

/* ---------------------------- data structures ---------------------------- */

/* thread-local objbuf pool capacity (does not grow) */
#define OBUF_POOL_CAP	(16384)
struct obuf_pool {
	size_t size; /* used bufpool size */
	uint8_t *base; /* provisioned objbuf pool */
};

struct obuf_shared_slot {
	int lock;
	/* a key-value store data structure to map an oid (pobj) to an objbuf */
	// But it can also be said that things in the form of obj lib all need to track obj, so it still maintains an extra objbuf
	struct cuckoo *kvs;
#ifdef PROBE
	size_t inserts;
	size_t removes;
	size_t objbufs;
	size_t maxobjs;
#endif
};

struct obuf_tlocal_slot {
	struct cuckoo *kvs;
	struct obuf_pool obp;
#ifdef PROBE
	size_t inserts;
	size_t removes;
	size_t objbufs;
	size_t maxobjs;
#endif
};

/*
 * Number of slots for objbufs. Shared objbufs go to different slots for better
 * concurrency. SHARED_NSLOTS should be a power of two for efficient computation
 * of a slot number. It should also be no less than #cores of a test machine.
 * Tlocal objbufs go to the slot of a transaction lane.
 */
#define SHARED_NSLOTS		(1 << 10)
#define SHARED_SLOT_MASK	(SHARED_NSLOTS - 1)
#define TLOCAL_NSLOTS		(OBJ_NLANES)

/*
 * replicated pool metadata size (starting from pop): pmemobjpool (8 KB),
 * lanes (3 MB), heap_header (4 KB).
 */
#define MREP_SIZE\
	(OBJ_LANES_OFFSET + OBJ_NLANES * sizeof(struct lane_layout) +\
	sizeof(struct heap_header))
/* offset of pool with pool metadata replication */
#define POOL_OFFSET		MREP_SIZE
/* offset of heap with pool metadata replication */
#define HEAP_OFFSET\
	(MREP_SIZE + OBJ_LANES_OFFSET + OBJ_NLANES * sizeof(struct lane_layout))
/* offset of zone0 with pool metadata replication */
#define ZONE_OFFSET		(HEAP_OFFSET + sizeof(struct heap_header))
#define ZONE_META_SIZE\
	(sizeof(struct zone_header) + sizeof(struct chunk_header) * MAX_CHUNK)

/* objbuf runtime information */
struct obuf_runtime {
	/*
	 * PGL-OPT: pop should be an array of pools, linearly indexed by the
	 * pool's uuid (most significant 16 bits of oid.pool_uuid_lo). For
	 * simplicity we use a single pool now.
	 */
	PMEMobjpool *pop; /* virtual address of a pmem object pool */
	struct obuf_shared_slot *shared; /* index for shared objbufs */
	struct obuf_tlocal_slot *tlocal; /* index for thread-local objbufs */

	uint32_t nzones; /* number of zones of this pool */

	/* max-sized zone's chunk arrays; part of rows reserved as parity */
	uint32_t zone_rows;
	uint32_t zone_rows_parity;

	/* max-sized zone's chunk array size in bytes */
	uint64_t zone_row_size;
	/* parity's start byte offset into a max-sized zone, parity's offset in zone */
	uint64_t zone_parity;
	uint64_t zone_parity_1;  // 2nd parity, zone_parity_1=zone_parity+zone_row_size

	/*
	 * range-column locks per zone
	 *
	 * PGL-OPT: We may use bit locks and __atomic_fetch_xor(). If we do
	 * locking at 4 KB page granularity, it requires a 64-bit vector lock
	 * for a 256 KB chunk.
	 */
#ifdef PANGOLIN_RCLOCKHLE
	int **zone_rclock;
#else
	os_rwlock_t **zone_rclock;
#endif

	/*
	 * All zones except the last one are max-sized and they share the same
	 * erasure-coding parameters. The last zone can have different ones
	 * since its size can be much smaller than a max-sized zone.
	 */
	uint32_t last_zone_id;
	uint32_t last_zone_rows;
	uint32_t last_zone_rows_parity;
	uint64_t last_zone_row_size;
	uint64_t last_zone_parity;
	uint64_t last_zone_parity_1;  // last_zone_parity_1=last_zone_parity+last_zone_row_size

	/*
	 * range size threshold for taking the parity range-lock and using
	 * SIMD instructions (non-atomic) for parity update.
	 */
	size_t rclock_threshold;  // 64B as default, or env $PANGOLIN_RCLOCK_THRESHOLD

	int get_verify_csum; /* verify every object's checksum with pgl_get */

	int scrub_tfd; /* scrub timerfd */
	int scrub_efd; /* scrub epoll fd */
	time_t scrub_sec; /* scrub interval's seconds */
	time_t scrub_msec; /* scrub interval's milliseconds */
	uint64_t scrub_txs; /* scrub per this number of transactions */
	int scrub_tid_valid;
	pthread_t scrub_tid; /* scrub thread id */

	uint32_t freeze; /* (atomic) pool-freezing flag */
	uint32_t actv_txs; /* ongoing transactions */
	uint64_t past_txs; /* past transactions */

#ifdef PROBE
	uint64_t new_objs; /* allocated objects */
	uint64_t new_size; /* allocated object size */
	uint64_t del_objs; /* deallocated objects */
	uint64_t del_size; /* deallocated object size */
	uint64_t mod_objs; /* modified objects */
	uint64_t mod_size; /* modified size in bytes */
	uint64_t vul_size; /* vulnerable object size */
	uint64_t get_size; /* object size by pgl_get */
	uint64_t get_size_max;
#endif
};

/* objbuf states */
enum obuf_state {
	OBUF_EINVAL = 0,
	OBUF_CPYOBJ = 1 << 0,
	/*
	 * OBUF_NEWOBJ means this is a newly allocated object but intentionally
	 * not opend by the user. In this case we do not allocate the uobj
	 * buffer in its objbuf.
	 */
	OBUF_NEWOBJ = 1 << 1,

	/* low 16 bits (above) are mutually exclusive */

	OBUF_ADDALL = 1 << 16, /* add all range to redo log */
	OBUF_SHARED = 1 << 17, /* objbuf shared by all threads */
	OBUF_INPOOL = 1 << 18, /* objbuf in provisioned objbuf pool */
	OBUF_TXFREE = 1 << 19, /* objbuf is freed in transaction */
	OBUF_CSDONE = 1 << 20  /* already checksumed */
};

/* PGL-OPT: Can we make a macro generator? */
#define OBUF_IS_CPYOBJ(obuf)\
((obuf->state & OBUF_CPYOBJ) == OBUF_CPYOBJ)
#define OBUF_IS_NEWOBJ(obuf)\
((obuf->state & OBUF_NEWOBJ) == OBUF_NEWOBJ)
#define OBUF_IS_ADDALL(obuf)\
((obuf->state & OBUF_ADDALL) == OBUF_ADDALL)
#define OBUF_IS_SHARED(obuf)\
((obuf->state & OBUF_SHARED) == OBUF_SHARED)
#define OBUF_IS_INPOOL(obuf)\
((obuf->state & OBUF_INPOOL) == OBUF_INPOOL)
#define OBUF_IS_TXFREE(obuf)\
((obuf->state & OBUF_TXFREE) == OBUF_TXFREE)
#define OBUF_IS_CSDONE(obuf)\
((obuf->state & OBUF_CSDONE) == OBUF_CSDONE)

#define OBUF_SET_CPYOBJ(obuf)\
((obuf->state & 0xFFFF0000) | OBUF_CPYOBJ)
#define OBUF_SET_NEWOBJ(obuf)\
((obuf->state & 0xFFFF0000) | OBUF_NEWOBJ)

#define OBUF_SET_ADDALL(obuf)\
(obuf->state | OBUF_ADDALL)
#define OBUF_CLR_ADDALL(obuf)\
(obuf->state & (enum obuf_state)(~OBUF_ADDALL))

#define OBUF_SET_SHARED(obuf)\
(obuf->state | OBUF_SHARED)
#define OBUF_CLR_SHARED(obuf)\
(obuf->state & (enum obuf_state)(~OBUF_SHARED))

#define OBUF_SET_INPOOL(obuf)\
(obuf->state | OBUF_INPOOL)
#define OBUF_CLR_INPOOL(obuf)\
(obuf->state & (enum obuf_state)(~OBUF_INPOOL))

#define OBUF_SET_TXFREE(obuf)\
(obuf->state | OBUF_TXFREE)
#define OBUF_CLR_TXFREE(obuf)\
(obuf->state & (enum obuf_state)(~OBUF_TXFREE))

#define OBUF_SET_CSDONE(obuf)\
(obuf->state | OBUF_CSDONE)
#define OBUF_CLR_CSDONE(obuf)\
(obuf->state & (enum obuf_state)(~OBUF_CSDONE))

/* objbuf actions */
enum obuf_action {
	OBUF_ACT_EINVAL = 0,
	OBUF_ACT_CPYOBJ = 1 << 0, /* copy data from an existing pmem object */
	OBUF_ACT_NEWOBJ = 1 << 1  /* newly allocated and fill with zeros */
};

/* modified ranges within an objbuf */
#define OBUF_SRANGES	(4)
/* object buffer structure */
/* canary bytes, a magic number */
#define OBUF_CANARY	(0x12345678)

/**
 * object's shadow buffer (micro-buffer)
*/
struct objbuf {
	uint32_t canary;  // canary for validation
	uint32_t disp; /* displacement for cache-line alignment */
	PMEMobjpool *pop; /* virtual address of the object's pool */
	void *pobj; /* PM data addr. virtual address of pmem object, excl. alloc header */
	uint32_t txid; /* max txid is #lanes, which should fit 32 bits */
	enum obuf_state state;
	SLIST_ENTRY(objbuf) txobj;

	union { /* small ranges, multiples of 8 bytes */
		uint8_t srange[8];
		uint64_t sranges;
	};

#ifdef PANGOLIN_PIPELINE_DATA
	// uint64_t **obj_delta;  // -> 2*size, get from alloc
	uint64_t *obj_delta;  // the delta buffer start addr
#else
	uint64_t unused[1]; /* can be mrange */
#endif

	union { /* large range, multiples of 32 bytes */
		uint32_t lrange[2]; /* large range start, end */
		uint64_t lranges;
	};

	/*
	 * Data fields above are transient, below will be committed to pmem.
	 * The persistent part must match PMDK's allocated object layout in pmem
	 * (incl. allocation header). We only support the compact header
	 * (allocation_header_compact).
	 *
	 * PGL-OPT: We like the following part being CL-aligned for good
	 * non-temporal store performance. Thus we have packed a CL amount of
	 * bytes above. But we still need to make the whole objbuf structure CL-
	 * aligned for the purpose. This may require a DRAM allocator for objbuf
	 * that supports custom alignment.
	 */
	struct {
		OBJHDR;
	};

	/* DRAM user's object data (as new data to be flushed)*/
	uint8_t uobj[];
} __attribute__((packed));

/* user's data offset within an objbuf */
#define OBUF_UOBJ_OFF	(sizeof(struct objbuf))
/* objbuf metadata size */
#define OBUF_META_SIZE	(OBUF_UOBJ_OFF - OHDR_SIZE)
/* get the objbuf reference from uobj */
#define OBUF(uobj)	((struct objbuf *)((uintptr_t)uobj - OBUF_UOBJ_OFF))

/**
 * scan thread metadata
*/
typedef struct thread_param {
	uint32_t c_start;
	uint32_t c_end;

	// uint64_t * xors_local;  // deprecated
	uint64_t ** parity_local;
	size_t num_parity;

	uint64_t * data_buf_local;

	uint32_t chunks;
	uint32_t rows;
	struct zone *z;
	size_t obj_size;

} thread_param;

/* ---------------------------- static functions ---------------------------- */

/* ---------------------------- pangolin runtime ---------------------------- */

/*
 * obuf_create_runtime -- create objbuf runtime data structure
 */
int
obuf_create_runtime(PMEMobjpool *pop);

/*
 * obuf_destroy_runtime -- destroy objbuf runtime data structures
 */
int
obuf_destroy_runtime(PMEMobjpool *pop);

/*
 * obuf_get_runtime -- get objbuf runtime data structure
 */
struct obuf_runtime *
obuf_get_runtime(void);

/*
 * obuf_find_shared -- find a shared objbuf
 */
struct objbuf *
obuf_find_shared(void *pobj);

/*
 * obuf_create_objbuf -- create an objbuf
 */
struct objbuf *
obuf_create_objbuf(PMEMobjpool *pop, void *pobj, struct obuf_pool *obp,
	enum obuf_action oact);

/*
 * obuf_insert_shared -- insert an objbuf into a shared slot
 */
struct objbuf *
obuf_insert_shared(void *pobj, struct objbuf *obuf);

/*
 * obuf_free_objbuf -- free an objbuf occupied memory
 */
void
obuf_free_objbuf(struct objbuf *obuf);

#ifdef DEBUG
struct objbuf *
obuf(void *uobj);

size_t
memdiff(char *src1, char *src2, size_t size);
#endif

/* -------------------------- pangolin replication -------------------------- */

/* pointed-to location type */
enum ptoloc {
	PTO_UNKNOWN = 0,
	PTO_TRANSIENT,
	PTO_POOL_META, /* 8 KB pool metadata */
	PTO_LANE_HEAP, /* 3 MB lane and 4 KB heap header */
	PTO_ZONE_META, /* zone header and chunk headers */
	PTO_USER_DATA  /* user data area, can store extended logs */
};

static inline enum ptoloc
pangolin_ptoloc(PMEMobjpool *pop, const void *src)
{
	if (src == NULL)
		FATAL("Invalid src address");

	if (pop == NULL)
		return PTO_TRANSIENT;

	if (pop->heap_size > 0 && !OBJ_PTR_FROM_POOL(pop, src))
		return PTO_UNKNOWN;

	uint64_t offset = OBJ_PTR_TO_OFF(pop, src);

	if (offset < OBJ_LANES_OFFSET)
		return PTO_POOL_META;

	if (POOL_OFFSET <= offset && offset < ZONE_OFFSET)
		return PTO_LANE_HEAP;

	offset -= ZONE_OFFSET;
	while (offset >= ZONE_MAX_SIZE)
		offset -= ZONE_MAX_SIZE;

	if (offset < ZONE_META_SIZE)
		return PTO_ZONE_META;

	return PTO_USER_DATA;
}

static inline void *
pangolin_extlog_repaddr(PMEMobjpool *pop, const void *src)
{
	return (char *)src + pop->tx_params->cache_size;
}

/*
 * pangolin_repaddr -- get pool or zone metadata replica address
 */
static inline void *
pangolin_repaddr(PMEMobjpool *pop, const void *src)
{
#if defined(PANGOLIN_METAREP) || defined(PANGOLIN_LOGREP)
	if (pop == NULL)
		return (void *)src;

	enum ptoloc ploc = pangolin_ptoloc(pop, src);

	if (ploc == PTO_UNKNOWN || ploc == PTO_TRANSIENT)
		return (void *)src;
	if (ploc == PTO_POOL_META)
		return (char *)src + MREP_SIZE;
	if (ploc == PTO_LANE_HEAP)
		return (char *)src - MREP_SIZE;
	if (ploc == PTO_ZONE_META)
		return (char *)src + ZONE_META_SIZE;

	/* Getting here means *src points to a heap-allocated ulog extension. */
	return pangolin_extlog_repaddr(pop, src);
#else
	FATAL("%s: called without PANGOLIN_METAREP or LOGREP", __func__);
#endif
}

int
pangolin_compare_zone_metarep(PMEMobjpool *pop, uint32_t zid);

void *
pangolin_replicate_heap_header(void *pop, const void *src, size_t len);

void *
pangolin_replicate_zone(void *pop, const void *src, size_t len);

/* ---------------------------- pangolin parity ---------------------------- */

uint32_t
pangolin_adjust_zone_size(uint32_t zone_id);

int
pangolin_update_data(PMEMobjpool *pop, void *src, void *dst, size_t size);

int
pangolin_update_parity(PMEMobjpool *pop, void *data, void *diff, size_t size);

int
pangolin_update_parity_x(PMEMobjpool *pop, void *data, void *diff, size_t size, uint8_t newd_is_delta);

void
pangolin_rebuild_lane_context(struct operation_context *ctx);

int
pangolin_repair_corruption(void *ptr, size_t size, int hwe,
	struct objbuf *obuf);

int
pangolin_register_sighdl(void);

#define NSECPSEC 1000000000
void
timer_get(struct timespec *time);

void
timer_diff(struct timespec *d, struct timespec *t1, struct timespec *t2);

double
timer_get_secs(struct timespec *t);

long
timer_get_nsecs(struct timespec *t);

/* --------------------------- pangolin checksum --------------------------- */

/* initial checksum value: ascii "CSUM", useful in hexdump debugging */
#define CSUM0		(0x4d555343UL)
#define ADLER_MOD	(65521)
#define MAX_ADLER_BUF	(1 << 28)
#define MAX_PATCH_BUF	(65200)

uint32_t
pangolin_crc32(uint32_t init, void *data, size_t len);

uint32_t
pangolin_adler32(uint32_t init, void *data, size_t len);

uint32_t
pangolin_adler32_patch(uint32_t csum, void *data, uint64_t dlen, uint64_t off,
	void *range, uint64_t rlen);

int
pangolin_update_zone_csum(PMEMobjpool *pop, int64_t zid, void *src, size_t len);

enum scrub_trigger {
	SCRUB_TXS = 0,
	SCRUB_TIMED
};

void
pangolin_update_stats(void);

int
pangolin_setup_scrub(PMEMobjpool *pop, struct obuf_runtime *ort);

void
pangolin_scrub_task(struct obuf_runtime *ort, enum scrub_trigger trigger);

#endif
