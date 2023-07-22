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
 * libpangolin.c -- libpangolin APIs
 */
#ifndef LIBPANGOLIN_H
#define LIBPANGOLIN_H 1

// #include "pangolin_ec.h"
#include "libpmemobj.h"

#ifdef __cplusplus
extern "C" {
#endif

#define RST "\x1B[0m"
#define RED "\x1B[1m\x1B[31m"
#define GRN "\x1B[1m\x1B[32m"
#define BLU "\x1B[1m\x1B[34m"

#define container_of(ptr, type, member) (\
{\
	const typeof(((type *)0)->member) *__mptr = (ptr);\
	(type *)((char *)__mptr - offsetof(type, member));\
})

enum pgl_tx_stage {
	PANGOLIN_TX_NONE,	/* no transaction in this thread */
	PANGOLIN_TX_WORK,	/* transaction in progress */
	PANGOLIN_TX_PRECOMMIT,	/* work done, prepare to committed */
	PANGOLIN_TX_POSTCOMMIT,	/* successfully committed */
	PANGOLIN_TX_ONABORT,	/* tx_begin failed or transaction aborted */
	PANGOLIN_TX_FINALLY,	/* always called */

	MAX_PANGOLIN_TX_STAGE
};

/* -------------------------- pangolin transaction -------------------------- */

#define PGL_TX_BEGIN(pop)\
{\
	int _stage;\
	int _bt_errno = pgl_tx_begin(pop);\
	if (_bt_errno)\
		errno = _bt_errno;\
	while ((_stage = pgl_tx_stage()) != PANGOLIN_TX_NONE) {\
		switch (_stage) {\
			case PANGOLIN_TX_WORK:

#define PGL_TX_ONABORT\
				pgl_tx_process();\
				break;\
			case PANGOLIN_TX_ONABORT:

#define PGL_TX_PRECOMMIT\
				pgl_tx_process();\
				break;\
			case PANGOLIN_TX_PRECOMMIT:

#define PGL_TX_POSTCOMMIT\
				pgl_tx_process();\
				break;\
			case PANGOLIN_TX_POSTCOMMIT:

#define PGL_TX_FINALLY\
				pgl_tx_process();\
				break;\
			case PANGOLIN_TX_FINALLY:

#define PGL_TX_END\
				pgl_tx_process();\
				break;\
			default:\
				pgl_tx_process();\
				break;\
		}\
	}\
	_bt_errno = pgl_tx_end();\
	if (_bt_errno)\
		errno = _bt_errno;\
}

#define SWAP_PTR(pa, pb)\
do { void *_ptemp_ = pa; pa = pb; pb = _ptemp_; } while (0)

#define PTR_TO_OBJ(ptr, pobj, type)\
((char *)pobj <= (char *)ptr && (char *)ptr < (char *)pobj + sizeof(type))

#define DISP(ref, ptr)\
((uintptr_t)ptr - (uintptr_t)ref)

#define PTR_TO(ptr, dist)\
((void *)((uintptr_t)ptr + (uintptr_t)dist))

#define PGL_TX_NEW(t)\
((TOID(t))pgl_tx_alloc(sizeof(t), TOID_TYPE_NUM(t)))

#define PGL_TX_ALLOC(t, size)\
((TOID(t))pgl_tx_alloc(size, TOID_TYPE_NUM(t)))

/* libpangolin always does zalloc */
#define PGL_TX_ZNEW(t)\
PGL_TX_NEW(t)

PMEMoid
pgl_oid(void *uobj);

void *
pgl_get(PMEMoid oid);

void *
pgl_open_shared(PMEMoid oid);

int
pgl_close(void *uobj);

int
pgl_close_shared(PMEMobjpool *pop, void *uobj);

void
pgl_close_all_shared(void);

int
pgl_tx_begin(PMEMobjpool *pop);

void *
pgl_open(PMEMobjpool *pop, PMEMoid oid);

int
pgl_commit(void *uobj);

void
pgl_poison_obj_page(PMEMoid oid);

void
pgl_scribble_obj(PMEMoid oid);

PMEMoid
pgl_tx_alloc(size_t size, uint64_t type_num);

void *
pgl_tx_alloc_open(size_t size, uint64_t type_num);

void *
pgl_tx_alloc_open_shared(size_t size, uint64_t type_num);

int
pgl_tx_free(PMEMoid oid);

void *
pgl_tx_open(PMEMoid oid);

void *
pgl_tx_open_shared(PMEMoid oid);

void *
pgl_tx_add(void *uobj);

void *
pgl_tx_add_shared(void *uobj);

void *
pgl_tx_add_range(void *uobj, uint64_t hoff, size_t size);

void *
pgl_tx_add_range_shared(void *uobj, uint64_t hoff, size_t size);

int
pgl_tx_commit(void);

int
pgl_tx_end(void);

enum pgl_tx_stage
pgl_tx_stage(void);

void
pgl_tx_process(void);

/* -------------------------- pangolin statistics -------------------------- */

void
pgl_print_stats(void);

void
pgl_reset_stats(void);

/* --------------------------- non-api functions --------------------------- */

int
pangolin_atomic_xor(PMEMobjpool *pop, void *src1, void *src2, void *dst,
	size_t size);

int
pangolin_avx_xor(PMEMobjpool *pop, void *src1, void *src2, void *dst,
	size_t size);

int
pangolin_repair_page(void *page);

int
pangolin_check_pool(PMEMobjpool *pop, int repair);

int
pangolin_scan(PMEMobjpool *pop, int repair, size_t num_thread, size_t num_parity);

int
pangolin_check_objs(PMEMobjpool *pop, int repair);

int
pangolin_check_obj_one(uint64_t *pobj);

#ifdef __cplusplus
}
#endif

#endif
