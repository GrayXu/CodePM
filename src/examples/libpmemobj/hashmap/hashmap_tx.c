/*
 * Copyright 2015-2017, Intel Corporation
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

/* integer hash set implementation which uses only transaction APIs */

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <inttypes.h>

#include <libpmemobj.h>
#include "hashmap_tx.h"
#include "hashmap_internal.h"

#include <libpangolin.h>

/* layout definition */
TOID_DECLARE(struct buckets, HASHMAP_TX_TYPE_OFFSET + 1);
TOID_DECLARE(struct entry, HASHMAP_TX_TYPE_OFFSET + 2);

struct entry {
	uint64_t key;
	PMEMoid value;

	/* next entry list pointer */
	TOID(struct entry) next;
};

struct buckets {
	/* number of buckets */
	size_t nbuckets;
	/* array of lists */
	TOID(struct entry) bucket[];
};

struct hashmap_tx {
	/* random number generator seed */
	uint32_t seed;

	/* hash function coefficients */
	uint32_t hash_fun_a;
	uint32_t hash_fun_b;
	uint64_t hash_fun_p;

	/* number of values inserted */
	uint64_t count;

	/* buckets */
	TOID(struct buckets) buckets;
};

/*
 * create_hashmap -- hashmap initializer
 */
static void
create_hashmap(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap, uint32_t seed)
{
	size_t len = INIT_BUCKETS_NUM;
	size_t sz = sizeof(struct buckets) +
			len * sizeof(TOID(struct entry));

	PGL_TX_BEGIN(pop) {
		struct hashmap_tx *hmap = pgl_tx_open(hashmap.oid);

		hmap->seed = seed;
		do {
			hmap->hash_fun_a = (uint32_t)rand();
		} while (hmap->hash_fun_a == 0);
		hmap->hash_fun_b = (uint32_t)rand();
		hmap->hash_fun_p = HASH_FUNC_COEFF_P;

		struct buckets *bkts = pgl_tx_alloc_open_shared(sz,
					TOID_TYPE_NUM(struct buckets));
		hmap->buckets = (TOID(struct buckets))pgl_oid(bkts);
		bkts->nbuckets = len;
	} PGL_TX_ONABORT {
		fprintf(stderr, "%s: transaction aborted\n", __func__);
		abort();
	} PGL_TX_END
}

/*
 * hash -- the simplest hashing function,
 * see https://en.wikipedia.org/wiki/Universal_hashing#Hashing_integers
 */
static uint64_t
hash(struct hashmap_tx *hmap, struct buckets *bkts, uint64_t value)
{
	uint32_t a = hmap->hash_fun_a;
	uint32_t b = hmap->hash_fun_b;
	uint64_t p = hmap->hash_fun_p;
	size_t len = bkts->nbuckets;

	return ((a * value + b) % p) % len;
}

/*
 * hm_tx_rebuild -- rebuilds the hashmap with a new number of buckets
 */
static void
hm_tx_rebuild(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap, size_t new_len)
{
	struct hashmap_tx *hmap = pgl_get(hashmap.oid);

	TOID(struct buckets) buckets_old = hmap->buckets;
	struct buckets *bkts_old = pgl_open_shared(buckets_old.oid);

	if (new_len == 0)
		new_len = bkts_old->nbuckets;

	size_t sz_new = sizeof(struct buckets) +
			new_len * sizeof(TOID(struct entry));

	PGL_TX_BEGIN(pop) {
		hmap = pgl_tx_add(hmap);
		struct buckets *bkts_new = pgl_tx_alloc_open_shared(sz_new,
						TOID_TYPE_NUM(struct buckets));
		bkts_new->nbuckets = new_len;

		bkts_old = pgl_tx_add_shared(bkts_old);

		for (size_t i = 0; i < bkts_old->nbuckets; ++i) {
			while (!TOID_IS_NULL(bkts_old->bucket[i])) {
				TOID(struct entry) en = bkts_old->bucket[i];
				struct entry *pen = pgl_tx_open(en.oid);
				uint64_t h = hash(hmap, bkts_new, pen->key);

				bkts_old->bucket[i] = pen->next;

				pen->next = bkts_new->bucket[h];
				bkts_new->bucket[h] = en;
			}
		}

		hmap->buckets = (TOID(struct buckets))pgl_oid(bkts_new);
		pgl_tx_free(buckets_old.oid);
	} PGL_TX_ONABORT {
		fprintf(stderr, "%s: transaction aborted\n", __func__);
	} PGL_TX_END
}

/*
 * hm_tx_insert -- inserts specified value into the hashmap,
 * returns:
 * - 0 if successful,
 * - 1 if value already existed,
 * - -1 if something bad happened
 */
int
hm_tx_insert(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap,
	uint64_t key, PMEMoid value)
{
	struct hashmap_tx *hmap = pgl_get(hashmap.oid);

	TOID(struct buckets) buckets = hmap->buckets;
	struct buckets *bkts = pgl_open_shared(buckets.oid);

	uint64_t h = hash(hmap, bkts, key);
	int num = 0;

	TOID(struct entry) var;
	struct entry *pvar = NULL;

	for (var = bkts->bucket[h]; !TOID_IS_NULL(var); var = pvar->next) {
		pvar = pgl_get(var.oid);
		if (pvar->key == key)
			return 1;

		num++;
	}

	int ret = 0;
	uint64_t count = hmap->count;
	size_t nbuckets = bkts->nbuckets;
	PGL_TX_BEGIN(pop) {
		hmap = pgl_tx_add(hmap);
		bkts = pgl_tx_add_range_shared(bkts,
				offsetof(struct buckets, bucket[h]),
				sizeof(PMEMoid));

		struct entry *e = pgl_tx_alloc_open(sizeof(struct entry),
						TOID_TYPE_NUM(struct entry));
		e->key = key;
		e->value = value;
		e->next = bkts->bucket[h];
		bkts->bucket[h] = (TOID(struct entry))pgl_oid(e);

		count += 1;
		hmap->count = count;
		num++;
	} PGL_TX_ONABORT {
		fprintf(stderr, "transaction aborted\n");
		ret = -1;
	} PGL_TX_END

	if (ret)
		return ret;

	if (num > MAX_HASHSET_THRESHOLD ||
		(num > MIN_HASHSET_THRESHOLD && count > 2 * nbuckets))
		hm_tx_rebuild(pop, hashmap, nbuckets * 2);

	return 0;
}

/*
 * hm_tx_remove -- removes specified value from the hashmap,
 * returns:
 * - key's value if successful,
 * - OID_NULL if value didn't exist or if something bad happened
 */
PMEMoid
hm_tx_remove(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap, uint64_t key)
{
	struct hashmap_tx *hmap = pgl_get(hashmap.oid);

	TOID(struct buckets) buckets = hmap->buckets;
	struct buckets *bkts = pgl_open_shared(buckets.oid);

	uint64_t h = hash(hmap, bkts, key);

	TOID(struct entry) var, prev = TOID_NULL(struct entry);
	struct entry *pvar = NULL, *pprev = NULL;

	for (var = bkts->bucket[h]; !TOID_IS_NULL(var);
			prev = var, var = pvar->next) {
		pvar = pgl_get(var.oid);
		if (pvar->key == key)
			break;
	}

	if (TOID_IS_NULL(var))
		return OID_NULL;

	int ret = 0;
	PMEMoid retoid = pvar->value;
	size_t nbuckets = bkts->nbuckets;

	PGL_TX_BEGIN(pop) {
		if (TOID_IS_NULL(prev)) {
			bkts = pgl_tx_add_range_shared(bkts,
					offsetof(struct buckets, bucket[h]),
					sizeof(PMEMoid));
		} else {
			pprev = pgl_tx_open(prev.oid);
		}
		hmap = pgl_tx_add(hmap);

		if (TOID_IS_NULL(prev)) {
			bkts->bucket[h] = pvar->next;
		} else {
			pprev->next = pvar->next;
		}
		hmap->count--;
		pgl_tx_free(var.oid);
	} PGL_TX_ONABORT {
		fprintf(stderr, "transaction aborted\n");
		ret = -1;
	} PGL_TX_END

	if (ret)
		return OID_NULL;

	if (hmap->count < nbuckets)
		hm_tx_rebuild(pop, hashmap, nbuckets / 2);

	return retoid;
}

/*
 * hm_tx_foreach -- prints all values from the hashmap
 */
int
hm_tx_foreach(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap,
	int (*cb)(uint64_t key, PMEMoid value, void *arg), void *arg)
{
	struct hashmap_tx *hmap = pgl_get(hashmap.oid);
	TOID(struct buckets) buckets = hmap->buckets;
	TOID(struct entry) var;

	struct buckets *bkts = pgl_open_shared(buckets.oid);
	struct entry *pvar = NULL;

	int ret = 0;
	for (size_t i = 0; i < bkts->nbuckets; ++i) {
		if (TOID_IS_NULL(bkts->bucket[i]))
			continue;

		for (var = bkts->bucket[i]; !TOID_IS_NULL(var);
				var = pvar->next) {
			pvar = pgl_get(var.oid);
			ret = cb(pvar->key, pvar->value, arg);
			if (ret)
				break;
		}
	}

	/*
	 * OBUF: This is the only place we close buckets' objbuf. In practice
	 * this close operation can be performed whenever the buckets is no
	 * longer used. If this foreach function is never called, libpangolin
	 * will remove and free this objbuf when program exits. The data_store
	 * program calls foreach seveval times, but map_bench doesn't.
	 */
	pgl_close_shared(pop, bkts);

	return ret;
}

/*
 * hm_tx_debug -- prints complete hashmap state
 */
static void
hm_tx_debug(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap, FILE *out)
{
	/* OBUF: This looks not used in evaluation. */
	fprintf(stderr, "%s not implemented with libpangolin\n", __func__);
}

/*
 * hm_tx_get -- checks whether specified value is in the hashmap
 */
PMEMoid
hm_tx_get(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap, uint64_t key)
{
	struct hashmap_tx *hmap = pgl_get(hashmap.oid);

	TOID(struct buckets) buckets = hmap->buckets;
	struct buckets *bkts = pgl_open_shared(buckets.oid);

	uint64_t h = hash(hmap, bkts, key);

	TOID(struct entry) var;
	struct entry *pvar = NULL;

	PMEMoid retoid = OID_NULL;
	for (var = bkts->bucket[h]; !TOID_IS_NULL(var); var = pvar->next) {
		pvar = pgl_get(var.oid);
		if (pvar->key == key) {
			retoid = pvar->value;
			break;
		}
	}

	return retoid;
}

/*
 * hm_tx_lookup -- checks whether specified value exists
 */
int
hm_tx_lookup(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap, uint64_t key)
{
	struct hashmap_tx *hmap = pgl_get(hashmap.oid);

	TOID(struct buckets) buckets = hmap->buckets;
	struct buckets *bkts = pgl_open_shared(buckets.oid);

	uint64_t h = hash(hmap, bkts, key);

	TOID(struct entry) var;
	struct entry *pvar = NULL;

	int found = 0;
	for (var = bkts->bucket[h]; !TOID_IS_NULL(var); var = pvar->next) {
		pvar = pgl_get(var.oid);
		if (pvar->key == key) {
			found = 1;
			break;
		}
	}

	return found;
}

/*
 * hm_tx_count -- returns number of elements
 */
size_t
hm_tx_count(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap)
{
	struct hashmap_tx *hmap = pgl_get(hashmap.oid);
	size_t count = hmap->count;

	return count;
}

/*
 * hm_tx_init -- recovers hashmap state, called after pmemobj_open
 */
int
hm_tx_init(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap)
{
	struct hashmap_tx *hmap = pgl_get(hashmap.oid);
	srand(hmap->seed);

	return 0;
}

/*
 * hm_tx_create -- allocates new hashmap
 */
int
hm_tx_create(PMEMobjpool *pop, TOID(struct hashmap_tx) *map, void *arg)
{
	struct hashmap_args *args = (struct hashmap_args *)arg;
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		*map = PGL_TX_ZNEW(struct hashmap_tx);

		uint32_t seed = args ? args->seed : 0;
		create_hashmap(pop, *map, seed);
	} PGL_TX_ONABORT {
		ret = -1;
	} PGL_TX_END

	return ret;
}

/*
 * hm_tx_check -- checks if specified persistent object is an
 * instance of hashmap
 */
int
hm_tx_check(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap)
{
	return TOID_IS_NULL(hashmap) || !TOID_VALID(hashmap);
}

/*
 * hm_tx_cmd -- execute cmd for hashmap
 */
int
hm_tx_cmd(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap,
		unsigned cmd, uint64_t arg)
{
	switch (cmd) {
		case HASHMAP_CMD_REBUILD:
			hm_tx_rebuild(pop, hashmap, arg);
			return 0;
		case HASHMAP_CMD_DEBUG:
			if (!arg)
				return -EINVAL;
			hm_tx_debug(pop, hashmap, (FILE *)arg);
			return 0;
		default:
			return -EINVAL;
	}
}
