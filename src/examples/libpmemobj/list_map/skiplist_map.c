/*
 * Copyright 2016, Intel Corporation
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
 * skiplist_map.c -- Skiplist implementation
 */

#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>

#include "skiplist_map.h"

#include <libpangolin.h>

#define SKIPLIST_LEVELS_NUM 24
#define NULL_NODE TOID_NULL(struct skiplist_map_node)

struct skiplist_map_entry {
	uint64_t key;
	PMEMoid value;
};

struct skiplist_map_node {
	TOID(struct skiplist_map_node) next[SKIPLIST_LEVELS_NUM];
	struct skiplist_map_entry entry;
};

/*
 * skiplist_map_create -- allocates a new skiplist instance
 */
int
skiplist_map_create(PMEMobjpool *pop, TOID(struct skiplist_map_node) *map,
	void *arg)
{
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		*map = (TOID(struct skiplist_map_node))pgl_tx_alloc(
				sizeof(struct skiplist_map_node),
				TOID_TYPE_NUM(struct skiplist_map_node));
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}

/*
 * skiplist_map_clear -- removes all elements from the map
 */
int
skiplist_map_clear(PMEMobjpool *pop, TOID(struct skiplist_map_node) map)
{
	struct skiplist_map_node *smap = pgl_get(map.oid), *pnext = NULL;

	while (!TOID_EQUALS(smap->next[0], NULL_NODE)) {
		pnext = pgl_get(smap->next[0].oid);
		skiplist_map_remove_free(pop, map, pnext->entry.key);
	}

	return 0;
}

/*
 * skiplist_map_destroy -- cleanups and frees skiplist instance
 */
int
skiplist_map_destroy(PMEMobjpool *pop, TOID(struct skiplist_map_node) *map)
{
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		skiplist_map_clear(pop, *map);
		pgl_tx_free(map->oid);
		*map = TOID_NULL(struct skiplist_map_node);
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}

/*
 * skiplist_map_insert_new -- allocates a new object and inserts it into
 * the list
 */
int
skiplist_map_insert_new(PMEMobjpool *pop, TOID(struct skiplist_map_node) map,
	uint64_t key, size_t size, unsigned type_num,
	void (*constructor)(PMEMobjpool *pop, void *ptr, void *arg),
	void *arg)
{
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		void *pn = pgl_tx_alloc_open(size, type_num);
		PMEMoid n = pgl_oid(pn);
		constructor(pop, pn, arg);
		skiplist_map_insert(pop, map, key, n);
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}

/*
 * skiplist_map_insert_node -- (internal) adds new node in selected place
 */

static void
skiplist_map_insert_node(struct skiplist_map_node *pnode,
	TOID(struct skiplist_map_node) path[SKIPLIST_LEVELS_NUM])
{
	unsigned current_level = 0;
	do {
		struct skiplist_map_node *pcurr =
			pgl_tx_open(path[current_level].oid);
		/*
		 * OBUF-OPT: Using pgl_tx_add_range() doesn't actually make
		 * much difference. Could the reason be creating small redo log
		 * entries?
		 *
		 * Comment out pgl_tx_add_range() to see the difference.
		 */
		pnode = pgl_tx_add_range(pnode,
			offsetof(struct skiplist_map_node,
				next[current_level]),
			sizeof(PMEMoid));

		pnode->next[current_level] = pcurr->next[current_level];

		pcurr = pgl_tx_add_range(pcurr,
			offsetof(struct skiplist_map_node,
				next[current_level]),
			sizeof(PMEMoid));

		pcurr->next[current_level] =
			(TOID(struct skiplist_map_node))pgl_oid(pnode);
	} while (++current_level < SKIPLIST_LEVELS_NUM && rand() % 2 == 0);
}

/*
 * skiplist_map_map_find -- (internal) returns path to searched node, or if
 * node doesn't exist, it will return path to place where key should be.
 */
static void
skiplist_map_find(uint64_t key, TOID(struct skiplist_map_node) map,
	TOID(struct skiplist_map_node) *path)
{
	int current_level;

	TOID(struct skiplist_map_node) active = map, next;
	struct skiplist_map_node *pactv = pgl_get(map.oid), *pnext = NULL;

	for (current_level = SKIPLIST_LEVELS_NUM - 1;
			current_level >= 0; current_level--) {
		next = pactv->next[current_level];
		pnext = pgl_get(next.oid);
		for (; !TOID_EQUALS(next, NULL_NODE) &&
				pnext->entry.key < key;
				pnext = pgl_get(next.oid)) {
			active = pactv->next[current_level];
			pactv = pgl_get(active.oid);
			next = pactv->next[current_level];
		}

		path[current_level] = active;
	}
}

/*
 * skiplist_map_insert -- inserts a new key-value pair into the map
 */
int
skiplist_map_insert(PMEMobjpool *pop, TOID(struct skiplist_map_node) map,
	uint64_t key, PMEMoid value)
{
	int ret = 0;
	TOID(struct skiplist_map_node) path[SKIPLIST_LEVELS_NUM];

	PGL_TX_BEGIN(pop) {
		struct skiplist_map_node *pnode =
			pgl_tx_alloc_open(sizeof(struct skiplist_map_node),
				TOID_TYPE_NUM(struct skiplist_map_node));

		pnode = pgl_tx_add_range(pnode,
			offsetof(struct skiplist_map_node, entry),
			sizeof(struct skiplist_map_entry));

		pnode->entry.key = key;
		pnode->entry.value = value;

		skiplist_map_find(key, map, path);
		skiplist_map_insert_node(pnode, path);
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}

/*
 * skiplist_map_remove_free -- removes and frees an object from the list
 */
int
skiplist_map_remove_free(PMEMobjpool *pop, TOID(struct skiplist_map_node) map,
	uint64_t key)
{
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		PMEMoid val = skiplist_map_remove(pop, map, key);
		pgl_tx_free(val);
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}

/*
 * skiplist_map_remove_node -- (internal) removes selected node
 */
static void
skiplist_map_remove_node(
	TOID(struct skiplist_map_node) path[SKIPLIST_LEVELS_NUM])
{
	struct skiplist_map_node *ppath = pgl_get(path[0].oid);
	TOID(struct skiplist_map_node) to_remove = ppath->next[0];
	struct skiplist_map_node *premove = pgl_get(to_remove.oid);

	for (int i = 0; i < SKIPLIST_LEVELS_NUM; i++) {
		ppath = pgl_get(path[i].oid);
		if (TOID_EQUALS(ppath->next[i], to_remove)) {

		/*	ppath = pgl_tx_add(ppath); */
			ppath = pgl_tx_add_range(ppath,
				offsetof(struct skiplist_map_node,
					next[i]),
				sizeof(PMEMoid));

			ppath->next[i] = premove->next[i];
		}
	}
}

/*
 * skiplist_map_remove -- removes key-value pair from the map
 */
PMEMoid
skiplist_map_remove(PMEMobjpool *pop, TOID(struct skiplist_map_node) map,
	uint64_t key)
{
	PMEMoid ret = OID_NULL;
	TOID(struct skiplist_map_node) path[SKIPLIST_LEVELS_NUM];

	skiplist_map_find(key, map, path); /* no need to be in tx */
	struct skiplist_map_node *ppath = pgl_get(path[0].oid);
	TOID(struct skiplist_map_node) to_remove = ppath->next[0];
	struct skiplist_map_node *premove = pgl_get(to_remove.oid);

	PGL_TX_BEGIN(pop) {
		if (!TOID_EQUALS(to_remove, NULL_NODE) &&
			premove->entry.key == key) {
			ret = premove->entry.value;
			skiplist_map_remove_node(path);
		}
	} PGL_TX_ONABORT {
		ret = OID_NULL;
	} PGL_TX_END

	return ret;
}

/*
 * skiplist_map_get -- searches for a value of the key
 */
PMEMoid
skiplist_map_get(PMEMobjpool *pop, TOID(struct skiplist_map_node) map,
	uint64_t key)
{
	PMEMoid ret = OID_NULL;
	TOID(struct skiplist_map_node) path[SKIPLIST_LEVELS_NUM], found;
	skiplist_map_find(key, map, path);

	struct skiplist_map_node *ppath = pgl_get(path[0].oid);
	found = ppath->next[0];
	struct skiplist_map_node *pfound = pgl_get(found.oid);

	if (!TOID_EQUALS(found, NULL_NODE) && pfound->entry.key == key) {
		ret = pfound->entry.value;
	}

	return ret;
}

/*
 * skiplist_map_lookup -- searches if a key exists
 */
int
skiplist_map_lookup(PMEMobjpool *pop, TOID(struct skiplist_map_node) map,
	uint64_t key)
{
	int ret = 0;
	TOID(struct skiplist_map_node) path[SKIPLIST_LEVELS_NUM], found;

	skiplist_map_find(key, map, path);

	struct skiplist_map_node *ppath = pgl_get(path[0].oid);
	found = ppath->next[0];
	struct skiplist_map_node *pfound = pgl_get(found.oid);

	if (!TOID_EQUALS(found, NULL_NODE) && pfound->entry.key == key)
		ret = 1;

	return ret;
}

/*
 * skiplist_map_foreach -- calls function for each node on a list
 */
int
skiplist_map_foreach(PMEMobjpool *pop, TOID(struct skiplist_map_node) map,
	int (*cb)(uint64_t key, PMEMoid value, void *arg), void *arg)
{
	struct skiplist_map_node *pnext = pgl_get(map.oid);

	while (!TOID_EQUALS(pnext->next[0], NULL_NODE)) {
		pnext = pgl_get(pnext->next[0].oid);
		cb(pnext->entry.key, pnext->entry.value, arg);
	}

	return 0;
}

/*
 * skiplist_map_is_empty -- checks whether the list map is empty
 */
int
skiplist_map_is_empty(PMEMobjpool *pop, TOID(struct skiplist_map_node) map)
{
	struct skiplist_map_node *smap = pgl_get(map.oid);
	int empty = TOID_IS_NULL(smap->next[0]);

	return empty;
}

/*
 * skiplist_map_check -- check if given persistent object is a skiplist
 */
int
skiplist_map_check(PMEMobjpool *pop, TOID(struct skiplist_map_node) map)
{
	return TOID_IS_NULL(map) || !TOID_VALID(map);
}
