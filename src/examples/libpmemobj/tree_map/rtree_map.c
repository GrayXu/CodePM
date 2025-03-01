/*
 * Copyright 2016-2017, Intel Corporation
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
 * rtree_map.c -- implementation of rtree
 */

#include <ex_common.h>
#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <stdbool.h>

#include "rtree_map.h"

#include <libpangolin.h>

TOID_DECLARE(struct tree_map_node, RTREE_MAP_TYPE_OFFSET + 1);

/* Good values: 0x10 an 0x100, but implementation is bound to 0x100 */
#ifndef ALPHABET_SIZE
#define ALPHABET_SIZE 0x100
#endif

struct tree_map_node {
	TOID(struct tree_map_node) slots[ALPHABET_SIZE];
	unsigned has_value;
	PMEMoid value;
	uint64_t key_size;
	unsigned char key[];
};

struct rtree_map {
	TOID(struct tree_map_node) root;
};

/*
 * rtree_map_create -- allocates a new rtree instance
 */
int
rtree_map_create(PMEMobjpool *pop, TOID(struct rtree_map) *map, void *arg)
{
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		*map = (TOID(struct rtree_map))pgl_tx_alloc(
				sizeof(struct rtree_map),
				TOID_TYPE_NUM(struct rtree_map));
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}

/*
 * rtree_map_clear_node -- (internal) removes all elements from the node
 */
static void
rtree_map_clear_node(TOID(struct tree_map_node) node)
{
	struct tree_map_node *pnode = pgl_tx_open(node.oid);
	for (unsigned i = 0; i < ALPHABET_SIZE; i++) {
		rtree_map_clear_node(pnode->slots[i]);
	}

	/* why does pmdk add a range that is to be deleted? */

	pgl_tx_free(node.oid);
}

/*
 * rtree_map_clear -- removes all elements from the map
 */
int
rtree_map_clear(PMEMobjpool *pop, TOID(struct rtree_map) map)
{
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		struct rtree_map *rmap = pgl_tx_open(map.oid);

		rtree_map_clear_node(rmap->root);

		rmap->root = TOID_NULL(struct tree_map_node);
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}


/*
 * rtree_map_destroy -- cleanups and frees rtree instance
 */
int
rtree_map_destroy(PMEMobjpool *pop, TOID(struct rtree_map) *map)
{
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		rtree_map_clear(pop, *map);
		pgl_tx_free(map->oid);
		*map = TOID_NULL(struct rtree_map);
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}

/*
 * rtree_new_node -- (internal) inserts a node into an empty map
 */
static TOID(struct tree_map_node)
rtree_new_node(const unsigned char *key, uint64_t key_size,
		PMEMoid value, unsigned has_value)
{
	TOID(struct tree_map_node) node;

	struct tree_map_node *pnode = pgl_tx_alloc_open(
			sizeof(struct tree_map_node) + key_size,
			TOID_TYPE_NUM(struct tree_map_node));

	pnode = pgl_tx_add_range(pnode,
			offsetof(struct tree_map_node, has_value),
			sizeof(unsigned) + sizeof(PMEMoid) + sizeof(uint64_t)
			+ key_size);

	pnode->value = value;
	pnode->has_value = has_value;
	pnode->key_size = key_size;

	memcpy(pnode->key, key, key_size);

	node = (TOID(struct tree_map_node))pgl_oid(pnode);

	return node;
}
/*
 * rtree_map_insert_empty -- (internal) inserts a node into an empty map
 */
static void
rtree_map_insert_empty(TOID(struct rtree_map) map,
	const unsigned char *key, uint64_t key_size, PMEMoid value)
{
	struct rtree_map *rmap = pgl_tx_open(map.oid);
	rmap->root = rtree_new_node(key, key_size, value, 1);
}

/*
 * key_comm_len -- (internal) calculate the len of common part of keys
 */
static unsigned
key_comm_len(struct tree_map_node *pnode,
	const unsigned char *key, uint64_t key_size)
{
	unsigned i;

	for (i = 0;
		i < MIN(key_size, pnode->key_size) &&
			key[i] == pnode->key[i];
		i++)
		;

	return i;
}

/*
 * rtree_map_insert_value -- (internal) inserts a pair into a tree
 */
static void
rtree_map_insert_value(TOID(struct tree_map_node) *node,
	const unsigned char *key, uint64_t key_size, PMEMoid value)
{
	unsigned i;

	if (TOID_IS_NULL(*node)) {
		*node = rtree_new_node(key, key_size, value, 1);
		return;
	}

	struct tree_map_node *pnode = pgl_get(node->oid);

	i = key_comm_len(pnode, key, key_size);

	if (i != pnode->key_size) {
		/* Node does not exist. Let's add. */
		struct tree_map_node *porig = pnode;
		TOID(struct tree_map_node) orig_node = *node;

		if (i != key_size) {
			*node = rtree_new_node(porig->key, i, OID_NULL, 0);
		} else {
			*node = rtree_new_node(porig->key, i, value, 1);
		}

		porig = pgl_tx_add(porig); /* will modify porig data */
		pnode = pgl_tx_open(node->oid); /* node was re-assigned */

		pnode = pgl_tx_add_range(pnode,
				offsetof(struct tree_map_node,
					slots[porig->key[i]]), sizeof(PMEMoid));
		pnode->slots[porig->key[i]] = orig_node;

		porig = pgl_tx_add_range(porig,
				offsetof(struct tree_map_node, key_size),
				sizeof(uint64_t) + porig->key_size);
		porig->key_size -= i;
		memmove(porig->key, porig->key + i, porig->key_size);

		if (i != key_size) {
			pnode = pgl_tx_add_range(pnode,
					offsetof(struct tree_map_node,
						slots[key[i]]),
					sizeof(PMEMoid));
			pnode->slots[key[i]] =
				rtree_new_node(key + i, key_size - i, value, 1);
		}

		return;
	}

	if (i == key_size) {
		if (OID_IS_NULL(pnode->value) || pnode->has_value) {
			/* for has_value and value */
			pnode = pgl_tx_add_range(pnode,
					offsetof(struct tree_map_node,
						has_value),
					sizeof(unsigned) + sizeof(PMEMoid));

			/* Just replace old value with new */
			pnode->value = value;
			pnode->has_value = 1;
		} else {
			/*
			 *  Ignore. By the fact current value should be
			 *  removed in advance, or handled in a different way.
			 */
		}
	} else {
		/* Recurse deeply */
		pnode = pgl_tx_add_range(pnode,
				offsetof(struct tree_map_node, slots[key[i]]),
				sizeof(PMEMoid));
		return rtree_map_insert_value(&pnode->slots[key[i]],
				key + i, key_size - i, value);
	}
}

/*
 * rtree_map_is_empty -- checks whether the tree map is empty
 */
int
rtree_map_is_empty(PMEMobjpool *pop, TOID(struct rtree_map) map)
{
	struct rtree_map *rmap = pgl_get(map.oid);
	TOID(struct tree_map_node) root = rmap->root;

	return TOID_IS_NULL(root);
}

/*
 * rtree_map_insert -- inserts a new key-value pair into the map
 */
int
rtree_map_insert(PMEMobjpool *pop, TOID(struct rtree_map) map,
	const unsigned char *key, uint64_t key_size, PMEMoid value)
{
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		if (rtree_map_is_empty(pop, map)) {
			rtree_map_insert_empty(map, key, key_size, value);
		} else {
			struct rtree_map *rmap = pgl_tx_open(map.oid);
			rtree_map_insert_value(&rmap->root,
					key, key_size, value);
		}
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}

/*
 * rtree_map_insert_new -- allocates a new object and inserts it into the tree
 */
int
rtree_map_insert_new(PMEMobjpool *pop, TOID(struct rtree_map) map,
		const unsigned char *key, uint64_t key_size,
		size_t size, unsigned type_num,
		void (*constructor)(PMEMobjpool *pop, void *ptr, void *arg),
		void *arg)
{
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		void *pn = pgl_tx_alloc_open(size, type_num);
		PMEMoid n = pgl_oid(pn);
		constructor(pop, pn, arg);
		rtree_map_insert(pop, map, key, key_size, n);
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}

/*
 * is_leaf -- (internal) check a node for zero qty of children
 */
static bool
is_leaf(struct tree_map_node *pnode)
{
	unsigned j;

	for (j = 0; j < ALPHABET_SIZE && TOID_IS_NULL(pnode->slots[j]); j++);

	return (j == ALPHABET_SIZE);
}

/*
 * has_only_one_child -- (internal) check a node for qty of children
 */
static bool
has_only_one_child(struct tree_map_node *pnode, unsigned *child_idx)
{
	unsigned j, child_qty;

	for (j = 0, child_qty = 0; j < ALPHABET_SIZE; j++)
		if (!TOID_IS_NULL(pnode->slots[j])) {
			child_qty++;
			*child_idx = j;
		}

	return (1 == child_qty);
}

/*
 * remove_extra_node -- (internal) remove unneeded extra node
 */
static void
remove_extra_node(TOID(struct tree_map_node) *node)
{
	unsigned child_idx;
	TOID(struct tree_map_node) tmp, tmp_child;

	/* Our node has child with only one child. */
	tmp = *node;
	struct tree_map_node *ptmp = pgl_get(node->oid);

	has_only_one_child(ptmp, &child_idx);
	tmp_child = ptmp->slots[child_idx];

	struct tree_map_node *pchild = pgl_get(tmp_child.oid);

	/*
	 * That child's incoming label is appended to the ours incoming label
	 * and the child is removed.
	 */
	uint64_t new_key_size = ptmp->key_size + pchild->key_size;
	unsigned char *new_key = (unsigned char *)malloc(new_key_size);
	assert(new_key != NULL);
	memcpy(new_key, ptmp->key, ptmp->key_size);
	memcpy(new_key + ptmp->key_size, pchild->key, pchild->key_size);

	*node = rtree_new_node(new_key, new_key_size,
			pchild->value, pchild->has_value);

	/* node was tx_allocated in rtree_new_node() */
	struct tree_map_node *pnode = pgl_tx_open(node->oid);

	free(new_key);

	pgl_tx_free(tmp.oid);

	pnode = pgl_tx_add_range(pnode,
			offsetof(struct tree_map_node, slots),
			sizeof(pchild->slots));

	memcpy(pnode->slots, pchild->slots, sizeof(pchild->slots));

	pgl_tx_free(tmp_child.oid);
}

/*
 * rtree_map_remove_node -- (internal) removes node from tree
 */
static PMEMoid
rtree_map_remove_node(TOID(struct rtree_map) map,
	TOID(struct tree_map_node) *node,
	const unsigned char *key, uint64_t key_size,
	bool *check_for_child)
{
	bool c4c;
	unsigned i, child_idx;
	PMEMoid ret = OID_NULL;

	*check_for_child = false;

	if (TOID_IS_NULL(*node))
		return OID_NULL;

	struct tree_map_node *pnode = pgl_get(node->oid);

	i = key_comm_len(pnode, key, key_size);

	if (i != pnode->key_size)
		/* Node does not exist */
		return OID_NULL;

	if (i == key_size) {
		if (0 == pnode->has_value)
			return OID_NULL;

		/* Node is found */
		ret = pnode->value;

		pnode = pgl_tx_add_range(pnode,
				offsetof(struct tree_map_node, has_value),
				sizeof(unsigned) + sizeof(PMEMoid));

		/* delete node from tree */
		pnode->value = OID_NULL;
		pnode->has_value = 0;

		if (is_leaf(pnode)) {
			pgl_tx_free(node->oid);
			(*node) = TOID_NULL(struct tree_map_node);
		}

		return ret;
	}

	/* Recurse deeply */
	pnode = pgl_tx_add_range(pnode,
			offsetof(struct tree_map_node, slots[key[i]]),
			sizeof(PMEMoid));
	ret = rtree_map_remove_node(map,
			&pnode->slots[key[i]],
			key + i, key_size - i,
			&c4c);

	if (c4c) {
		/* Our node has child with only one child. Remove. */
		remove_extra_node(&pnode->slots[key[i]]);

		return ret;
	}

	if (has_only_one_child(pnode, &child_idx) && (0 == pnode->has_value)) {
		*check_for_child = true;
	}

	return ret;
}

/*
 * rtree_map_remove -- removes key-value pair from the map
 */
PMEMoid
rtree_map_remove(PMEMobjpool *pop, TOID(struct rtree_map) map,
		const unsigned char *key, uint64_t key_size)
{
	PMEMoid ret = OID_NULL;
	bool check_for_child;

	if (TOID_IS_NULL(map))
		return OID_NULL;

	PGL_TX_BEGIN(pop) {
		struct rtree_map *rmap = pgl_tx_open(map.oid);
		ret = rtree_map_remove_node(map,
				&rmap->root, key, key_size,
				&check_for_child);

		if (check_for_child) {
			/* Our root node has only one child. Remove. */
			remove_extra_node(&rmap->root);
		}
	} PGL_TX_END

	return ret;
}

/*
 * rtree_map_remove_free -- removes and frees an object from the tree
 */
int
rtree_map_remove_free(PMEMobjpool *pop, TOID(struct rtree_map) map,
		const unsigned char *key, uint64_t key_size)
{
	int ret = 0;

	if (TOID_IS_NULL(map))
		return 1;

	PGL_TX_BEGIN(pop) {
		pmemobj_tx_free(rtree_map_remove(pop, map, key, key_size));
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}

/*
 * rtree_map_get_in_node -- (internal) searches for a value in the node
 */
static PMEMoid
rtree_map_get_in_node(TOID(struct tree_map_node) node,
		const unsigned char *key, uint64_t key_size)
{
	unsigned i;

	if (TOID_IS_NULL(node))
		return OID_NULL;

	struct tree_map_node *pnode = pgl_get(node.oid);

	i = key_comm_len(pnode, key, key_size);

	if (i != pnode->key_size)
		/* Node does not exist */
		return OID_NULL;

	if (i == key_size) {
		/* Node is found */
		PMEMoid value = pnode->value;
		return value;
	} else {
		/* Recurse deeply */
		TOID(struct tree_map_node) slot = pnode->slots[key[i]];
		return rtree_map_get_in_node(slot, key + i, key_size - i);
	}
}

/*
 * rtree_map_get -- searches for a value of the key
 */
PMEMoid
rtree_map_get(PMEMobjpool *pop, TOID(struct rtree_map) map,
		const unsigned char *key, uint64_t key_size)
{
	struct rtree_map *rmap = pgl_get(map.oid);
	if (TOID_IS_NULL(rmap->root))
		return OID_NULL;

	PMEMoid retoid = rtree_map_get_in_node(rmap->root, key, key_size);

	return retoid;
}

/*
 * rtree_map_lookup_in_node -- (internal) searches for key if exists
 */
static int
rtree_map_lookup_in_node(TOID(struct tree_map_node) node,
		const unsigned char *key, uint64_t key_size)
{
	unsigned i;

	if (TOID_IS_NULL(node))
		return 0;

	struct tree_map_node *pnode = pgl_get(node.oid);

	i = key_comm_len(pnode, key, key_size);

	if (i != pnode->key_size)
		/* Node does not exist */
		return 0;

	if (i == key_size)
		/* Node is found */
		return 1;

	TOID(struct tree_map_node) slot = pnode->slots[key[i]];

	/* Recurse deeply */
	return rtree_map_lookup_in_node(slot, key + i, key_size - i);
}

/*
 * rtree_map_lookup -- searches if key exists
 */
int
rtree_map_lookup(PMEMobjpool *pop, TOID(struct rtree_map) map,
		const unsigned char *key, uint64_t key_size)
{
	struct rtree_map *rmap = pgl_get(map.oid);

	if (TOID_IS_NULL(rmap->root))
		return 0;

	int found = rtree_map_lookup_in_node(rmap->root, key, key_size);

	return found;
}

/*
 * rtree_map_foreach_node -- (internal) recursively traverses tree
 */
static int
rtree_map_foreach_node(const TOID(struct tree_map_node) node,
	int (*cb)(const unsigned char *key, uint64_t key_size,
			PMEMoid, void *arg),
	void *arg)
{
	unsigned i;

	if (TOID_IS_NULL(node))
		return 0;

	struct tree_map_node *pnode = pgl_get(node.oid);

	for (i = 0; i < ALPHABET_SIZE; i++) {
		if (rtree_map_foreach_node(pnode->slots[i], cb, arg) != 0)
			return 1;
	}

	if (NULL != cb) {
		if (cb(pnode->key, pnode->key_size, pnode->value, arg) != 0)
			return 1;
	}

	return 0;
}

/*
 * rtree_map_foreach -- initiates recursive traversal
 */
int
rtree_map_foreach(PMEMobjpool *pop, TOID(struct rtree_map) map,
	int (*cb)(const unsigned char *key, uint64_t key_size,
			PMEMoid value, void *arg),
	void *arg)
{
	struct rtree_map *rmap = pgl_get(map.oid);
	TOID(struct tree_map_node) root = rmap->root;

	return rtree_map_foreach_node(root, cb, arg);
}

/*
 * ctree_map_check -- check if given persistent object is a tree map
 */
int
rtree_map_check(PMEMobjpool *pop, TOID(struct rtree_map) map)
{
	return TOID_IS_NULL(map) || !TOID_VALID(map);
}
