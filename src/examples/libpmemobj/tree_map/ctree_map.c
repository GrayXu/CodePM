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

/*
 * ctree_map.c -- Crit-bit trie implementation
 */

#include <ex_common.h>
#include <assert.h>
#include <errno.h>
#include <stdlib.h>

#include "ctree_map.h"

#include <libpangolin.h>

#define BIT_IS_SET(n, i) (!!((n) & (1ULL << (i))))

TOID_DECLARE(struct tree_map_node, CTREE_MAP_TYPE_OFFSET + 1);

struct tree_map_entry {
	uint64_t key;
	PMEMoid slot;
};

struct tree_map_node {
	int diff; /* most significant differing bit */
	struct tree_map_entry entries[2];
};

struct ctree_map {
	struct tree_map_entry root;
};

/*
 * find_crit_bit -- (internal) finds the most significant differing bit
 */
static int
find_crit_bit(uint64_t lhs, uint64_t rhs)
{
	return find_last_set_64(lhs ^ rhs);
}

/*
 * ctree_map_create -- allocates a new crit-bit tree instance
 */
int
ctree_map_create(PMEMobjpool *pop, TOID(struct ctree_map) *map, void *arg)
{
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		/*
		 * We do not have to add *map to a transaction (again) because
		 * if map points to a pmem object, the target should reside in
		 * an objbuf that is created and committed by the caller.
		 */
		*map = PGL_TX_ZNEW(struct ctree_map);
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}

/*
 * ctree_map_clear_node -- (internal) clears this node and its children
 */
static void
ctree_map_clear_node(PMEMoid p)
{
	if (OID_IS_NULL(p))
		return;

	if (OID_INSTANCEOF(p, struct tree_map_node)) {
		struct tree_map_node *node = pgl_get(p);
		ctree_map_clear_node(node->entries[0].slot);
		ctree_map_clear_node(node->entries[1].slot);
	}

	pgl_tx_free(p);
}


/*
 * ctree_map_clear -- removes all elements from the map
 */
int
ctree_map_clear(PMEMobjpool *pop, TOID(struct ctree_map) map)
{
	PGL_TX_BEGIN(pop) {
		struct ctree_map *cmap = pgl_tx_open(map.oid);
		ctree_map_clear_node(cmap->root.slot);
		cmap->root.slot = OID_NULL;
	} PGL_TX_END

	return 0;
}

/*
 * ctree_map_destroy -- cleanups and frees crit-bit tree instance
 */
int
ctree_map_destroy(PMEMobjpool *pop, TOID(struct ctree_map) *map)
{
	int ret = 0;
	PGL_TX_BEGIN(pop) {
		ctree_map_clear(pop, *map);
		pgl_tx_free(map->oid);
		*map = TOID_NULL(struct ctree_map);
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}

/*
 * ctree_map_insert_leaf -- (internal) inserts a new leaf at the position
 */
static void
ctree_map_insert_leaf(struct tree_map_entry *p,
	struct tree_map_entry e, int diff)
{
	struct tree_map_node *new_node = pgl_tx_alloc_open(
		sizeof(struct tree_map_node),
		TOID_TYPE_NUM(struct tree_map_node));
	new_node->diff = diff;

	int d = BIT_IS_SET(e.key, new_node->diff);

	/* insert the leaf at the direction based on the critical bit */
	new_node->entries[d] = e;

	/* find the appropriate position in the tree to insert the node */
	struct tree_map_node *node = NULL, *next = NULL;
	while (OID_INSTANCEOF(p->slot, struct tree_map_node)) {

		next = pgl_get(p->slot);
		/* the critical bits have to be sorted */
		if (next->diff < new_node->diff)
			break;

		SWAP_PTR(node, next);
		p = &node->entries[BIT_IS_SET(e.key, node->diff)];
	}

	/* insert the found destination in the other slot */
	new_node->entries[!d] = *p;

	/*
	 * In CoW mode node was pointing to pmem, so was p. This step is to
	 * reset p to node's objbuf after it's added to transaction. It's not
	 * necessary in non-CoW mode.
	 */
	if (PTR_TO_OBJ(p, node, struct tree_map_node))
		p = PTR_TO(pgl_tx_add(node), DISP(node, p));

	p->key = 0;
	p->slot = pgl_oid(new_node);
}

/*
 * ctree_map_insert_new -- allocates a new object and inserts it into the tree
 */
int
ctree_map_insert_new(PMEMobjpool *pop, TOID(struct ctree_map) map,
		uint64_t key, size_t size, unsigned type_num,
		void (*constructor)(PMEMobjpool *pop, void *ptr, void *arg),
		void *arg)
{
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		void *pn = pgl_tx_alloc_open(size, type_num);
		constructor(pop, pn, arg);
		ctree_map_insert(pop, map, key, pgl_oid(pn));
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}


/*
 * ctree_map_insert -- inserts a new key-value pair into the map
 */
int
ctree_map_insert(PMEMobjpool *pop, TOID(struct ctree_map) map,
	uint64_t key, PMEMoid value)
{
	int ret = 0;
	struct ctree_map *cmap = pgl_get(map.oid);
	struct tree_map_entry *p = &cmap->root;

	/* descend the path until a best matching key is found */
	struct tree_map_node *node = NULL;
	while (!OID_IS_NULL(p->slot) &&
		OID_INSTANCEOF(p->slot, struct tree_map_node)) {
		node = pgl_get(p->slot);
		p = &node->entries[BIT_IS_SET(key, node->diff)];
	}

	struct tree_map_entry e = {key, value};
	PGL_TX_BEGIN(pop) {
		cmap = pgl_tx_add(cmap);

		if (PTR_TO_OBJ(p, node, struct tree_map_node))
			p = PTR_TO(pgl_tx_add(node), DISP(node, p));
		else
			p = &cmap->root;

		if (p->key == 0 || p->key == key) {
			*p = e;
		} else {
			ctree_map_insert_leaf(&cmap->root, e,
					find_crit_bit(p->key, key));
		}
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}

/*
 * ctree_map_get_leaf -- (internal) searches for a leaf of the key
 */
static struct tree_map_entry *
ctree_map_get_leaf(struct ctree_map *map, uint64_t key,
	struct tree_map_entry **parent, struct tree_map_node **retnode,
	struct tree_map_node **retparent)
{
	struct tree_map_entry *n = &map->root;
	struct tree_map_entry *p = NULL;

	struct tree_map_node *node = NULL, *parentnode = NULL;
	while (!OID_IS_NULL(n->slot) &&
				OID_INSTANCEOF(n->slot, struct tree_map_node)) {

		SWAP_PTR(node, parentnode);
		p = n;

		node = pgl_get(n->slot);
		n = &node->entries[BIT_IS_SET(key, node->diff)];
	}

	if (n->key == key) {
		if (parent)
			*parent = p;

		/* must provide receivers so as to close them after use */
		*retnode = node;
		*retparent = parentnode;

		return n;
	}

	return NULL;
}

/*
 * ctree_map_remove_free -- removes and frees an object from the tree
 */
int
ctree_map_remove_free(PMEMobjpool *pop, TOID(struct ctree_map) map,
		uint64_t key)
{
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		PMEMoid val = ctree_map_remove(pop, map, key);
		pgl_tx_free(val);
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}

/*
 * ctree_map_remove -- removes key-value pair from the map
 */
PMEMoid
ctree_map_remove(PMEMobjpool *pop, TOID(struct ctree_map) map, uint64_t key)
{
	struct tree_map_entry *parent = NULL;
	struct ctree_map *cmap = pgl_get(map.oid);
	struct tree_map_node *leafnode = NULL, *parentnode = NULL;
	struct tree_map_entry *leaf = ctree_map_get_leaf(cmap, key, &parent,
							&leafnode, &parentnode);

	if (leaf == NULL)
		return OID_NULL;

	PMEMoid ret = leaf->slot;

	if (parent == NULL) { /* root */
		/* only modifying leaf, parent and its node not needed */
		PGL_TX_BEGIN(pop) {
			if (PTR_TO_OBJ(leaf, leafnode, struct tree_map_node)) {
				leaf = PTR_TO(pgl_tx_add(leafnode),
						DISP(leafnode, leaf));
			} else {
				cmap = pgl_tx_add(cmap);
				leaf = &cmap->root;
			}

			/*
			 * OBUF-OPT: Struct ctree_map only has a tree_map_entry.
			 * The leaf pointer returned by ctree_map_get_leaf() can
			 * point to ctree_map if that is the only thing in the
			 * tree, but then leafnode is NULL since ctree_map is
			 * not a tree_map_node. Thus we have the if branch to
			 * add map to transaction because the following leaf->
			 * operations modify it.
			 *
			 * The same principle applies to the parent pointer.
			 */
			leaf->key = 0;
			leaf->slot = OID_NULL;
		} PGL_TX_END
	} else {
		/*
		 * In this situation:
		 *	 parent
		 *	/     \
		 *   LEFT   RIGHT
		 * there's no point in leaving the parent internal node
		 * so it's swapped with the remaining node and then also freed.
		 */
		PGL_TX_BEGIN(pop) {
			if (PTR_TO_OBJ(parent, parentnode,
					struct tree_map_node)) {
				parent = PTR_TO(pgl_tx_add(parentnode),
						DISP(parentnode, parent));
			} else {
				cmap = pgl_tx_add(cmap);
				parent = &cmap->root;
			}

			struct tree_map_entry *dest = parent;

			PMEMoid noid = parent->slot;
			struct tree_map_node *node = pgl_get(noid);
			*dest = node->entries[
				node->entries[0].key == leaf->key];
			/*
			 * Need to close the obejct if it was opened in unque
			 * mode and not added to a transaction.
			 */
			pgl_tx_free(noid);
		} PGL_TX_END
	}

	return ret;
}

/*
 * ctree_map_get -- searches for a value of the key
 */
PMEMoid
ctree_map_get(PMEMobjpool *pop, TOID(struct ctree_map) map, uint64_t key)
{
	struct ctree_map *cmap = pgl_get(map.oid);
	struct tree_map_node *leafnode = NULL, *parentnode = NULL;
	struct tree_map_entry *entry = ctree_map_get_leaf(cmap, key, NULL,
							&leafnode, &parentnode);

	return entry ? entry->slot : OID_NULL;
}

/*
 * ctree_map_lookup -- searches if a key exists
 */
int
ctree_map_lookup(PMEMobjpool *pop, TOID(struct ctree_map) map,
		uint64_t key)
{
	struct ctree_map *cmap = pgl_get(map.oid);
	struct tree_map_node *leafnode = NULL, *parentnode = NULL;
	struct tree_map_entry *entry = ctree_map_get_leaf(cmap, key, NULL,
							&leafnode, &parentnode);

	return entry != NULL;
}

/*
 * ctree_map_foreach_node -- (internal) recursively traverses tree
 */
static int
ctree_map_foreach_node(struct tree_map_entry e,
	int (*cb)(uint64_t key, PMEMoid value, void *arg), void *arg)
{
	int ret = 0;

	if (OID_INSTANCEOF(e.slot, struct tree_map_node)) {
		struct tree_map_node *node = pgl_get(e.slot);
		if (ctree_map_foreach_node(node->entries[0], cb, arg) == 0)
			ctree_map_foreach_node(node->entries[1], cb, arg);
	} else { /* leaf */
		ret = cb(e.key, e.slot, arg);
	}

	return ret;
}

/*
 * ctree_map_foreach -- initiates recursive traversal
 */
int
ctree_map_foreach(PMEMobjpool *pop, TOID(struct ctree_map) map,
	int (*cb)(uint64_t key, PMEMoid value, void *arg), void *arg)
{
	struct ctree_map *cmap = pgl_get(map.oid);
	if (OID_IS_NULL(cmap->root.slot))
		return 0;

	int ret = ctree_map_foreach_node(cmap->root, cb, arg);

	return ret;
}

/*
 * ctree_map_is_empty -- checks whether the tree map is empty
 */
int
ctree_map_is_empty(PMEMobjpool *pop, TOID(struct ctree_map) map)
{
	struct ctree_map *cmap = pgl_get(map.oid);
	int empty = cmap->root.key == 0;

	return empty;
}

/*
 * ctree_map_check -- check if given persistent object is a tree map
 */
int
ctree_map_check(PMEMobjpool *pop, TOID(struct ctree_map) map)
{
	return TOID_IS_NULL(map) || !TOID_VALID(map);
}
