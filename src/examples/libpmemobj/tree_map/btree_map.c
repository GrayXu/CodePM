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
 * btree_map.c -- textbook implementation of btree /w preemptive splitting
 */

#include <assert.h>
#include <errno.h>
#include <stdio.h>

#include "btree_map.h"

#include <libpangolin.h>

TOID_DECLARE(struct tree_map_node, BTREE_MAP_TYPE_OFFSET + 1);

#define BTREE_ORDER 8 /* can't be odd */
#define BTREE_MIN ((BTREE_ORDER / 2) - 1) /* min number of keys per node */

struct tree_map_node_item {
	uint64_t key;
	PMEMoid value;
};


struct tree_map_node {
	int n; /* number of occupied slots */
	struct tree_map_node_item items[BTREE_ORDER - 1];
	TOID(struct tree_map_node) slots[BTREE_ORDER];
};

struct btree_map {
	TOID(struct tree_map_node) root;
};

/*
 * set_empty_item -- (internal) sets null to the item
 */
static void
set_empty_item(struct tree_map_node_item *item)
{
	item->key = 0;
	item->value = OID_NULL;
}

/*
 * btree_map_create -- allocates a new btree instance
 */
int
btree_map_create(PMEMobjpool *pop, TOID(struct btree_map) *map, void *arg)
{
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		/*
		 * The object pointed by map should be added to transaction by
		 * the caller.
		 */
		*map = (TOID(struct btree_map))pgl_tx_alloc(
					sizeof(struct btree_map),
					TOID_TYPE_NUM(struct btree_map));
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}

/*
 * btree_map_clear_node -- (internal) removes all elements from the node
 */
static void
btree_map_clear_node(TOID(struct tree_map_node) node)
{
	struct tree_map_node *pnode = pgl_get(node.oid);
	for (int i = 0; i < pnode->n; ++i) {
		btree_map_clear_node(pnode->slots[i]);
	}

	pgl_tx_free(node.oid);
}


/*
 * btree_map_clear -- removes all elements from the map
 */
int
btree_map_clear(PMEMobjpool *pop, TOID(struct btree_map) map)
{
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		struct btree_map *bmap = pgl_tx_open(map.oid);

		btree_map_clear_node(bmap->root);

		bmap->root = TOID_NULL(struct tree_map_node);
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}


/*
 * btree_map_destroy -- cleanups and frees btree instance
 */
int
btree_map_destroy(PMEMobjpool *pop, TOID(struct btree_map) *map)
{
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		btree_map_clear(pop, *map);
		pgl_tx_free(map->oid);
		*map = TOID_NULL(struct btree_map);
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}

/*
 * btree_map_insert_item_at -- (internal) inserts an item at position
 */
static void
btree_map_insert_item_at(struct tree_map_node *pnode, int pos,
	struct tree_map_node_item item)
{
	pnode->items[pos] = item;
	pnode->n += 1;
}

/*
 * btree_map_insert_empty -- (internal) inserts an item into an empty node
 */
static void
btree_map_insert_empty(TOID(struct btree_map) map,
	struct tree_map_node_item item)
{
	struct btree_map *bmap = pgl_tx_open(map.oid);
	struct tree_map_node *proot =
		pgl_tx_alloc_open(sizeof(struct tree_map_node),
				TOID_TYPE_NUM(struct tree_map_node));
	bmap->root = (TOID(struct tree_map_node))pgl_oid(proot);

	btree_map_insert_item_at(proot, 0, item);
}

/*
 * btree_map_insert_node -- (internal) inserts and makes space for new node
 */
static void
btree_map_insert_node(TOID(struct tree_map_node) node, int p,
	struct tree_map_node_item item,
	TOID(struct tree_map_node) left, TOID(struct tree_map_node) right)
{
	struct tree_map_node *pnode = pgl_tx_open(node.oid);
	if (pnode->items[p].key != 0) { /* move all existing data */
		memmove(&pnode->items[p + 1], &pnode->items[p],
		sizeof(struct tree_map_node_item) * ((BTREE_ORDER - 2 - p)));

		memmove(&pnode->slots[p + 1], &pnode->slots[p],
		sizeof(TOID(struct tree_map_node)) * ((BTREE_ORDER - 1 - p)));
	}
	pnode->slots[p] = left;
	pnode->slots[p + 1] = right;
	btree_map_insert_item_at(pnode, p, item);
}

/*
 * btree_map_create_split_node -- (internal) splits a node into two
 */
static TOID(struct tree_map_node)
btree_map_create_split_node(TOID(struct tree_map_node) node,
	struct tree_map_node_item *m)
{
	struct tree_map_node *pright =
		pgl_tx_alloc_open(sizeof(struct tree_map_node),
			TOID_TYPE_NUM(struct tree_map_node));

	struct tree_map_node *pnode = pgl_tx_open(node.oid);

	int c = (BTREE_ORDER / 2);
	*m = pnode->items[c - 1]; /* select median item */
	set_empty_item(&pnode->items[c - 1]);

	/* move everything pright side of median to the new node */
	for (int i = c; i < BTREE_ORDER; ++i) {
		if (i != BTREE_ORDER - 1) {
			pright->items[pright->n++] = pnode->items[i];
			set_empty_item(&pnode->items[i]);
		}
		pright->slots[i - c] = pnode->slots[i];
		pnode->slots[i] = TOID_NULL(struct tree_map_node);
	}
	pnode->n = c - 1;

	return (TOID(struct tree_map_node))pgl_oid(pright);
}

/*
 * btree_map_find_dest_node -- (internal) finds a place to insert the new key at
 */
static TOID(struct tree_map_node)
btree_map_find_dest_node(TOID(struct btree_map) map,
	TOID(struct tree_map_node) n, TOID(struct tree_map_node) parent,
	uint64_t key, int *p)
{
	struct tree_map_node *pn = pgl_get(n.oid);
	if (pn->n == BTREE_ORDER - 1) { /* node is full, perform a split */
		struct tree_map_node_item m;
		TOID(struct tree_map_node) right =
			btree_map_create_split_node(n, &m);

		/* btree_map_create_split_node() can modify n's data */
		pn = pgl_get(n.oid);

		if (!TOID_IS_NULL(parent)) {
			btree_map_insert_node(parent, *p, m, n, right);
			if (key > m.key) { /* select node to continue search */
				n = right;
				pn = pgl_get(n.oid);
			}
		} else { /* replacing root node, the tree grows in height */
			struct tree_map_node *pup = pgl_tx_alloc_open(
					sizeof(struct tree_map_node),
					TOID_TYPE_NUM(struct tree_map_node));
			TOID(struct tree_map_node) up =
				(TOID(struct tree_map_node))pgl_oid(pup);

			pup->n = 1;
			pup->items[0] = m;
			pup->slots[0] = n;
			pup->slots[1] = right;

			struct btree_map *bmap = pgl_tx_open(map.oid);

			bmap->root = up;
			n = up;
			pn = pgl_get(n.oid);
		}
	}

	int i;
	TOID(struct tree_map_node) slot;
	for (i = 0; i < BTREE_ORDER - 1; ++i) {
		*p = i;
		slot = pn->slots[i];

		/*
		 * The key either fits somewhere in the middle or at the
		 * right edge of the node.
		 */
		if (pn->n == i || pn->items[i].key > key) {
			return TOID_IS_NULL(slot) ? n :
				btree_map_find_dest_node(map, slot, n, key, p);
		}
	}

	/*
	 * The key is bigger than the last node element, go one level deeper
	 * in the rightmost child.
	 */
	return btree_map_find_dest_node(map, slot, n, key, p);
}

/*
 * btree_map_insert_item -- (internal) inserts and makes space for new item
 */
static void
btree_map_insert_item(TOID(struct tree_map_node) node, int p,
	struct tree_map_node_item item)
{
	struct tree_map_node *pnode = pgl_tx_open(node.oid);
	if (pnode->items[p].key != 0) {
		memmove(&pnode->items[p + 1], &pnode->items[p],
		sizeof(struct tree_map_node_item) * ((BTREE_ORDER - 2 - p)));
	}
	btree_map_insert_item_at(pnode, p, item);
}

/*
 * btree_map_is_empty -- checks whether the tree map is empty
 */
int
btree_map_is_empty(PMEMobjpool *pop, TOID(struct btree_map) map)
{
	struct btree_map *bmap = pgl_get(map.oid);
	struct tree_map_node *proot = pgl_get(bmap->root.oid);

	int empty = TOID_IS_NULL(bmap->root) || proot->n == 0;

	return empty;
}

/*
 * btree_map_insert -- inserts a new key-value pair into the map
 */
int
btree_map_insert(PMEMobjpool *pop, TOID(struct btree_map) map,
	uint64_t key, PMEMoid value)
{
	struct tree_map_node_item item = {key, value};
	struct btree_map *bmap = pgl_get(map.oid);

	PGL_TX_BEGIN(pop) {
		if (btree_map_is_empty(pop, map)) {
			btree_map_insert_empty(map, item);
		} else {
			int p; /* position at the dest node to insert */
			TOID(struct tree_map_node) parent =
				TOID_NULL(struct tree_map_node);
			TOID(struct tree_map_node) dest =
				btree_map_find_dest_node(map, bmap->root,
					parent, key, &p);

			btree_map_insert_item(dest, p, item);
		}
	} PGL_TX_END

	return 0;
}

/*
 * btree_map_rotate_right -- (internal) takes one element from right sibling
 */
static void
btree_map_rotate_right(TOID(struct tree_map_node) rsb,
	TOID(struct tree_map_node) node,
	TOID(struct tree_map_node) parent, int p)
{
	struct tree_map_node *pnode = pgl_tx_open(node.oid);
	struct tree_map_node *ppa = pgl_tx_open(parent.oid);
	struct tree_map_node *prsb = pgl_tx_open(rsb.oid);

	/* move the separator from parent to the deficient node */
	struct tree_map_node_item sep = ppa->items[p];
	btree_map_insert_item(node, pnode->n, sep);

	/* the first element of the right sibling is the new separator */
	ppa->items[p] = prsb->items[0];

	/* the nodes are not necessarily leafs, so copy also the slot */
	pnode->slots[pnode->n] = prsb->slots[0];

	prsb->n -= 1; /* it loses one element, but still > min */

	/* move all existing elements back by one array slot */
	memmove(prsb->items, prsb->items + 1,
		sizeof(struct tree_map_node_item) * (prsb->n));
	memmove(prsb->slots, prsb->slots + 1,
		sizeof(TOID(struct tree_map_node)) * (prsb->n + 1));
}

/*
 * btree_map_rotate_left -- (internal) takes one element from left sibling
 */
static void
btree_map_rotate_left(TOID(struct tree_map_node) lsb,
	TOID(struct tree_map_node) node,
	TOID(struct tree_map_node) parent, int p)
{
	struct tree_map_node *pnode = pgl_tx_open(node.oid);
	struct tree_map_node *ppa = pgl_tx_open(parent.oid);
	struct tree_map_node *plsb = pgl_tx_open(lsb.oid);

	/* move the separator from parent to the deficient node */
	struct tree_map_node_item sep = ppa->items[p - 1];
	btree_map_insert_item(node, 0, sep);

	/* the last element of the left sibling is the new separator */
	ppa->items[p - 1] = plsb->items[plsb->n - 1];

	/* rotate the node children */
	memmove(pnode->slots + 1, pnode->slots,
		sizeof(TOID(struct tree_map_node)) * (pnode->n));

	/* the nodes are not necessarily leafs, so copy also the slot */
	pnode->slots[0] = plsb->slots[plsb->n];

	plsb->n -= 1; /* it loses one element, but still > min */
}

/*
 * btree_map_merge -- (internal) merges node and right sibling
 */
static void
btree_map_merge(TOID(struct btree_map) map, TOID(struct tree_map_node) rn,
	TOID(struct tree_map_node) node,
	TOID(struct tree_map_node) parent, int p)
{
	struct tree_map_node *pnode = pgl_tx_open(node.oid);
	struct tree_map_node *ppa = pgl_tx_open(parent.oid);

	struct tree_map_node_item sep = ppa->items[p];

	/* add separator to the deficient node */
	pnode->items[pnode->n++] = sep;

	struct tree_map_node *prn = pgl_get(rn.oid);

	/* copy right sibling data to node */
	memcpy(&pnode->items[pnode->n], prn->items,
	sizeof(struct tree_map_node_item) * prn->n);
	memcpy(&pnode->slots[pnode->n], prn->slots,
	sizeof(TOID(struct tree_map_node)) * (prn->n + 1));

	pnode->n += prn->n;

	pgl_tx_free(rn.oid); /* right node is now empty */

	ppa->n -= 1;

	/* move everything to the right of the separator by one array slot */
	memmove(ppa->items + p, ppa->items + p + 1,
	sizeof(struct tree_map_node_item) * (ppa->n - p));

	memmove(ppa->slots + p + 1, ppa->slots + p + 2,
	sizeof(TOID(struct tree_map_node)) * (ppa->n - p + 1));

	/* if the parent is empty then the tree shrinks in height */
	struct btree_map *bmap = pgl_get(map.oid);
	if (ppa->n == 0 && TOID_EQUALS(parent, bmap->root)) {
		bmap = pgl_tx_add(bmap);
		pgl_tx_free(bmap->root.oid);
		bmap->root = node;
	}
}

/*
 * btree_map_rebalance -- (internal) performs tree rebalance
 */
static void
btree_map_rebalance(TOID(struct btree_map) map, TOID(struct tree_map_node) node,
	TOID(struct tree_map_node) parent, int p)
{
	struct tree_map_node *ppa = pgl_get(parent.oid);

	TOID(struct tree_map_node) rsb = p >= ppa->n ?
		TOID_NULL(struct tree_map_node) : ppa->slots[p + 1];
	TOID(struct tree_map_node) lsb = p == 0 ?
		TOID_NULL(struct tree_map_node) : ppa->slots[p - 1];

	struct tree_map_node *prsb = pgl_get(rsb.oid);
	struct tree_map_node *plsb = pgl_get(lsb.oid);

	if (!TOID_IS_NULL(rsb) && prsb->n > BTREE_MIN)
		btree_map_rotate_right(rsb, node, parent, p);
	else if (!TOID_IS_NULL(lsb) && plsb->n > BTREE_MIN)
		btree_map_rotate_left(lsb, node, parent, p);
	else if (TOID_IS_NULL(rsb)) /* always merge with rightmost node */
		btree_map_merge(map, node, lsb, parent, p - 1);
	else
		btree_map_merge(map, rsb, node, parent, p);
}

/*
 * btree_map_get_leftmost_leaf -- (internal) searches for the successor
 */
static TOID(struct tree_map_node)
btree_map_get_leftmost_leaf(TOID(struct btree_map) map,
	TOID(struct tree_map_node) n, TOID(struct tree_map_node) *p)
{
	struct tree_map_node *pn = pgl_get(n.oid);

	if (TOID_IS_NULL(pn->slots[0]))
		return n;

	*p = n;

	TOID(struct tree_map_node) slot = pn->slots[0];

	return btree_map_get_leftmost_leaf(map, slot, p);
}

/*
 * btree_map_remove_from_node -- (internal) removes element from node
 */
static void
btree_map_remove_from_node(TOID(struct btree_map) map,
	TOID(struct tree_map_node) node,
	TOID(struct tree_map_node) parent, int p)
{
	struct tree_map_node *pnode = pgl_tx_open(node.oid);

	if (TOID_IS_NULL(pnode->slots[0])) { /* leaf */
		if (pnode->n == 1 || p == BTREE_ORDER - 2) {
			set_empty_item(&pnode->items[p]);
		} else if (pnode->n != 1) {
			memmove(&pnode->items[p],
				&pnode->items[p + 1],
				sizeof(struct tree_map_node_item) *
				(pnode->n - p));
		}

		pnode->n -= 1;
		return;
	}

	/* can't delete from non-leaf nodes, remove successor */
	TOID(struct tree_map_node) rchild = pnode->slots[p + 1];
	TOID(struct tree_map_node) lp = node;
	TOID(struct tree_map_node) lm =
		btree_map_get_leftmost_leaf(map, rchild, &lp);

	/* btree_map_remove_from_node() will modify lm's data */
	struct tree_map_node *plm = pgl_tx_open(lm.oid);

	pnode->items[p] = plm->items[0];

	btree_map_remove_from_node(map, lm, lp, 0);

	if (plm->n < BTREE_MIN) /* right child can be deficient now */
		btree_map_rebalance(map, lm, lp,
			TOID_EQUALS(lp, node) ? p + 1 : 0);
}

#define NODE_CONTAINS_ITEM(pn, _i, _k)\
((_i) != pn->n && pn->items[_i].key == (_k))

#define NODE_CHILD_CAN_CONTAIN_ITEM(pn, _i, _k)\
((_i) == pn->n || pn->items[_i].key > (_k)) &&\
!TOID_IS_NULL(pn->slots[_i])

/*
 * btree_map_remove_item -- (internal) removes item from node
 */
static PMEMoid
btree_map_remove_item(TOID(struct btree_map) map,
	TOID(struct tree_map_node) node, TOID(struct tree_map_node) parent,
	uint64_t key, int p)
{
	PMEMoid ret = OID_NULL;

	struct tree_map_node *pnode = pgl_get(node.oid);

	for (int i = 0; i <= pnode->n; ++i) {
		if (NODE_CONTAINS_ITEM(pnode, i, key)) {
			ret = pnode->items[i].value;
			btree_map_remove_from_node(map, node, parent, i);
			break;
		} else if (NODE_CHILD_CAN_CONTAIN_ITEM(pnode, i, key)) {
			ret = btree_map_remove_item(map, pnode->slots[i],
				node, key, i);
			break;
		}
	}

	/* check for deficient nodes walking up */
	if (!TOID_IS_NULL(parent) && pnode->n < BTREE_MIN)
		btree_map_rebalance(map, node, parent, p);

	return ret;
}

/*
 * btree_map_remove -- removes key-value pair from the map
 */
PMEMoid
btree_map_remove(PMEMobjpool *pop, TOID(struct btree_map) map, uint64_t key)
{
	PMEMoid ret = OID_NULL;

	struct btree_map *bmap = pgl_get(map.oid);
	PGL_TX_BEGIN(pop) {
		ret = btree_map_remove_item(map, bmap->root,
				TOID_NULL(struct tree_map_node), key, 0);
	} PGL_TX_END

	return ret;
}

/*
 * btree_map_get_in_node -- (internal) searches for a value in the node
 */
static PMEMoid
btree_map_get_in_node(TOID(struct tree_map_node) node, uint64_t key)
{
	struct tree_map_node *pnode = pgl_get(node.oid);

	PMEMoid retoid = OID_NULL;
	for (int i = 0; i <= pnode->n; ++i) {
		if (NODE_CONTAINS_ITEM(pnode, i, key)) {
			retoid = pnode->items[i].value;
			break;
		} else if (NODE_CHILD_CAN_CONTAIN_ITEM(pnode, i, key)) {
			retoid = btree_map_get_in_node(pnode->slots[i], key);
			break;
		}
	}

	return retoid;
}

/*
 * btree_map_get -- searches for a value of the key
 */
PMEMoid
btree_map_get(PMEMobjpool *pop, TOID(struct btree_map) map, uint64_t key)
{
	struct btree_map *bmap = pgl_get(map.oid);
	TOID(struct tree_map_node) root = bmap->root;

	if (TOID_IS_NULL(root))
		return OID_NULL;
	return btree_map_get_in_node(root, key);
}

/*
 * btree_map_lookup_in_node -- (internal) searches for key if exists
 */
static int
btree_map_lookup_in_node(TOID(struct tree_map_node) node, uint64_t key)
{
	struct tree_map_node *pnode = pgl_get(node.oid);

	int exist = 0;
	for (int i = 0; i <= pnode->n; ++i) {
		if (NODE_CONTAINS_ITEM(pnode, i, key)) {
			exist = 1;
			break;
		} else if (NODE_CHILD_CAN_CONTAIN_ITEM(pnode, i, key)) {
			exist = btree_map_lookup_in_node(pnode->slots[i], key);
			break;
		}
	}

	return exist;
}

/*
 * btree_map_lookup -- searches if key exists
 */
int
btree_map_lookup(PMEMobjpool *pop, TOID(struct btree_map) map, uint64_t key)
{
	struct btree_map *bmap = pgl_get(map.oid);
	TOID(struct tree_map_node) root = bmap->root;

	if (TOID_IS_NULL(root))
		return 0;
	return btree_map_lookup_in_node(root, key);
}

/*
 * btree_map_foreach_node -- (internal) recursively traverses tree
 */
static int
btree_map_foreach_node(const TOID(struct tree_map_node) p,
	int (*cb)(uint64_t key, PMEMoid, void *arg), void *arg)
{
	if (TOID_IS_NULL(p))
		return 0;

	struct tree_map_node *pp = pgl_get(p.oid);

	int retval = 0;
	for (int i = 0; i <= pp->n; ++i) {
		if (btree_map_foreach_node(pp->slots[i], cb, arg) != 0) {
			retval = 1;
			break;
		}

		if (i != pp->n && pp->items[i].key != 0) {
			if (cb(pp->items[i].key, pp->items[i].value,
					arg) != 0) {
				retval = 1;
				break;
			}
		}
	}

	return retval;
}

/*
 * btree_map_foreach -- initiates recursive traversal
 */
int
btree_map_foreach(PMEMobjpool *pop, TOID(struct btree_map) map,
	int (*cb)(uint64_t key, PMEMoid value, void *arg), void *arg)
{
	struct btree_map *bmap = pgl_get(map.oid);
	TOID(struct tree_map_node) root = bmap->root;

	return btree_map_foreach_node(root, cb, arg);
}

/*
 * ctree_map_check -- check if given persistent object is a tree map
 */
int
btree_map_check(PMEMobjpool *pop, TOID(struct btree_map) map)
{
	return TOID_IS_NULL(map) || !TOID_VALID(map);
}

/*
 * btree_map_insert_new -- allocates a new object and inserts it into the tree
 */
int
btree_map_insert_new(PMEMobjpool *pop, TOID(struct btree_map) map,
		uint64_t key, size_t size, unsigned type_num,
		void (*constructor)(PMEMobjpool *pop, void *ptr, void *arg),
		void *arg)
{
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		void *pn = pgl_tx_alloc_open(size, type_num);
		PMEMoid n = pgl_oid(pn);
		constructor(pop, pn, arg);
		btree_map_insert(pop, map, key, n);
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}

/*
 * btree_map_remove_free -- removes and frees an object from the tree
 */
int
btree_map_remove_free(PMEMobjpool *pop, TOID(struct btree_map) map,
		uint64_t key)
{
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		PMEMoid val = btree_map_remove(pop, map, key);
		pgl_tx_free(val);
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}
