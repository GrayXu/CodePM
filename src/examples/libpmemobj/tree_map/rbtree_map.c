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
 * rbtree.c -- red-black tree implementation /w sentinel nodes
 */

#include <assert.h>
#include <errno.h>

#include "rbtree_map.h"

#include <libpangolin.h>

TOID_DECLARE(struct tree_map_node, RBTREE_MAP_TYPE_OFFSET + 1);

#define NODE_P(_n)\
D_RW(_n)->parent

#define NODE_GRANDP(_n)\
NODE_P(NODE_P(_n))

#define NODE_PARENT_AT(_n, _rbc)\
D_RW(NODE_P(_n))->slots[_rbc]

#define NODE_PARENT_RIGHT(_n)\
NODE_PARENT_AT(_n, RB_RIGHT)

#define NODE_IS(_n, _rbc)\
TOID_EQUALS(_n, NODE_PARENT_AT(_n, _rbc))

#define NODE_IS_RIGHT(_n)\
TOID_EQUALS(_n, NODE_PARENT_RIGHT(_n))

#define NODE_LOCATION(_n)\
NODE_IS_RIGHT(_n)

#define RB_FIRST(_m)\
D_RW(D_RW(_m)->root)->slots[RB_LEFT]

#define NODE_IS_NULL(_n)\
TOID_EQUALS(_n, s)

enum rb_color {
	COLOR_BLACK,
	COLOR_RED,

	MAX_COLOR
};

enum rb_children {
	RB_LEFT,
	RB_RIGHT,

	MAX_RB
};

struct tree_map_node {
	uint64_t key;
	PMEMoid value;
	enum rb_color color;
	TOID(struct tree_map_node) parent;
	TOID(struct tree_map_node) slots[MAX_RB];
};

struct rbtree_map {
	TOID(struct tree_map_node) sentinel;
	TOID(struct tree_map_node) root;
};

/*
 * rbtree_map_create -- allocates a new red-black tree instance
 */
int
rbtree_map_create(PMEMobjpool *pop, TOID(struct rbtree_map) *map, void *arg)
{
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		struct rbtree_map *rbmap = pgl_tx_alloc_open(
					sizeof(struct rbtree_map),
					TOID_TYPE_NUM(struct rbtree_map));
		*map = (TOID(struct rbtree_map))pgl_oid(rbmap);

		struct tree_map_node *ps = pgl_tx_alloc_open(
					sizeof(struct tree_map_node),
					TOID_TYPE_NUM(struct tree_map_node));
		TOID(struct tree_map_node) s =
				(TOID(struct tree_map_node))pgl_oid(ps);

		ps->color = COLOR_BLACK;
		ps->parent = s;
		ps->slots[RB_LEFT] = s;
		ps->slots[RB_RIGHT] = s;

		struct tree_map_node *pr = pgl_tx_alloc_open(
					sizeof(struct tree_map_node),
					TOID_TYPE_NUM(struct tree_map_node));
		TOID(struct tree_map_node) r =
				(TOID(struct tree_map_node))pgl_oid(pr);

		pr->color = COLOR_BLACK;
		pr->parent = s;
		pr->slots[RB_LEFT] = s;
		pr->slots[RB_RIGHT] = s;

		rbmap->sentinel = s;
		rbmap->root = r;
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}

/*
 * rbtree_map_clear_node -- (internal) clears this node and its children
 */
static void
rbtree_map_clear_node(TOID(struct rbtree_map) map, TOID(struct tree_map_node) p)
{
	struct rbtree_map *rbmap = pgl_get(map.oid);
	TOID(struct tree_map_node) s = rbmap->sentinel;

	struct tree_map_node *pp = pgl_tx_open(p.oid);

	if (!NODE_IS_NULL(pp->slots[RB_LEFT]))
		rbtree_map_clear_node(map, pp->slots[RB_LEFT]);

	if (!NODE_IS_NULL(pp->slots[RB_RIGHT]))
		rbtree_map_clear_node(map, pp->slots[RB_RIGHT]);

	pgl_tx_free(p.oid);
}

/*
 * rbtree_map_clear -- removes all elements from the map
 */
int
rbtree_map_clear(PMEMobjpool *pop, TOID(struct rbtree_map) map)
{
	PGL_TX_BEGIN(pop) {
		struct rbtree_map *rbmap = pgl_tx_open(map.oid);
		rbtree_map_clear_node(map, rbmap->root);

		pgl_tx_free(rbmap->sentinel.oid);

		rbmap->root = TOID_NULL(struct tree_map_node);
		rbmap->sentinel = TOID_NULL(struct tree_map_node);
	} PGL_TX_END

	return 0;
}

/*
 * rbtree_map_destroy -- cleanups and frees red-black tree instance
 */
int
rbtree_map_destroy(PMEMobjpool *pop, TOID(struct rbtree_map) *map)
{
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		rbtree_map_clear(pop, *map);
		pgl_tx_free(map->oid);
		*map = TOID_NULL(struct rbtree_map);
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}

/*
 * rbtree_map_rotate -- (internal) performs a left/right rotation around a node
 */
static void
rbtree_map_rotate(TOID(struct rbtree_map) map,
	TOID(struct tree_map_node) node, enum rb_children c)
{
	struct rbtree_map *rbmap = pgl_get(map.oid);
	struct tree_map_node *pn = pgl_tx_open(node.oid);
	struct tree_map_node *pch = pgl_tx_open(pn->slots[!c].oid);
	TOID(struct tree_map_node) s = rbmap->sentinel;

	pn->slots[!c] = pch->slots[c];

	if (!TOID_EQUALS(pch->slots[c], s)) {
		struct tree_map_node *slot =
			pgl_tx_open(pch->slots[c].oid);
		slot->parent = node;
	}

	pch->parent = pn->parent;

	TOID(struct tree_map_node) child =
		(TOID(struct tree_map_node))pgl_oid(pch);

	struct tree_map_node *ppn = pgl_tx_open(pn->parent.oid);
	ppn->slots[TOID_EQUALS(node, ppn->slots[RB_RIGHT])] = child;

	pch->slots[c] = node;
	pn->parent = child;
}

/*
 * rbtree_map_insert_bst -- (internal) inserts a node in regular BST fashion
 */
static void
rbtree_map_insert_bst(TOID(struct rbtree_map) map, TOID(struct tree_map_node) n)
{
	struct rbtree_map *rbmap = pgl_get(map.oid);
	struct tree_map_node *pnode = pgl_get(rbmap->root.oid);

	TOID(struct tree_map_node) parent = rbmap->root;
	TOID(struct tree_map_node) *dst = &pnode->slots[RB_LEFT];
	TOID(struct tree_map_node) s = rbmap->sentinel;

	struct tree_map_node *pn = pgl_tx_open(n.oid);
	pn->slots[RB_LEFT] = s;
	pn->slots[RB_RIGHT] = s;

	while (!NODE_IS_NULL(*dst)) {
		parent = *dst;
		pnode = pgl_get(dst->oid);
		dst = &pnode->slots[pn->key > pnode->key];
	}

	pn->parent = parent;

	/* add pnode to transaction and redirect dst to the objbuf */
	dst = PTR_TO(pgl_tx_add(pnode), DISP(pnode, dst));
	*dst = n;
}

/*
 * rbtree_map_recolor -- (internal) restores red-black tree properties
 */
static TOID(struct tree_map_node)
rbtree_map_recolor(TOID(struct rbtree_map) map,
	TOID(struct tree_map_node) n, enum rb_children c)
{
	struct tree_map_node *pn = pgl_get(n.oid);
	struct tree_map_node *ppn = pgl_tx_open(pn->parent.oid);
	struct tree_map_node *pgn = pgl_tx_open(ppn->parent.oid);

	TOID(struct tree_map_node) uncle = pgn->slots[!c];

	struct tree_map_node *puncle = pgl_get(uncle.oid);

	if (puncle->color == COLOR_RED) {
		puncle = pgl_tx_add(puncle);
		puncle->color = COLOR_BLACK;
		ppn->color = COLOR_BLACK;
		pgn->color = COLOR_RED;

		return ppn->parent;
	} else {
		if (TOID_EQUALS(n, ppn->slots[!c])) {
			n = pn->parent;
			rbtree_map_rotate(map, n, c);
			/* n was assigned a new value so buffers need reset */
			pn = pgl_get(n.oid);
			ppn = pgl_get(pn->parent.oid);
			pgn = pgl_get(ppn->parent.oid);
		}

		ppn->color = COLOR_BLACK;
		pgn->color = COLOR_RED;
		rbtree_map_rotate(map, ppn->parent, (enum rb_children)!c);
	}

	return n;
}

/*
 * rbtree_map_insert -- inserts a new key-value pair into the map
 */
int
rbtree_map_insert(PMEMobjpool *pop, TOID(struct rbtree_map) map,
	uint64_t key, PMEMoid value)
{
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		struct tree_map_node *pn = pgl_tx_alloc_open(
					sizeof(struct tree_map_node),
					TOID_TYPE_NUM(struct tree_map_node));
		TOID(struct tree_map_node) n =
			(TOID(struct tree_map_node))pgl_oid(pn);

		pn->key = key;
		pn->value = value;

		rbtree_map_insert_bst(map, n);

		pn->color = COLOR_RED;

		struct tree_map_node *ppn = pgl_get(pn->parent.oid);
		struct tree_map_node *pgn = pgl_get(ppn->parent.oid);
		while (ppn->color == COLOR_RED) {
			n = rbtree_map_recolor(map, n, (enum rb_children)
				TOID_EQUALS(pn->parent, pgn->slots[RB_RIGHT]));
			pn = pgl_get(n.oid);
			ppn = pgl_get(pn->parent.oid);
			pgn = pgl_get(ppn->parent.oid);
		}

		struct rbtree_map *rbmap = pgl_get(map.oid);
		struct tree_map_node *proot = pgl_get(rbmap->root.oid);
		struct tree_map_node *first =
			pgl_tx_open(proot->slots[RB_LEFT].oid);

		first->color = COLOR_BLACK;
	} PGL_TX_END

	return ret;
}

/*
 * rbtree_map_successor -- (internal) returns the successor of a node
 */
static TOID(struct tree_map_node)
rbtree_map_successor(TOID(struct rbtree_map) map, TOID(struct tree_map_node) n)
{
	struct tree_map_node *pn = pgl_get(n.oid);
	struct rbtree_map *rbmap = pgl_get(map.oid);

	TOID(struct tree_map_node) dst = pn->slots[RB_RIGHT];
	TOID(struct tree_map_node) s = rbmap->sentinel;

	struct tree_map_node *pdst = pgl_get(dst.oid);
	if (!TOID_EQUALS(s, dst)) {
		while (!NODE_IS_NULL(pdst->slots[RB_LEFT])) {
			dst = pdst->slots[RB_LEFT];
			pdst = pgl_get(dst.oid);
		}
	} else {
		dst = pn->parent;
		while (TOID_EQUALS(n, pdst->slots[RB_RIGHT])) {
			n = dst;
			dst = pdst->parent;
			pdst = pgl_get(dst.oid);
		}
		if (TOID_EQUALS(dst, rbmap->root))
			return s;
	}

	return dst;
}

/*
 * rbtree_map_find_node -- (internal) returns the node that contains the key
 */
static TOID(struct tree_map_node)
rbtree_map_find_node(TOID(struct rbtree_map) map, uint64_t key)
{
	struct rbtree_map *rbmap = pgl_get(map.oid);
	struct tree_map_node *proot = pgl_get(rbmap->root.oid);

	TOID(struct tree_map_node) dst = proot->slots[RB_LEFT];
	TOID(struct tree_map_node) s = rbmap->sentinel;

	struct tree_map_node *pdst = pgl_get(dst.oid);
	while (!NODE_IS_NULL(dst)) {
		if (pdst->key == key)
			return dst;

		dst = pdst->slots[key > pdst->key];
		pdst = pgl_get(dst.oid);
	}

	return TOID_NULL(struct tree_map_node);
}

/*
 * rbtree_map_repair_branch -- (internal) restores red-black tree in one branch
 */
static TOID(struct tree_map_node)
rbtree_map_repair_branch(TOID(struct rbtree_map) map,
	TOID(struct tree_map_node) n, enum rb_children c)
{
	struct tree_map_node *pn = pgl_tx_open(n.oid);
	struct tree_map_node *ppn = pgl_tx_open(pn->parent.oid);

	TOID(struct tree_map_node) sb = ppn->slots[!c]; /* sibling */
	struct tree_map_node *psb = pgl_tx_open(sb.oid);

	if (psb->color == COLOR_RED) {
		psb->color = COLOR_BLACK;
		ppn->color = COLOR_RED;
		rbtree_map_rotate(map, pn->parent, c);
		sb = ppn->slots[!c];
		psb = pgl_get(sb.oid);
	}

	struct tree_map_node *psb_slots[MAX_RB];
	psb_slots[RB_LEFT] = pgl_tx_open(psb->slots[RB_LEFT].oid);
	psb_slots[RB_RIGHT] = pgl_tx_open(psb->slots[RB_RIGHT].oid);

	if (psb_slots[RB_RIGHT]->color == COLOR_BLACK &&
		psb_slots[RB_LEFT]->color == COLOR_BLACK) {
		psb->color = COLOR_RED;
		return pn->parent;
	} else {
		if (psb_slots[!c]->color == COLOR_BLACK) {
			psb_slots[c]->color = COLOR_BLACK;
			psb->color = COLOR_RED;
			rbtree_map_rotate(map, sb, (enum rb_children)!c);
			sb = ppn->slots[!c];
			/* sb is assigned a new value, reset relevant buffers */
			psb = pgl_get(sb.oid);
			psb_slots[RB_LEFT] =
				pgl_get(psb->slots[RB_LEFT].oid);
			psb_slots[RB_RIGHT] =
				pgl_get(psb->slots[RB_RIGHT].oid);
		}
		psb->color = ppn->color;
		ppn->color = COLOR_BLACK;
		psb_slots[!c]->color = COLOR_BLACK;
		rbtree_map_rotate(map, pn->parent, c);

		struct rbtree_map *rbmap = pgl_get(map.oid);
		struct tree_map_node *proot = pgl_get(rbmap->root.oid);

		TOID(struct tree_map_node) rb1st = proot->slots[RB_LEFT];

		return rb1st;
	}

	return n;
}

/*
 * rbtree_map_repair -- (internal) restores red-black tree properties
 * after remove
 */
static void
rbtree_map_repair(TOID(struct rbtree_map) map, TOID(struct tree_map_node) n)
{
	struct rbtree_map *rbmap = pgl_get(map.oid);
	struct tree_map_node *proot = pgl_get(rbmap->root.oid);

	struct tree_map_node *pn = pgl_get(n.oid);
	struct tree_map_node *ppn = pgl_get(pn->parent.oid);
	while (!TOID_EQUALS(n, proot->slots[RB_LEFT]) &&
			pn->color == COLOR_BLACK) {
		n = rbtree_map_repair_branch(map, n, (enum rb_children)
				TOID_EQUALS(n, ppn->slots[RB_RIGHT]));
		pn = pgl_get(n.oid);
		ppn = pgl_get(pn->parent.oid);
		/* rbtree_map_repair_branch() could modify root */
		proot = pgl_get(rbmap->root.oid);
	}

	pn = pgl_tx_add(pn);
	pn->color = COLOR_BLACK;
}

/*
 * rbtree_map_remove -- removes key-value pair from the map
 */
PMEMoid
rbtree_map_remove(PMEMobjpool *pop, TOID(struct rbtree_map) map, uint64_t key)
{
	PMEMoid ret = OID_NULL;

	TOID(struct tree_map_node) n = rbtree_map_find_node(map, key);
	if (TOID_IS_NULL(n))
		return ret;

	struct tree_map_node *pn = pgl_get(n.oid);
	struct rbtree_map *rbmap = pgl_get(map.oid);

	ret = pn->value;

	TOID(struct tree_map_node) s = rbmap->sentinel;
	TOID(struct tree_map_node) r = rbmap->root;

	TOID(struct tree_map_node) y = (NODE_IS_NULL(pn->slots[RB_LEFT]) ||
					NODE_IS_NULL(pn->slots[RB_RIGHT]))
					? n : rbtree_map_successor(map, n);

	struct tree_map_node *py = pgl_get(y.oid);
	TOID(struct tree_map_node) x = NODE_IS_NULL(py->slots[RB_LEFT]) ?
			py->slots[RB_RIGHT] : py->slots[RB_LEFT];

	PGL_TX_BEGIN(pop) {
		struct tree_map_node *px = pgl_tx_open(x.oid);
		px->parent = py->parent;
		if (TOID_EQUALS(px->parent, r)) {
			struct tree_map_node *pr = pgl_tx_open(r.oid);
			pr->slots[RB_LEFT] = x;
		} else {
			struct tree_map_node *ppy =
				pgl_tx_open(py->parent.oid);
			ppy->slots[TOID_EQUALS(y, ppy->slots[RB_RIGHT])] = x;
		}

		if (py->color == COLOR_BLACK)
			rbtree_map_repair(map, x);

		if (!TOID_EQUALS(y, n)) {
			py = pgl_tx_add(py);
			/* rbtree_map_repair() might modify n's data */
			pn = pgl_get(n.oid);
			py->slots[RB_LEFT] = pn->slots[RB_LEFT];
			py->slots[RB_RIGHT] = pn->slots[RB_RIGHT];
			py->parent = pn->parent;
			py->color = pn->color;

			struct tree_map_node *pnl =
				pgl_tx_open(pn->slots[RB_LEFT].oid);
			struct tree_map_node *pnr =
				pgl_tx_open(pn->slots[RB_RIGHT].oid);
			pnl->parent = y;
			pnr->parent = y;

			struct tree_map_node *ppn =
				pgl_tx_open(pn->parent.oid);
			ppn->slots[TOID_EQUALS(n, ppn->slots[RB_RIGHT])] = y;
		}

		pgl_tx_free(n.oid);
	} PGL_TX_END

	return ret;
}

/*
 * rbtree_map_get -- searches for a value of the key
 */
PMEMoid
rbtree_map_get(PMEMobjpool *pop, TOID(struct rbtree_map) map, uint64_t key)
{
	TOID(struct tree_map_node) node = rbtree_map_find_node(map, key);
	if (TOID_IS_NULL(node))
		return OID_NULL;

	struct tree_map_node *pn = pgl_get(node.oid);
	PMEMoid retoid = pn->value;

	return retoid;
}

/*
 * rbtree_map_lookup -- searches if key exists
 */
int
rbtree_map_lookup(PMEMobjpool *pop, TOID(struct rbtree_map) map, uint64_t key)
{
	TOID(struct tree_map_node) node = rbtree_map_find_node(map, key);
	if (TOID_IS_NULL(node))
		return 0;

	return 1;
}

/*
 * rbtree_map_foreach_node -- (internal) recursively traverses tree
 */
static int
rbtree_map_foreach_node(TOID(struct rbtree_map) map,
	TOID(struct tree_map_node) p,
	int (*cb)(uint64_t key, PMEMoid value, void *arg), void *arg)
{
	int ret = 0;

	struct rbtree_map *rbmap = pgl_get(map.oid);
	if (TOID_EQUALS(p, rbmap->sentinel))
		return 0;

	struct tree_map_node *pp = pgl_get(p.oid);
	/* this is an in-order traversal and should return sorted keys */
	if ((ret = rbtree_map_foreach_node(map,
		pp->slots[RB_LEFT], cb, arg)) == 0) {
		if ((ret = cb(pp->key, pp->value, arg)) == 0)
			rbtree_map_foreach_node(map,
				pp->slots[RB_RIGHT], cb, arg);
	}

	return ret;
}

/*
 * rbtree_map_foreach -- initiates recursive traversal
 */
int
rbtree_map_foreach(PMEMobjpool *pop, TOID(struct rbtree_map) map,
	int (*cb)(uint64_t key, PMEMoid value, void *arg), void *arg)
{
	struct rbtree_map *rbmap = pgl_get(map.oid);
	struct tree_map_node *proot = pgl_get(rbmap->root.oid);

	TOID(struct tree_map_node) rb1st = proot->slots[RB_LEFT];

	return rbtree_map_foreach_node(map, rb1st, cb, arg);
}

/*
 * rbtree_map_is_empty -- checks whether the tree map is empty
 */
int
rbtree_map_is_empty(PMEMobjpool *pop, TOID(struct rbtree_map) map)
{
	struct rbtree_map *rbmap = pgl_get(map.oid);
	struct tree_map_node *proot = pgl_get(rbmap->root.oid);

	TOID(struct tree_map_node) rb1st = proot->slots[RB_LEFT];

	return TOID_IS_NULL(rb1st);
}

/*
 * rbtree_map_check -- check if given persistent object is a tree map
 */
int
rbtree_map_check(PMEMobjpool *pop, TOID(struct rbtree_map) map)
{
	return TOID_IS_NULL(map) || !TOID_VALID(map);
}

/*
 * rbtree_map_insert_new -- allocates a new object and inserts it into the tree
 */
int
rbtree_map_insert_new(PMEMobjpool *pop, TOID(struct rbtree_map) map,
		uint64_t key, size_t size, unsigned type_num,
		void (*constructor)(PMEMobjpool *pop, void *ptr, void *arg),
		void *arg)
{
	int ret = 0;

	PGL_TX_BEGIN(pop) {
		void *pn = pgl_tx_alloc_open(size, type_num);
		PMEMoid n = pgl_oid(pn);
		constructor(pop, pn, arg);
		rbtree_map_insert(pop, map, key, n);
	} PGL_TX_ONABORT {
		ret = 1;
	} PGL_TX_END

	return ret;
}

/*
 * rbtree_map_remove_free -- removes and frees an object from the tree
 */
int
rbtree_map_remove_free(PMEMobjpool *pop, TOID(struct rbtree_map) map,
		uint64_t key)
{
	int ret = 0;

	TX_BEGIN(pop) {
		PMEMoid val = rbtree_map_remove(pop, map, key);
		pgl_tx_free(val);
	} TX_ONABORT {
		ret = 1;
	} TX_END

	return ret;
}
