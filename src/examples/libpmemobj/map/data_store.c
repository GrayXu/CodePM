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
 * data_store.c -- tree_map example usage
 */

#include <ex_common.h>
#include <stdio.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include "map.h"
#include "map_ctree.h"
#include "map_btree.h"
#include "map_rtree.h"
#include "map_rbtree.h"
#include "map_hashmap_atomic.h"
#include "map_hashmap_tx.h"
#include "map_hashmap_rp.h"
#include "map_skiplist.h"

#include <libpangolin.h>

POBJ_LAYOUT_BEGIN(data_store);
POBJ_LAYOUT_ROOT(data_store, struct store_root);
POBJ_LAYOUT_TOID(data_store, struct store_item);
POBJ_LAYOUT_END(data_store);

#define MAX_INSERTS 100000

#define DEBUG_LEVEL 1

/* prints for debug but do not commit since Travis thinks they are unexpected */
#if DEBUG_LEVEL >= 2
#define print1(fmt, ...) printf(fmt, ##__VA_ARGS__)
#define print2(fmt, ...) printf(fmt, ##__VA_ARGS__)
#else
#define print2(fmt, ...)
#if DEBUG_LEVEL >= 1
#define print1(fmt, ...) printf(fmt, ##__VA_ARGS__)
#else
#define print1(fmt, ...)
#endif
#endif

/* generated keys */
static uint64_t rkeys;
static uint64_t rndk[MAX_INSERTS];

static uint64_t nkeys;
static uint64_t keys[MAX_INSERTS];

struct store_item {
	uint64_t item_data;
};

struct store_root {
	TOID(struct map) map;
};

/*
 * new_store_item -- transactionally creates and initializes new item
 */
static TOID(struct store_item)
new_store_item(int i)
{
	/*
	 * This function allocates a pmem block to store data, and the "value"
	 * stored in a KV store data structure is the PMEMoid pointing to this
	 * allocated pmem block.
	 */
	struct store_item *item = pgl_tx_alloc_open(
		sizeof(struct store_item), TOID_TYPE_NUM(struct store_item));

	/* ascii VAL- for better visibility in hexdump debugging */
	item->item_data = (uint64_t)(i + 1) << 32 | 0x2d4c4156UL;

	return (TOID(struct store_item))pgl_oid(item);
}

/*
 * make_key -- make a key
 */
static uint64_t
make_key(int i)
{
	/* note tested data structures usually do not insert duplicated keys */

	unsigned key_lo = rand();
	unsigned key_hi = (i + 1) << 24 | 0x59454b; /* ascii KEY */
	uint64_t key = (((uint64_t)key_hi) << 32) | ((uint64_t)key_lo);

	print2("0x%016lx%s", key, (i + 1) % 5 == 0 ? ",\n" : ", ");

	rndk[rkeys++] = key;

	return key;
}


/*
 * get_keys -- inserts the keys of the items by key order (sorted, descending)
 */
static int
get_keys(uint64_t key, PMEMoid value, void *arg)
{
	print2("0x%016lx%s", key, (nkeys + 1) % 5 == 0 ? ",\n" : ", ");

	keys[nkeys++] = key;

	return 0;
}

/*
 * dec_keys -- decrements the keys count for every item
 */
static int
dec_keys(uint64_t key, PMEMoid value, void *arg)
{
	nkeys--;
	return 0;
}

/*
 * parse_map_type -- parse type of map
 */
static const struct map_ops *
parse_map_type(const char *type)
{
	if (strcmp(type, "ctree") == 0)
		return MAP_CTREE;
	else if (strcmp(type, "btree") == 0)
		return MAP_BTREE;
	else if (strcmp(type, "rtree") == 0)
		return MAP_RTREE;
	else if (strcmp(type, "rbtree") == 0)
		return MAP_RBTREE;
	else if (strcmp(type, "hashmap_atomic") == 0)
		return MAP_HASHMAP_ATOMIC;
	else if (strcmp(type, "hashmap_tx") == 0)
		return MAP_HASHMAP_TX;
	else if (strcmp(type, "hashmap_rp") == 0)
		return MAP_HASHMAP_RP;
	else if (strcmp(type, "skiplist") == 0)
		return MAP_SKIPLIST;
	return NULL;

}

int main(int argc, const char *argv[]) {
	if (argc < 3) {
		printf("usage: %s "
			"<ctree|btree|rtree|rbtree|hashmap_atomic|hashmap_rp|"
			"hashmap_tx|skiplist> file-name [nops]\n", argv[0]);
		return 1;
	}

	const char *type = argv[1];
	const char *path = argv[2];
	const struct map_ops *map_ops = parse_map_type(type);
	if (!map_ops) {
		fprintf(stderr, "invalid container type -- '%s'\n", type);
		return 1;
	}

	int nops = MAX_INSERTS;

	if (argc > 3) {
		nops = atoi(argv[3]);
		if (nops <= 0 || nops > MAX_INSERTS) {
			fprintf(stderr, "number of operations must be "
				"in range 1..%d\n", MAX_INSERTS);
			return 1;
		}
	}

	PMEMobjpool *pop;
	srand((unsigned)time(NULL));

	if (file_exists(path) != 0) {
		if ((pop = pmemobj_create(path, POBJ_LAYOUT_NAME(data_store),
			800 * 1024 * 1024, 0666)) == NULL) {
			perror("failed to create pool\n");
			return 1;
		}
	} else {
		if ((pop = pmemobj_open(path,
				POBJ_LAYOUT_NAME(data_store))) == NULL) {
			perror("failed to open pool\n");
			return 1;
		}
	}

	int txmode = 0;
	char *env_mode = getenv("TX_MODE");
	if (env_mode != NULL)
		txmode = atoi(env_mode);

	int exitpoint = 0;
	char *env_exit = getenv("EXIT_POINT");
	if (env_exit != NULL)
		exitpoint = atoi(env_exit);

	TOID(struct store_root) root = POBJ_ROOT(pop, struct store_root);

	struct map_ctx *mapc = map_ctx_init(map_ops, pop);
	if (!mapc) {
		perror("cannot allocate map context\n");
		return 1;
	}

	/*
	 * OBUF: libpangolin can allow writing to &D_RW(root)->map, which points
	 * to pmem. This write operation is not intended to be transanctional so
	 * we don't need to use BT here.
	 *
	 * Similarly many functions below only uses the value of D_RW(root)->map
	 * which also work with libpmemobj and so we don't change the code for
	 * simplicity.
	 */

	/* delete the map if it exists */
	if (!map_check(mapc, D_RW(root)->map))
		map_destroy(mapc, &D_RW(root)->map);

	pgl_reset_stats();

	/* insert random items in a transaction */
	int aborted = 0;
	if (txmode == 0) { /* all inserts as one transaction */
		struct store_root *sroot = pgl_get(root.oid);
		PGL_TX_BEGIN(pop) {
			sroot = pgl_tx_add(sroot);
			map_create(mapc, &sroot->map, NULL);

			for (int i = 0; i < nops; ++i) {
				/* new_store_item is transactional! */
				map_insert(mapc, sroot->map, make_key(i),
						new_store_item(i).oid);
			}
		} PGL_TX_ONABORT {
			perror("transaction aborted\n");
			map_ctx_free(mapc);
			aborted = 1;
		} PGL_TX_END /* sroot is closed on transaction commit */
	} else {
		struct store_root *sroot = pgl_get(root.oid);
		PGL_TX_BEGIN(pop) {
			sroot = pgl_tx_add(sroot);
			map_create(mapc, &sroot->map, NULL);
		} PGL_TX_END

		print2(BLU "\n[Info] Generated keys:\n" RST);
		for (int i = 0; i < nops; ++i) {
			PGL_TX_BEGIN(pop) {
				/* new_store_item is transactional! */
				map_insert(mapc, D_RW(root)->map, make_key(i),
						new_store_item(i).oid);
			} PGL_TX_END /* sroot is closed on transaction commit */
		}
	}

	if (aborted)
		return -1;

	print1(BLU "\n[Info]  map_insert(): %d keys" RST, nops);

	pgl_print_stats();
	if (exitpoint == 1)
		goto out;

	/*
	 * The foreach function of rtree has some bug and it can not recognize
	 * invalid keys that are short.
	 *
	 * Note that if there are duplicated keys in rndk[], then actually only
	 * 1 such key was inserted, and later remove it more than once will
	 * fail.
	 */
	if (strcmp(type, "rtree") == 0) {
		for (int i = 0; i < nops; ++i)
			keys[nkeys++] = rndk[i];

		print1(BLU "\n[Info] map_foreach() skipped for rtree");
		goto skip_foreach;
	}

	/* count the items */
	print2(BLU "\n[Info] Foreach keys:\n" RST);
	map_foreach(mapc, D_RW(root)->map, get_keys, NULL);

	/* nkeys can be smaller than nops if there were duplicated keys */
	print1(BLU "\n[Info] map_foreach(): %lu keys" RST, nkeys);

skip_foreach:

	if (exitpoint == 2)
		goto out;

	int found = 0;
	for (int i = 0; i < nkeys; ++i) {
		PMEMoid keyoid = map_get(mapc, D_RW(root)->map, keys[i]);
		if (OID_IS_NULL(keyoid)) {
			print1("\nkeys[%d] 0x%016lx has NULL oid", i, keys[i]);
			continue;
		}
		found++;
	}
	if (found != nkeys) {
		print1(RED "\n[Fail]     map_get(): %d keys" RST, found);
	} else {
		print1(GRN "\n[Pass]     map_get(): %d keys" RST, found);
	}

	if (exitpoint == 3)
		goto out;

	/* remove the items without outer transaction */
	int removed = 0;
	print2(BLU "\n[Info] Remove keys:\n" RST);
	pgl_reset_stats();
	for (int i = 0; i < nkeys; ++i) {
		/*
		 * OBUF: map_remove() does not free the returned item, i.e.
		 * allocated by new_store_item(). Use map_remove_free() can free
		 * it but the function doesn't return the oid for the following
		 * asserts.
		 */
		print2("0x%016lx%s", keys[i], (i + 1) % 5 == 0 ? ",\n" : ", ");
		PMEMoid item = map_remove(mapc, D_RW(root)->map, keys[i]);

		if (OID_IS_NULL(item)) {
			for (int j = 0; j < nkeys; ++j) {
				if (j % 5 == 0) print2("\n");
				print2("0x%016lx  ", keys[j]);
			}
			print1("\nkeys[%d] 0x%016lx not found\n", i, keys[i]);
			abort();
		}
		assert(!OID_IS_NULL(item));

		assert(OID_INSTANCEOF(item, struct store_item));

		removed++;
	}
	if (removed != nkeys) {
		print1(RED "\n[Fail]   map_remove(): %d keys" RST, removed);
	} else {
		print1(GRN "\n[Pass]  map_remove(): %d keys" RST, removed);
	}

	pgl_print_stats();
	if (exitpoint == 4)
		goto out;

	uint64_t old_nkeys = nkeys;

	/* tree should be empty */
	map_foreach(mapc, D_RW(root)->map, dec_keys, NULL);
	assert(old_nkeys == nkeys);

	printf("\n");
	pgl_print_stats();

out:
	if (pangolin_check_pool(pop, 0) != 0)
		print1(RED "\n[Fail] pangolin pool check failed" RST);
	else
		print1(GRN "\n[Pass] pangolin pool check passed" RST);

	if (pangolin_check_objs(pop, 0) != 0)
		print1(RED "\n[Fail] pangolin objs check failed\n" RST);
	else
		print1(GRN "\n[Pass] pangolin objs check passed\n" RST);

	map_ctx_free(mapc);
	pmemobj_close(pop);

	return 0;
}
