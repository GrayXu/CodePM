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
 * cuckoo.c -- implementation of cuckoo hash table
 */
#include <stdint.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>

#include "cuckoo.h"
#include "out.h"

#define MAX_HASH_FUNCS 2

#define GROWTH_FACTOR 1.2f
#define INITIAL_SIZE 8
#define MAX_INSERTS 8
#define MAX_GROWS 32

struct cuckoo_slot {
	uint64_t key;
	void *value;
};

struct cuckoo {
	size_t size; /* number of hash table slots */
	struct cuckoo_slot *tab;
};

static const struct cuckoo_slot null_slot = {0, NULL};

/*
 * hash_mod -- (internal) first hash function
 */
static size_t
hash_mod(struct cuckoo *c, uint64_t key)
{
	return key % c->size;
}

/*
 * hash_mixer -- (internal) second hash function
 *
 * Based on Austin Appleby MurmurHash3 64-bit finalizer.
 */
static size_t
hash_mixer(struct cuckoo *c, uint64_t key)
{
	key ^= key >> 33;
	key *= 0xff51afd7ed558ccd;
	key ^= key >> 33;
	key *= 0xc4ceb9fe1a85ec53;
	key ^= key >> 33;
	return key % c->size;
}

static size_t
(*hash_funcs[MAX_HASH_FUNCS])(struct cuckoo *c, uint64_t key) = {
	hash_mod,
	hash_mixer
};

/*
 * cuckoo_new -- allocates and initializes cuckoo hash table
 */
struct cuckoo *
cuckoo_new(void)
{
	COMPILE_ERROR_ON((size_t)(INITIAL_SIZE * GROWTH_FACTOR)
		== INITIAL_SIZE);

	struct cuckoo *c = Malloc(sizeof(struct cuckoo));
	if (c == NULL) {
		ERR("!Malloc");
		goto error_cuckoo_malloc;
	}

	c->size = INITIAL_SIZE;
	size_t tab_rawsize = c->size * sizeof(struct cuckoo_slot);
	c->tab = Zalloc(tab_rawsize);
	if (c->tab == NULL)
		goto error_tab_malloc;

	return c;

error_tab_malloc:
	Free(c);
error_cuckoo_malloc:
	return NULL;
}

/*
 * cuckoo_delete -- cleanups and deallocates cuckoo hash table
 */
void
cuckoo_delete(struct cuckoo *c)
{
	ASSERTne(c, NULL);
	Free(c->tab);
	Free(c);
}

/*
 * cuckoo_insert_try -- (internal) try inserting into the existing hash table
 */
static int
cuckoo_insert_try(struct cuckoo *c, struct cuckoo_slot *src)
{
	struct cuckoo_slot srct;
	size_t h[MAX_HASH_FUNCS] = {0};
	for (int n = 0; n < MAX_INSERTS; ++n) {
		for (int i = 0; i < MAX_HASH_FUNCS; ++i) {
			h[i] = hash_funcs[i](c, src->key);
			if (c->tab[h[i]].value == NULL) {
				c->tab[h[i]] = *src;
				return 0;
			} else if (c->tab[h[i]].key == src->key) {
				return EINVAL;
			}
		}

		srct = c->tab[h[0]];
		c->tab[h[0]] = *src;
		src->key = srct.key;
		src->value = srct.value;
	}

	return EAGAIN;
}

/*
 * cuckoo_grow -- (internal) rehashes the table with GROWTH_FACTOR * size
 */
static int
cuckoo_grow(struct cuckoo *c)
{
	size_t oldsize = c->size;
	struct cuckoo_slot *oldtab = c->tab;

	int n;
	for (n = 0; n < MAX_GROWS; ++n) {
		size_t nsize = (size_t)((float)c->size * GROWTH_FACTOR);

		size_t tab_rawsize = nsize * sizeof(struct cuckoo_slot);
		c->tab = Zalloc(tab_rawsize);
		if (c->tab == NULL) {
			c->tab = oldtab;
			return ENOMEM;
		}

		c->size = nsize;
		unsigned i;
		for (i = 0; i < oldsize; ++i) {
			struct cuckoo_slot s = oldtab[i];
			if (s.value != NULL && (cuckoo_insert_try(c, &s) != 0))
				break;
		}

		if (i == oldsize)
			break;
		else
			Free(c->tab);
	}

	if (n == MAX_GROWS) {
		c->tab = oldtab;
		c->size = oldsize;
		return EINVAL;
	}

	Free(oldtab);
	return 0;
}

/*
 * cuckoo_insert -- inserts key-value pair into the hash table
 */
int
cuckoo_insert(struct cuckoo *c, uint64_t key, void *value)
{
	ASSERTne(c, NULL);
	int err;
	struct cuckoo_slot src = {key, value};
	for (int n = 0; n < MAX_GROWS; ++n) {
		if ((err = cuckoo_insert_try(c, &src)) != EAGAIN)
			return err;

		if ((err = cuckoo_grow(c)) != 0)
			return err;
	}

	return EINVAL;
}

/*
 * cuckoo_find_slot -- (internal) finds the hash table slot of key
 */
static struct cuckoo_slot *
cuckoo_find_slot(struct cuckoo *c, uint64_t key)
{
	for (int i = 0; i < MAX_HASH_FUNCS; ++i) {
		size_t h = hash_funcs[i](c, key);
		if (c->tab[h].key == key)
			return &c->tab[h];
	}

	return NULL;
}

/*
 * cuckoo_remove -- removes key-value pair from the hash table
 */
void *
cuckoo_remove(struct cuckoo *c, uint64_t key)
{
	ASSERTne(c, NULL);
	void *ret = NULL;
	struct cuckoo_slot *s = cuckoo_find_slot(c, key);
	if (s) {
		ret = s->value;
		*s = null_slot;
	}

	return ret;
}

/*
 * cuckoo_get -- returns the value of a key
 */
void *
cuckoo_get(struct cuckoo *c, uint64_t key)
{
	ASSERTne(c, NULL);
	struct cuckoo_slot *s = cuckoo_find_slot(c, key);
	return s ? s->value : NULL;
}

/*
 * cuckoo_get_size -- returns the size of the underlying table, useful for
 *	calculating load factor and predicting possible rehashes
 */
size_t
cuckoo_get_size(struct cuckoo *c)
{
	ASSERTne(c, NULL);
	return c->size;
}

/*
 * cuckoo_foreach_cb -- iterate through cuckoo slots and callback to process
 */
void
cuckoo_foreach_cb(struct cuckoo *c, cuckoo_slot_cb callback, int remove)
{
	ASSERTne(callback, NULL);

	for (size_t i = 0; i < c->size; i++) {
		struct cuckoo_slot *slot = &c->tab[i];
		if (slot->key != 0 && slot->value != NULL) {
			callback(slot->key, slot->value);
			if (remove)
				*slot = null_slot;
		}
	}
}
