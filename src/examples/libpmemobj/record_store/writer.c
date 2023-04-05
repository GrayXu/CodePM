/*
 * Copyright 2018-2019, University of California, San Diego
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
 * writer.c -- write a name-address record
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpmemobj.h>
#include <libpangolin.h>

#include "layout.h"

int
main(int argc, char *argv[])
{
	if (argc != 3) {
		printf("usage: %s file-name [create | pmemobj | pangolin]\n",
			argv[0]);
		return 1;
	}

	int mode = 0; // create
	if (strcmp(argv[2], "pmemobj") == 0) {
		mode = 1; // pmemobj
	} else if (strcmp(argv[2], "pangolin") == 0) {
		mode = 2; // pangolin
	} else if (strcmp(argv[2], "create") != 0) {
		printf("Unknown mode: %s\n", argv[2]);
		return 1;
	}

	const char *old_name = "Hello World";
	const char *old_addr = "Unknown place";

	PMEMobjpool *pop;
	PMEMoid root;

	if (mode == 0) {
		pop = pmemobj_create(argv[1], LAYOUT_NAME, 16 * 1024 * 1024,
			0666);
		if (pop == NULL) {
			perror("pmemobj_create");
			return 1;
		}
		root = pmemobj_root(pop, sizeof(struct record));
	} else {
		pop = pmemobj_open(argv[1], LAYOUT_NAME);
		if (pop == NULL) {
			perror("pmemobj_open");
			return 1;
		}
		root = pmemobj_root(pop, sizeof(struct record));
	}

	if (mode == 0) { // create, NOT CRASH-SAFE, JUST FOR INITIALIZATION
		struct record *prec = pmemobj_direct(root);
		memcpy(prec->name, old_name, strlen(old_name));
		memcpy(prec->addr, old_addr, strlen(old_addr));
	}

	if (mode == 1) { // Libpmemobj, should be crash-safe
		const char *new_name = "John Doe (Pmemobj)";
		const char *new_addr = "John street 1, Los Angeles, California";
		TX_BEGIN(pop) {
			struct record *prec = pmemobj_direct(root);
			pmemobj_tx_add_range_direct(prec->name, MAX_STR_LEN);
			memset(prec->name, 0, MAX_STR_LEN);
			memcpy(prec->name, new_name, strlen(new_name));

			// exit(0); // Emulate a crash.
			// Crashing here is not be a problem because the change
			// made to persistent storage will be fixed by the undo
			// logging.

			pmemobj_tx_add_range_direct(prec->addr, MAX_STR_LEN);
			memset(prec->addr, 0, MAX_STR_LEN);
			memcpy(prec->addr, new_addr, strlen(new_addr));
		} TX_END
	}

	if (mode == 2) { // Libpangolin, should be crash-safe
		const char *new_name = "Alice Bob (Pangolin)";
		const char *new_addr = "Alice street 1, Denver, Colorado";
		PGL_TX_BEGIN(pop) {
			struct record *prec = pgl_tx_open(root);
			memset(prec->name, 0, MAX_STR_LEN);
			memcpy(prec->name, new_name, strlen(new_name));

			memset(prec->addr, 0, MAX_STR_LEN);
			memcpy(prec->addr, new_addr, strlen(new_addr));

			// exit(0); // Emulate a crash.
			// Crashing here is not a problem because changes are
			// not written to persistent storage yet.

			pgl_tx_add_range(prec, 0, MAX_STR_LEN);
			pgl_tx_add_range(prec, MAX_STR_LEN, MAX_STR_LEN);
		} PGL_TX_END
	}

	pmemobj_close(pop);

	return 0;
}
