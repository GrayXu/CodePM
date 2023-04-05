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
 * objbuf_tx.h -- pangolin's objbuf-based transactions
 */

#ifndef OBJBUF_TX_H
#define OBJBUF_TX_H 1

#include "ulog.h"
#include "pangolin.h"
#include "libpangolin.h"

/*
 * A pangolin transaction shares the same stages (enum tx_state, transient) and
 * states (enum pgl_tx_stage, persistent) as a pmemobj transaction.
 */
struct obuf_tx {
	int frozen;
	uint32_t txid;
	uint32_t level;
	PMEMobjpool *pop;
	struct lane *lane;
	enum pgl_tx_stage stage;

	struct obuf_runtime *ort;
	struct obuf_tlocal_slot *ost;
	struct obuf_pool *obp;

	uint32_t actvobjs;
	SLIST_HEAD(, objbuf) txobjs;

	VEC(, struct pobj_action) actions;

	int first_snapshot;
};

struct obuf_tx *
obuf_get_tx(void);

#endif
