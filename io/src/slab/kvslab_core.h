/**
 *   BSD LICENSE
 *
 *   Copyright (c) 2017 Samsung Electronics Co., Ltd.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Samsung Electronics Co., Ltd. nor the names of
 *       its contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
/*
 * kvslab_core.h
 */

#ifndef KVSLAB_CORE_H_
#define KVSLAB_CORE_H_

typedef int kvsl_rstatus_t; /* return type */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>
#include <stddef.h>
#include <stdbool.h>
#include <inttypes.h>

#include <sys/mman.h>
#include <sys/types.h>
#include <errno.h>

#include "kvslab.h"
#include "kvlog.h"
#include <kvslab_util.h>


#define KVSLAB_OK        0
#define KVSLAB_ERROR    -1
#define KVSLAB_EAGAIN   -2
#define KVSLAB_ENOMEM   -3

#define KVSLAB_FACTOR           (1.25)
#define KVSLAB_CHUNK_SIZE       KVSLAB_ITEM_CHUNK_SIZE
#define KVSLAB_SLAB_MEMORY      (64 * MB)
#define KVSLAB_SLABCLASS_MAX_IDS       UCHAR_MAX

#define HUGEPAGE_SIZE (4ULL*1024*1024)
#define MAX_NUM_SLAB_CLASS (11)
#define MIN_TOTAL_SLAB_SIZE (HUGEPAGE_SIZE*MAX_NUM_SLAB_CLASS)

struct kvsl_settings {
	double factor;				/* item chunk size growth factor */
	size_t max_slab_memory;			/* maximum memory allowed for slabs in bytes */
	size_t chunk_size;			/* minimum item chunk size */
	size_t max_chunk_size;			/* maximum item chunk size */
	size_t slab_size;			/* slab size */
	size_t profile[KVSLAB_SLABCLASS_MAX_IDS];   /* slab profile */
	uint8_t profile_last_id;		/* last id in slab profile */
	int slab_alloc_policy;			/* malloc or huge page alloc */
	int nr_slab;				/* number of slab */
};


void kvsl_set_options(bool use_default, double factor, size_t max_slab_memory, size_t chunk_size, size_t slab_size, int slab_alloc_policy, int nr_slab);
struct kvsl_item* kvsl_get_free_item(uint32_t size, uint8_t did);


struct kvsl_item {
	uint32_t magic;		/* item magic (const), (item_get) */
	uint32_t offset;	/* raw offset from owner slab base (const), (slab_get_item) */
	uint32_t sid;		/* slab id (const), (_slab_get_item) */
	uint8_t cid;		/* slab class id (const), (item_get) */
	uint8_t did;		/* (multi slab) device(slab) id*/
	uint8_t evicted;	/* valid or invalid item, (item_get or slab_evict) */
	uint8_t unused[1];	/* unused */
	uint32_t ndata;		/* date length */
	uint8_t end[1];		/* item data */
};


#define KVSLAB_ITEM_MAGIC      0xfeedface
#define KVSLAB_ITEM_HDR_SIZE   offsetof(struct kvsl_item, end)
#define KVSLAB_ITEM_PAYLOAD_SIZE       32
#define KVSLAB_ITEM_CHUNK_SIZE         \
    KV_MEM_ALIGN(KVSLAB_ITEM_HDR_SIZE + KVSLAB_ITEM_PAYLOAD_SIZE, KVSLAB_ALIGNMENT)


static inline uint8_t * kvsl_item_data(struct kvsl_item *it){
	KVSLAB_ASSERT(it->magic == KVSLAB_ITEM_MAGIC);
	return it->end;
}

static inline struct kvsl_item * kvsl_data_item(void *data){
	struct kvsl_item* item = (struct kvsl_item*)((uint8_t *)data - KVSLAB_ITEM_HDR_SIZE);
	KVSLAB_ASSERT(item->magic == KVSLAB_ITEM_MAGIC);
	return item;
}


#define KVSLAB_SLAB_MAGIC      0xdeadbeef
#define KVSLAB_SLAB_HDR_SIZE   offsetof(struct kvsl_slab, data)
#define KVSLAB_SLAB_MIN_SIZE   ((size_t) MB)
#define KVSLAB_SLAB_SIZE       MB
#define KVSLAB_SLAB_MAX_SIZE   ((size_t) (512 * MB))


#define KVSLAB_SLABCLASS_MIN_ID        0
#define KVSLAB_SLABCLASS_MAX_ID        (UCHAR_MAX - 1)
#define KVSLAB_SLABCLASS_INVALID_ID    UCHAR_MAX
#define KVSLAB_SLABCLASS_MAX_IDS       UCHAR_MAX


struct kvsl_slab {
	uint32_t magic;		/* slab magic (const) */
	uint32_t sid;		/* slab id */
	uint8_t cid;		/* slab class id */
	uint8_t did;		/* (multi slab) device(slab) id*/
	uint8_t unused[2];	/* unused */
	uint8_t data[1];	/* opaque data */
};


struct kvsl_slabinfo {
	uint32_t sid;				/* slab id (const) */
	void *addr;				/* kvsl_slab address */
	KVSLAB_TAILQ_ENTRY(kvsl_slabinfo) tqe;	/* link in free q / partial q / full q */
	uint32_t nalloc;			/* # item alloced (monotonic) */
	uint8_t cid;				/* class id */
	uint8_t did;
	uint32_t in_use_cnt;			/*used item cnt*/
	pthread_mutex_t in_use_cnt_mutex;	/*used item cnt mutex*/
};


KVSLAB_TAILQ_HEAD(kvsl_slabhinfo, kvsl_slabinfo);	/* make slab head info structure */


struct kvsl_slabclass {
	uint32_t nitem;		/* # item per slab (const) */
	size_t size;		/* item size (const) */
	size_t slack;		/* unusable slack space (const) */
	struct kvsl_slabhinfo *partial_msinfoq; /* partial slabinfo q */
	uint32_t *nmslab;	/* # memory slab */
	uint64_t *nevict;	/* # eviect time */
};


kvsl_rstatus_t kvsl_slab_init(void);
kvsl_rstatus_t kvsl_slab_deinit(void);

void print_slab_class_info(bool print_detail);
void print_slab_info(void);

void kvsl_slab_increase_use_cnt(uint8_t did, uint32_t sid);
void kvsl_slab_decrease_use_cnt(uint8_t did, uint32_t sid);

#endif /* KVSLAB_CORE_H_ */
