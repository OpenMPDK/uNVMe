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

#ifndef _KVCACHE_H_
#define _KVCACHE_H_

#include <stdint.h>
#include <pthread.h>
#include "kv_types.h"
#include "kv_apis.h"
#include "kvradix.h"
#include "kvslab.h"

#define RECORD_HIT_COUNT 0 /*record and check hit count*/

#define check_lock(r) if(r!=0) {                        \
	fprintf(stderr,"multi-threads lock err=%d\n", r);\
        exit(-1);                                       \
}    

#ifdef __cplusplus
extern "C" {
#endif

typedef struct statistics{
        unsigned long long total_write;
        unsigned long long total_read;
        unsigned long long hit_read;
        unsigned long long hit_write;
}statistics;

typedef struct kv_cache{
	art_tree rtree;
	pthread_rwlock_t tree_rwlock;
#if RECORD_HIT_COUNT
	statistics stat;
	pthread_mutex_t hitcnt_mutex;
#endif
}kv_cache;

enum kv_cache_result{
	KV_CACHE_SUCCESS = 0,
	KV_CACHE_ERR_NO_CACHED_KEY=-1,
	KV_CACHE_ERR_INVALID_PARAM = -2,
	KV_CACHE_ERR_ALLOC_FAILURE = -3
};

int kv_cache_init();
int kv_cache_finalize();
int kv_cache_write(kv_pair* pair);
int kv_cache_read(kv_pair* pair);
int kv_cache_delete(kv_pair* pair);

#ifdef __cplusplus
}
#endif

#endif
