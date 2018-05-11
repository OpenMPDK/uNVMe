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

#include<stdlib.h>
#include<stdio.h>
#include<unistd.h>
#include<string.h>

#include "kv_types.h"
#include "kvcache.h"
#include "kvlog.h"

kv_cache g_cache;

/*
 */
int kv_cache_init(){
	int ret = 0;

	ret = art_tree_init(&g_cache.rtree);
	log_debug(KV_LOG_INFO, "[%s] art_tree_init=%d\n",__FUNCTION__,ret);

	ret |= pthread_rwlock_init(&g_cache.tree_rwlock, NULL); /*used for art_search or art_insert*/
#if RECORD_HIT_COUNT
	ret |= pthread_mutex_init(&g_cache.hitcnt_mutex, NULL); /*used for g_cache.stat.hit++*/
#endif
	if (ret) {
		log_debug(KV_LOG_INFO, "[%s] pthread_rwlock_init=%d\n",__FUNCTION__, ret);
		return ret;
	}
	log_debug(KV_LOG_INFO, "[DONE]mutex and rw_lock was enabled\n");

	return ret;
}

/*
 */
int kv_cache_finalize(){	
	int ret = 0;
	ret = art_tree_destroy(&g_cache.rtree);	
	log_debug(KV_LOG_INFO, "[%s] art_tree_destroy=%d\n",__FUNCTION__,ret);

#if RECORD_HIT_COUNT
	fprintf(stderr, "Cache Hit Read Count: %llu\n", g_cache.stat.hit_read);
	fprintf(stderr, "Cache Hit Write Count: %llu\n", g_cache.stat.hit_write);

	g_cache.stat.hit_read = 0;
	g_cache.stat.hit_write = 0; 	
#endif

	pthread_rwlock_destroy(&g_cache.tree_rwlock);
#if RECORD_HIT_COUNT
	pthread_mutex_destroy(&g_cache.hitcnt_mutex);
#endif
	log_debug(KV_LOG_INFO, "[DONE]mutex and rw_lock was destroyed\n");
	return ret;
}

/*
desc :  try to write given key and value into cache entries
return : KV_SUCCESS = cache write success 
 */
int kv_cache_write(kv_pair* kv){
	if(!kv || !kv->key.key || !kv->value.value ){
		return KV_CACHE_ERR_INVALID_PARAM;
	}
	int ret = KV_CACHE_SUCCESS;
	
	check_lock(pthread_rwlock_wrlock(&g_cache.tree_rwlock));

	void * old = art_insert(&g_cache.rtree, kv->key.key, kv->key.length, kv);

	check_lock(pthread_rwlock_unlock(&g_cache.tree_rwlock));
	
	//TODO: control the old kv_pair
	if (old) { }

	return ret;
}

/*
desc : try to read given key and value from cache entries
return : 0 = success , -1 = failure
 */
int kv_cache_read(kv_pair* kv){
	if(!kv || !kv->key.key || !kv->value.value ){
		return KV_CACHE_ERR_INVALID_PARAM;
	}
	int ret = KV_CACHE_SUCCESS;
	kv_key* key = &kv->key;
	kv_value* value = &kv->value;

        check_lock(pthread_rwlock_rdlock(&g_cache.tree_rwlock));

	kv_pair* cache_kv = art_search(&g_cache.rtree, key->key, key->length);

        check_lock(pthread_rwlock_unlock(&g_cache.tree_rwlock));

	if(cache_kv){
		memcpy(kv->value.value,cache_kv->value.value,cache_kv->value.length);
		kv->value.length = cache_kv->value.length;
		memcpy(kv->key.key,cache_kv->key.key,cache_kv->key.length);
		kv->key.length = cache_kv->key.length;
#if RECORD_HIT_COUNT
		pthread_mutex_lock(&g_cache.hitcnt_mutex);
		g_cache.stat.hit_read++;
		pthread_mutex_unlock(&g_cache.hitcnt_mutex);
#endif
	}else{
		ret = KV_CACHE_ERR_NO_CACHED_KEY;
	}
	return ret;
}

/*
return : 0 = success , -1 = failure
 */
int kv_cache_delete(kv_pair* kv){
	if(!kv || !kv->key.key ){
		return KV_CACHE_ERR_INVALID_PARAM;
	}
	int ret = KV_CACHE_SUCCESS;
	kv_key* key = &kv->key;

	check_lock(pthread_rwlock_wrlock(&g_cache.tree_rwlock));

	kv_pair* deleted = art_delete(&g_cache.rtree, key->key, key->length);

        check_lock(pthread_rwlock_unlock(&g_cache.tree_rwlock));

	if(deleted){
		//g_cache.mm.free_fn(deleted);
	}
	else{
		ret = KV_CACHE_ERR_NO_CACHED_KEY;
	}
	return ret;
}
