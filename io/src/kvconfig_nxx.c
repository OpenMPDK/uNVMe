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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <math.h>
#include <time.h>
#include <pthread.h>
#include <string.h>
#include <errno.h>

#include "kv_types.h"
#include "kv_apis.h"
#include "kvconfig_nxx.h"

#define NOT_SET (0)
#define KV_DEFAULT_NUM_DEVICES_PER_CQ_CORE (4)          /* 1 CQ core per 4 devices */
#define KV_DEFAULT_PORTION_CQ_CORES (1/4.0f)            /* preserve 1/4 of total system cores as cq */

typedef struct {
	uint8_t sync_cores[MAX_CPU_CORES];
	uint8_t async_cores[MAX_CPU_CORES];
	uint8_t next_sync_idx;
	uint8_t next_async_idx;
	uint8_t num_sync_cores;
	uint8_t num_async_cores;
	pthread_mutex_t sync_core_mutex;
	pthread_mutex_t async_core_mutex;
}kv_core_allocator;
kv_core_allocator core_allocator[NR_MAX_SSD];
int g_num_devices;

int kv_get_num_cores(void){
	return (int)sysconf(_SC_NPROCESSORS_ONLN);
}

int kv_get_sync_core(int did){
	if (did < 0 || did >= g_num_devices){
		fprintf(stderr, "[%s] Invaild parameter\n", __FUNCTION__);
		return -1;
	}

	kv_core_allocator *allocator = &core_allocator[did];
	if (allocator->num_sync_cores == 0){
		fprintf(stderr, "[%s] This device does not support sync I/O\n", __FUNCTION__);
		return -1;
	}

	pthread_mutex_lock(&allocator->sync_core_mutex);
	uint8_t core = allocator->sync_cores[allocator->next_sync_idx];
	allocator->next_sync_idx = (allocator->next_sync_idx + 1) % allocator->num_sync_cores;
	pthread_mutex_unlock(&allocator->sync_core_mutex);
	return (int)core;
}

int kv_get_async_core(int did){
	if (did < 0 || did >= g_num_devices){
		fprintf(stderr, "[%s] Invaild parameter\n", __FUNCTION__);
		return -1;
	}

	kv_core_allocator *allocator = &core_allocator[did];
	if (allocator->num_async_cores == 0){
		fprintf(stderr, "[%s] This device does not support async I/O\n", __FUNCTION__);
		return -1;
	}

	pthread_mutex_lock(&allocator->async_core_mutex);
	uint8_t core = allocator->async_cores[allocator->next_async_idx];
	allocator->next_async_idx = (allocator->next_async_idx + 1) % allocator->num_async_cores;
	pthread_mutex_unlock(&allocator->async_core_mutex);
	return (int)core;
}

static void kv_set_sdk_nxx_default(kv_sdk *sdk_opt){
        sdk_opt->slab_size = KV_DEFAULT_SLAB_SIZE_PER_DEV;
        sdk_opt->use_cache = false;
        sdk_opt->cache_algorithm = CACHE_ALGORITHM_RADIX;;
        sdk_opt->cache_reclaim_policy = CACHE_RECLAIM_LRU;
        sdk_opt->slab_alloc_policy = SLAB_MM_ALLOC_HUGE;
        sdk_opt->ssd_type = KV_TYPE_SSD;
        sdk_opt->log_level = 0;
        strcpy(sdk_opt->log_file, "/tmp/kvsdk.log");
}

/**
 * NOTICE:
 *   when user set core_mask with non-zero value, sync_mask is also considered as specified
 *   even if the value of sync_mask is zero (means, non-zero value of sync_mask will be ignored if user's core_mask is zero)
 */
static void kv_copy_dev_config(kv_sdk *dst, kv_sdk *src){
	dst->nr_ssd = src->nr_ssd;
	for(int i = 0; i < dst->nr_ssd; i++){
		memcpy(dst->dev_id[i], src->dev_id[i], DEV_ID_LEN);
		if (src->dd_options[i].core_mask){
			dst->dd_options[i].core_mask = src->dd_options[i].core_mask;
			dst->dd_options[i].sync_mask = src->dd_options[i].sync_mask;
			dst->dd_options[i].cq_thread_mask = src->dd_options[i].cq_thread_mask;
			dst->dd_options[i].num_cq_threads = 0;
                        for(int j = 0; j < MAX_CPU_CORES; j++){
                                if (dst->dd_options[i].cq_thread_mask & (1ULL << j)){
                                        dst->dd_options[i].num_cq_threads++;
                                }
                        }
		} else {
			dst->dd_options[i].core_mask = NOT_SET;
			dst->dd_options[i].sync_mask = NOT_SET;
			dst->dd_options[i].num_cq_threads = NOT_SET;
			dst->dd_options[i].cq_thread_mask = NOT_SET;
		}

		if (src->dd_options[i].queue_depth){
			dst->dd_options[i].queue_depth = src->dd_options[i].queue_depth;
		} else {
			dst->dd_options[i].queue_depth = NOT_SET;
		}
	}
}

/**
 * if detail descriptions of devices are NOT_SET, set default values
 */
static int kv_set_dev_nxx_default(kv_sdk *sdk_opt){
	int ret = KV_SUCCESS;
	int num_devices = sdk_opt->nr_ssd;
	int num_cores, num_sync_cores, num_cq_cores, num_cq_cores_used, num_submit_cores;
	int num_dev_per_cur_cq_core, cur_cq_core;
	uint64_t total_cq_mask = 0;

	num_cores = kv_get_num_cores();
	assert(num_cores > 0);
	num_cq_cores = (int)(num_cores * KV_DEFAULT_PORTION_CQ_CORES);
	assert(num_cq_cores > 0);

	num_sync_cores = (num_cores - num_cq_cores) / 2;
	assert(num_sync_cores);

	num_submit_cores = num_cores - num_cq_cores;

	num_dev_per_cur_cq_core = 0;
	cur_cq_core = rand() % num_cq_cores + num_submit_cores;
	total_cq_mask |= (1 << cur_cq_core);

	num_cq_cores_used = 0;
        for(int i = 0; i < num_devices; i++){
		if (sdk_opt->dd_options[i].core_mask == NOT_SET) {
			sdk_opt->dd_options[i].core_mask = (uint64_t)(pow(2.0, num_submit_cores)-1);
	                sdk_opt->dd_options[i].sync_mask = (uint64_t)(pow(2.0, num_sync_cores)-1);
			sdk_opt->dd_options[i].cq_thread_mask = (uint64_t)(pow(2.0, cur_cq_core));
			sdk_opt->dd_options[i].num_cq_threads = (uint64_t)0x1;

			num_dev_per_cur_cq_core++;
			if (num_dev_per_cur_cq_core >= KV_DEFAULT_NUM_DEVICES_PER_CQ_CORE){
				int cq_core;
				if (++num_cq_cores_used == num_cq_cores){
					num_cq_cores_used = 0;
					total_cq_mask = 0;
					cq_core = rand() % num_cq_cores + num_submit_cores;
				} else {
					cq_core = cur_cq_core;
				}
				while ((total_cq_mask & (1 << cq_core)) || (cq_core == 0)){
					cq_core = rand() % num_cq_cores + num_submit_cores;
				}
				cur_cq_core = cq_core;
				total_cq_mask |= (1 << cur_cq_core);
				num_dev_per_cur_cq_core = 0;
			}
		}
		if (sdk_opt->dd_options[i].queue_depth == NOT_SET){
			sdk_opt->dd_options[i].queue_depth = KV_DEFAULT_IO_QUEUE_SIZE;
		}
	}

exit:
	return ret;
}

static int kv_set_dev_nxx(kv_sdk *sdk_opt, int init_from, void *option){
	int ret = KV_SUCCESS, i = 0;
	switch(init_from){
		case KV_SDK_INIT_FROM_STR:
		{
			kv_sdk *user_opt = (kv_sdk *)option;
			user_opt->nr_ssd = 0;
			while((i < NR_MAX_SSD) && (strlen(user_opt->dev_id[i]) > 0)){
				user_opt->nr_ssd++;
				i++;
			}
			if (user_opt->nr_ssd <= 0){
				ret = KV_ERR_SDK_OPEN;
				goto exit;
			}
			kv_copy_dev_config(sdk_opt, user_opt);
			break;
		}
		case KV_SDK_INIT_FROM_JSON:
		{
			kv_sdk user_opt;
			memset(&user_opt, 0, sizeof(kv_sdk));
			ret = kv_sdk_load_option(&user_opt, (char *)option);
			if (ret != KV_SUCCESS || user_opt.nr_ssd <= 0){
				ret = KV_ERR_SDK_OPEN;
				goto exit;
			}
			kv_copy_dev_config(sdk_opt, &user_opt);
			break;
		}
		default:
			break;
	}
	ret = kv_set_dev_nxx_default(sdk_opt);

exit:
	return ret;
}

static void kv_init_core_allocator(kv_sdk *sdk_opt){
	g_num_devices = sdk_opt->nr_ssd;
	kv_core_allocator *allocator;
	uint8_t core_id;

	for(int i = 0; i < g_num_devices; i++){
		allocator = &core_allocator[i];

		pthread_mutex_init(&allocator->sync_core_mutex, NULL);
		pthread_mutex_init(&allocator->async_core_mutex, NULL);

		uint64_t core_mask = sdk_opt->dd_options[i].core_mask;
		uint64_t sync_mask = sdk_opt->dd_options[i].sync_mask;
		allocator->num_sync_cores = allocator->num_async_cores = 0;
		for(int j = 0; j < MAX_CPU_CORES; j++){
			if (core_mask & (1ULL << j)){
				if (sync_mask & (1ULL << j)){
					allocator->sync_cores[allocator->num_sync_cores++] = j;
				} else {
					allocator->async_cores[allocator->num_async_cores++] = j;
				}
			}
		}

		if(allocator->num_sync_cores){
			allocator->next_sync_idx = rand() % allocator->num_sync_cores;
		}
		if(allocator->num_async_cores){
			allocator->next_async_idx = rand() % allocator->num_async_cores;
		}
	}
	return;
}

static void kv_print_config_info(kv_sdk *sdk_opt){
	for(int i = 0; i < sdk_opt->nr_ssd; i++){
		printf("[dev_opt][dev_id=%d] bdf=%s, cm=0x%lX, sm=0x%lX, num_cq=%lu, cm=0x%lX qd=%u\n", i, \
			sdk_opt->dev_id[i], sdk_opt->dd_options[i].core_mask, sdk_opt->dd_options[i].sync_mask, \
			sdk_opt->dd_options[i].num_cq_threads, sdk_opt->dd_options[i].cq_thread_mask, sdk_opt->dd_options[i].queue_depth);

		kv_core_allocator *allocator = &core_allocator[i];
		printf(" num_sync_cores=%u(", allocator->num_sync_cores);
		for(int j = 0; j < allocator->num_sync_cores; j++) printf("%d,", allocator->sync_cores[j]);
		printf(")\n");
		if (allocator->num_sync_cores) printf(" next_sync_idx=%u\n", allocator->next_sync_idx);

		printf(" num_async_cores=%u(", allocator->num_async_cores);
		for(int j = 0; j < allocator->num_async_cores; j++) printf("%d,", allocator->async_cores[j]);
		printf(")\n");
		if (allocator->num_async_cores) printf(" next_async_idx=%u\n", allocator->next_async_idx);
	}
}

int kv_sdk_load_nxx_config(kv_sdk *sdk_opt, int init_from, void *option){
	int ret = KV_SUCCESS;
	srand(getpid());

	ret = kv_set_dev_nxx(sdk_opt, init_from, option);	//set dev descriptions
	if (ret != KV_SUCCESS) {
		goto exit;
	}
	kv_set_sdk_nxx_default(sdk_opt);			//set sdk options

	kv_init_core_allocator(sdk_opt);
	//kv_print_config_info(sdk_opt);
exit:
	return ret;
}
