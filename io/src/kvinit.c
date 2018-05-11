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
#include <string.h>
#include <fcntl.h>
#include <inttypes.h>
#include <syslog.h>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <errno.h>

#include "kv_apis.h"
#include "kvcache.h"
#include "kvnvme.h"
#include "kvutil.h"
#include "kvlog.h"

#include "spdk/json.h"

#include "kvconfig_nxx.h"

#define NUM_PARAMS (256)
#define KV_ERR_SDK_OPTION_LOAD (-1)

kv_sdk g_sdk;
int g_kvsdk_ref_count = 0;
static pthread_mutex_t g_init_mutex = PTHREAD_MUTEX_INITIALIZER;


static void parse_description(struct spdk_json_val* values, int idx, kv_nvme_io_options* opt_dst, char* dev_id){
	int dev_values_cnt = values[idx].len;
	kv_nvme_io_options opt = {0,};

	idx++;
	for(int i = idx; i < idx+dev_values_cnt; i++){
	        if (memcmp(values[i].start, "dev_id", values[i].len) == 0) {
			i++;
                        memcpy(dev_id, values[i].start, values[i].len);
                }
	        if (memcmp(values[i].start, "core_mask", values[i].len) == 0) {
			i++;
                        uint64_t val = (uint64_t)strtoull((char*)values[i].start, NULL, 16);
			if (!val) continue;
                        opt.core_mask = val;
                }
	        if (memcmp(values[i].start, "sync_mask", values[i].len) == 0) {
			i++;
                        uint64_t val = (uint64_t)strtoull((char*)values[i].start, NULL, 16);
			if (!val) continue;
                        opt.sync_mask = val;
                }
	        if (memcmp(values[i].start, "cq_thread_mask", values[i].len) == 0) {
			i++;
                        uint64_t val = (uint64_t)strtoull((char*)values[i].start, NULL, 16);
			if (!val) continue;
                        opt.cq_thread_mask = val;
                        opt.num_cq_threads = 0;
                        for(int i = 0; i < MAX_CPU_CORES; i++){
                                if(opt.cq_thread_mask & (1ULL << i)){
                                        opt.num_cq_threads++;
                                }
                        }
                }
	        if (memcmp(values[i].start, "queue_depth", values[i].len) == 0) {
			i++;
			uint32_t qd;
			spdk_json_decode_uint32(&values[i], &qd);
                        opt.queue_depth = qd;
                }

	}

        if (opt.core_mask && opt.cq_thread_mask){
                //set options only when core_mask, (sync_mask), and cq_mask are set all.
                //else, use default.
                opt_dst->core_mask = opt.core_mask;
                opt_dst->sync_mask = opt.sync_mask;
                opt_dst->cq_thread_mask = opt.cq_thread_mask;
        }
        if (opt.queue_depth){
                opt_dst->queue_depth = opt.queue_depth;
        }

}


void dump_kv_sdk_options(void){
	fprintf(stderr, "----kv sdk options----\n");
	fprintf(stderr, "use_cache: %d \t\t(0: false, 1: true)\n", g_sdk.use_cache);
	fprintf(stderr, "cache algorithm: %d \t(0: radix)\n", g_sdk.cache_algorithm);
	fprintf(stderr, "cache reclaim policy: %d (0: LRU)\n", g_sdk.cache_reclaim_policy);
	fprintf(stderr, "slab size: %lu \t(%luMB)\n", g_sdk.slab_size, g_sdk.slab_size/1024/1024);
	fprintf(stderr, "ssd type: %d \t\t(0: kv, 1: lba)\n", g_sdk.ssd_type);
	fprintf(stderr, "polling interval: %d \t\t(us unit)\n", g_sdk.polling_interval);
	fprintf(stderr, "nr ssd : %d\n", g_sdk.nr_ssd);
	for(int i=0;i<g_sdk.nr_ssd;i++){
		fprintf(stderr, "\tdevice id[%d]: %s\n", i, g_sdk.dev_id[i]);
		fprintf(stderr, "\tcore mask: %08lx\n", g_sdk.dd_options[i].core_mask);
		fprintf(stderr, "\tsync mask: %08lx\n", g_sdk.dd_options[i].sync_mask);
		fprintf(stderr, "\tnum_cq_threads: %ld\n", g_sdk.dd_options[i].num_cq_threads);
		fprintf(stderr, "\tcq_thread_mask: %08lx\n", g_sdk.dd_options[i].cq_thread_mask);
		fprintf(stderr, "\tqueue_depth: %d\n\n", g_sdk.dd_options[i].queue_depth);
	}

	fprintf(stderr, "log level: %d\n", g_sdk.log_level);
	fprintf(stderr, "log file: %s\n", g_sdk.log_file);
	fprintf(stderr, "----------------------\n");
}


void kv_sdk_load_default(kv_sdk *sdk_opt){
	sdk_opt->use_cache = false;
	sdk_opt->cache_algorithm = CACHE_ALGORITHM_RADIX;;
	sdk_opt->cache_reclaim_policy = CACHE_RECLAIM_LRU;
	sdk_opt->slab_size = 512*1024*1024;
	sdk_opt->slab_alloc_policy = SLAB_MM_ALLOC_HUGE;
	sdk_opt->ssd_type = KV_TYPE_SSD;
	sdk_opt->polling_interval = 1;
	sdk_opt->log_level = 0;
	strcpy(sdk_opt->log_file, "/tmp/kvsdk.log");

	sdk_opt->nr_ssd = 1;
	strcpy(sdk_opt->dev_id[0],"0000:02:00.0");
	sdk_opt->dd_options[0].core_mask = 0x01;
	sdk_opt->dd_options[0].sync_mask = 0x00;
	sdk_opt->dd_options[0].num_cq_threads = 0x01;
	sdk_opt->dd_options[0].cq_thread_mask = 0x02;
	sdk_opt->dd_options[0].queue_depth = 64;
}


int kv_sdk_load_option(kv_sdk* sdk_opt, char* log_path){ //json parser using SPDK lib
	int ret = KV_SUCCESS;
        int i, count;

        FILE *fp;
        char *json_line = NULL;
        long int len = 0;
        size_t read;

	struct spdk_json_val values[NUM_PARAMS];

	if (log_path == NULL)
		return KV_ERR_SDK_OPTION_LOAD;

        fp = fopen(log_path, "r");
        if (fp == NULL) {
                fprintf(stderr, "Error to open json file: %s, %s\n",log_path, strerror(errno));
                return KV_ERR_SDK_OPTION_LOAD;
        }

        fseek(fp, 0, SEEK_END);
        len = ftell(fp);
        if (len == -1) {
                fprintf(stderr, "Error to calculate a length of json file: %s, %s\n",log_path, strerror(errno));
                return KV_ERR_SDK_OPTION_LOAD;
        }
        rewind(fp);

        json_line = (char *)calloc(sizeof(char), len+1);
        if (json_line == NULL) {
                fclose(fp);
                return KV_ERR_SDK_OPTION_LOAD;
        }

        read = fread(json_line, 1, len, fp);
        if (read != len) {
                ret = KV_ERR_SDK_OPTION_LOAD;
                fprintf(stderr, "Error to read json file: %s, %s\n",log_path, strerror(errno));
                goto exit;
        }

	uint32_t parse_flags = 0;
	void* end;
	ssize_t num_values;
	parse_flags |= SPDK_JSON_PARSE_FLAG_ALLOW_COMMENTS;
	num_values = spdk_json_parse(json_line, len, values, NUM_PARAMS, end, parse_flags);
	if (num_values < 0) {
		fprintf(stderr, "Error to parse json file: %s (err=%ld)\n", log_path, num_values);
		ret = KV_ERR_SDK_OPTION_LOAD;
		goto exit;
	}

	for(int i = 0; i < num_values; i++){
		switch(values[i].type){
			case SPDK_JSON_VAL_NAME:
			{
				if (memcmp(values[i].start, "cache", values[i].len) == 0) {
					i++;
					if (memcmp(values[i].start, "on", values[i].len) == 0) {
						sdk_opt->use_cache = true;
					} else if (memcmp(values[i].start, "off", values[i].len) == 0) {
						sdk_opt->use_cache = false;
					} else {
						fprintf(stderr, "Unknown cache on/off option: %.*s\n", values[i].len, (char*)values[i].start);
	                                        ret = KV_ERR_SDK_OPTION_LOAD;
		                                goto exit;
			                }
				}
				else if (memcmp(values[i].start, "cache_algorithm", values[i].len) == 0) {
					i++;
					sdk_opt->cache_algorithm = 0;
				}
				else if (memcmp(values[i].start, "cache_reclaim_policy", values[i].len) == 0) {
					i++;
					if (memcmp(values[i].start, "lru", values[i].len) == 0) {
	                                        sdk_opt->cache_reclaim_policy = 0;
					} else {
	                                        fprintf(stderr, "Unknown cache reclaim policy: %.*s\n", values[i].len, (char*)values[i].start);
		                                ret = KV_ERR_SDK_OPTION_LOAD;
			                        goto exit;
					}
                                }
				else if (memcmp(values[i].start, "slab_size", values[i].len) == 0) {
					i++;
					uint32_t slab_size = 0;
					spdk_json_decode_uint32(&values[i], &slab_size);
					sdk_opt->slab_size = slab_size * 1024 * 1024;
				}
				else if (memcmp(values[i].start, "slab_alloc_policy", values[i].len) == 0) {
					i++;
	                                if (memcmp(values[i].start, "huge", values[i].len) == 0) {
	                                        sdk_opt->slab_alloc_policy = SLAB_MM_ALLOC_HUGE;
					} else if (memcmp(values[i].start, "posix", values[i].len) == 0) {
						sdk_opt->slab_alloc_policy = SLAB_MM_ALLOC_POSIX;
					} else {
	                                        fprintf(stderr, "Unknown slab alloc policy: %.*s\n", values[i].len, (char*)values[i].start);
		                                ret = KV_ERR_SDK_OPTION_LOAD;
			                        goto exit;
	                                }
				}
				else if (memcmp(values[i].start, "ssd_type", values[i].len) == 0) {
					i++;
	                                if (memcmp(values[i].start, "kv", values[i].len) == 0) {
						sdk_opt->ssd_type = KV_TYPE_SSD;
	                                } else if (memcmp(values[i].start, "lba", values[i].len) == 0) {
		                                sdk_opt->ssd_type = LBA_TYPE_SSD;
			                } else {
				                fprintf(stderr, "Unknown ssd type: %.*s\n", values[i].len, (char*)values[i].start);
					        sdk_opt->ssd_type = KV_TYPE_SSD;
						ret = KV_ERR_SDK_OPTION_LOAD;
	                                        goto exit;
		                        }
			        }
				else if (memcmp(values[i].start, "polling_interval", values[i].len) == 0) {
					i++;
					uint32_t max_interval = 1000;
					uint32_t polling_interval = 0;
					spdk_json_decode_uint32(&values[i], &polling_interval);
					sdk_opt->polling_interval = polling_interval > max_interval ? max_interval : polling_interval;
				}
				else if (memcmp(values[i].start, "log_level", values[i].len) == 0) {
					i++;
					uint32_t log_level = 0;
                                        spdk_json_decode_uint32(&values[i], &log_level);
					sdk_opt->log_level = log_level;
			        }
				else if (memcmp(values[i].start, "log_file", values[i].len) == 0) {
					i++;
					memcpy(sdk_opt->log_file, values[i].start, values[i].len);
	                        }
				else if (memcmp(values[i].start, "device_description", values[i].len) == 0) {
					i++;
					sdk_opt->nr_ssd = 0;
					if (values[i].type == SPDK_JSON_VAL_OBJECT_BEGIN) {
						parse_description(values, i, &sdk_opt->dd_options[0], (char*)&sdk_opt->dev_id[0]);
						sdk_opt->nr_ssd = 1;
						i += (values[i].len + 1);
					} else if (values[i].type == SPDK_JSON_VAL_ARRAY_BEGIN) {
						int num_values_in_array = values[i].len;
						for(int j = i; j < i + num_values_in_array + 1; j++){
							if (values[j].type == SPDK_JSON_VAL_ARRAY_BEGIN)
								continue;
							if (values[j].type == SPDK_JSON_VAL_ARRAY_END)
								break;
							parse_description(values, j, &sdk_opt->dd_options[sdk_opt->nr_ssd], (char*)&sdk_opt->dev_id[sdk_opt->nr_ssd]);
							sdk_opt->nr_ssd++;
							j += (values[j].len) + 1;
						}
						i += num_values_in_array + 1;	
					} else {
				                fprintf(stderr, "Device description is broken\n");
					        ret = KV_ERR_SDK_OPTION_LOAD;
						goto exit;
					}
				}
				break;
			}
			case SPDK_JSON_VAL_OBJECT_BEGIN:
			case SPDK_JSON_VAL_OBJECT_END:
			case SPDK_JSON_VAL_ARRAY_BEGIN:
			case SPDK_JSON_VAL_ARRAY_END:
			default:
				break;
		}
	}
exit:
        fclose(fp);

        if (json_line != NULL)
                free(json_line);

	return ret;
}


int check_device_description(kv_sdk *sdk_opt){
	int ret = KV_SUCCESS;
	int n = sdk_opt->nr_ssd;

	for(int i = 0 ; i < n ; i++){
		if (strlen(sdk_opt->dev_id[i])<=0){
			ret = KV_ERR_SDK_OPTION_LOAD;
			break;
		}
		if (sdk_opt->dd_options[i].core_mask == 0){
			ret = KV_ERR_SDK_OPTION_LOAD;
			break;
		}
		if (sdk_opt->dd_options[i].sync_mask < 0){
			ret = KV_ERR_SDK_OPTION_LOAD;
			break;
		}
		if (sdk_opt->dd_options[i].num_cq_threads <= 0){
			ret = KV_ERR_SDK_OPTION_LOAD;
			break;
		}
		if (sdk_opt->dd_options[i].cq_thread_mask < 0){
			ret = KV_ERR_SDK_OPTION_LOAD;
			break;
		}
		if (sdk_opt->dd_options[i].queue_depth <= 0){
			ret = KV_ERR_SDK_OPTION_LOAD;
			break;
		}
	}
	return ret;
}

int kv_sdk_load_option_from_str(kv_sdk *sdk_opt){
	/**
	 * NOTE: kv_sdk_load_default(&g_sdk) should be called before this function
	 */
	int ret = KV_SUCCESS;

	if (sdk_opt->cache_algorithm == CACHE_ALGORITHM_RADIX) {
		g_sdk.cache_algorithm = sdk_opt->cache_algorithm;
	}
	if (sdk_opt->cache_reclaim_policy == CACHE_RECLAIM_LRU) {
		g_sdk.cache_reclaim_policy = sdk_opt->cache_reclaim_policy;
	}
	if (sdk_opt->slab_size >0) {
		g_sdk.slab_size = sdk_opt->slab_size;
	}
	//support hugepages memory and kv(not lba) ssd only for now
	if ((sdk_opt->slab_alloc_policy == SLAB_MM_ALLOC_HUGE) || (sdk_opt->slab_alloc_policy == SLAB_MM_ALLOC_POSIX)) {
		g_sdk.slab_alloc_policy = SLAB_MM_ALLOC_HUGE;
	}
	if ((sdk_opt->ssd_type == KV_TYPE_SSD) || (sdk_opt->ssd_type == LBA_TYPE_SSD)) {
		g_sdk.ssd_type = sdk_opt->ssd_type;
	}
	if ((sdk_opt->log_level >= 0) && (sdk_opt->log_level <= 3)) {
		g_sdk.log_level = sdk_opt->log_level;
	}
	if (strlen(sdk_opt->log_file) > 0 ) {
		memcpy(g_sdk.log_file, sdk_opt->log_file, sizeof(g_sdk.log_file));
	}
	if (sdk_opt->app_hugemem_size) {
		g_sdk.app_hugemem_size = sdk_opt->app_hugemem_size;
	}

	if (sdk_opt->polling_interval) {
		g_sdk.polling_interval = sdk_opt->polling_interval;
	}

	g_sdk.nr_ssd = 0;
	for(int j=0; j<NR_MAX_SSD; j++){
		if (strlen(sdk_opt->dev_id[j]) <= 0) {
			break;
		}
		memcpy(g_sdk.dev_id[j], sdk_opt->dev_id[j], 32);
		if ((sdk_opt->dd_options[j].core_mask) && (sdk_opt->dd_options[j].cq_thread_mask)){
			g_sdk.dd_options[j].core_mask = sdk_opt->dd_options[j].core_mask;
			g_sdk.dd_options[j].sync_mask = sdk_opt->dd_options[j].sync_mask;
			g_sdk.dd_options[j].cq_thread_mask = sdk_opt->dd_options[j].cq_thread_mask;
			if (sdk_opt->dd_options[j].num_cq_threads) {
				g_sdk.dd_options[j].num_cq_threads = sdk_opt->dd_options[j].num_cq_threads;
			}
		}
		if (sdk_opt->dd_options[j].queue_depth) {
			g_sdk.dd_options[j].queue_depth = sdk_opt->dd_options[j].queue_depth;
		}
		g_sdk.nr_ssd++;
	}

	if (g_sdk.nr_ssd == 0) {
		fprintf(stderr, "At least one device ID should be specified\n");
		return KV_ERR_SDK_OPTION_LOAD;
	}

	if ((sdk_opt->use_cache == true) || (sdk_opt->use_cache == false)){
		g_sdk.use_cache = sdk_opt->use_cache;
	}

	memcpy(sdk_opt, &g_sdk, sizeof(g_sdk));
	return ret;
}

#define per_device_default (328196u)
#define queue_default (216u)
#define per_queue_depth (4432u)
/**
 * returns additional hugepage memory per device needed to initialize D/D
 */
static size_t kv_sdk_calc_additional_mem_needed(void){
	uint32_t queue_depth = g_sdk.dd_options[0].queue_depth;
	size_t driver_hugemem_size = 0;
	for(int i = 0; i < g_sdk.nr_ssd; i++) {
		uint32_t num_used_cores = 0;
		for(int j = 0; j < MAX_CPU_CORES; j++){
			if(g_sdk.dd_options[i].core_mask & (1ULL << j)){
                                        num_used_cores++;
			}
                }
		driver_hugemem_size += (size_t)(per_device_default + MAX(queue_default + (per_queue_depth * queue_depth) - 4*KB, 5*KB) * num_used_cores);
	}

	return driver_hugemem_size + g_sdk.app_hugemem_size;
}

int kv_sdk_init(int init_from, void *option){
	int ret = KV_SUCCESS;
	char *json_path;
	size_t add_hugemem_size;
	kv_sdk *sdk_opt = NULL;

	pthread_mutex_lock(&g_init_mutex);
	if(g_kvsdk_ref_count++ > 0){
		fprintf(stderr, "KV SDK is already initialized\n");
		pthread_mutex_unlock(&g_init_mutex);
		return KV_SUCCESS;
	}
	pthread_mutex_unlock(&g_init_mutex);

	kv_sdk_load_default(&g_sdk);
	kv_sdk_load_nxx_config(&g_sdk, init_from, option);

	switch(init_from){
		case KV_SDK_INIT_FROM_STR:
			log_debug(KV_LOG_INFO, "Initialize sdk from data structure...\n");
			sdk_opt = (kv_sdk *)option;
			if (kv_sdk_load_option_from_str(sdk_opt) != KV_SUCCESS){
				fprintf(stderr, "Can't load str kv sdk options, apply default\n");
				kv_sdk_load_default(&g_sdk);
			}
			break;
		case KV_SDK_INIT_FROM_JSON:
		default:
			log_debug(KV_LOG_INFO, "Initialize sdk from json file...\n");
			json_path = (char *)option;
			if (kv_sdk_load_option(&g_sdk, json_path) != KV_SUCCESS){
				fprintf(stderr, "Can't load json kv sdk options, apply default\n");
				kv_sdk_load_default(&g_sdk);
			}
			break;
	}

	dump_kv_sdk_options();

	ret = log_init(g_sdk.log_level,g_sdk.log_file);
	log_debug(KV_LOG_INFO, "[%s] log_init=%d log_level=%d\n",__FUNCTION__,ret, g_sdk.log_level);

	add_hugemem_size = kv_sdk_calc_additional_mem_needed(); //for user app and driver
	ret |= kvslab_init(g_sdk.slab_size, g_sdk.slab_alloc_policy, g_sdk.nr_ssd, add_hugemem_size);

	log_debug(KV_LOG_INFO, "[%s] slab_init=%d log_level=%d\n",__FUNCTION__,ret, g_sdk.slab_alloc_policy);

	if(g_sdk.use_cache){
		ret += kv_cache_init();
	}

	for(int i=0;i<g_sdk.nr_ssd;i++){
		ret = kv_nvme_init(g_sdk.dev_id[i], &g_sdk.dd_options[i], g_sdk.ssd_type);
		log_debug(KV_LOG_INFO, "[%s] ret=%d for %s\n",__FUNCTION__, ret, g_sdk.dev_id[i]);
		assert(ret == 0);
		//TODO: run setup.sh
		//if (ret == KV_ERR_DD_NO_DEVICE){
		//	fprintf(stderr, "Run setup scripts..\n");
		//	kv_sdk_run_setup_script();
		//	ret = kv_nvme_init(g_sdk.dev_id[i], &g_sdk.dd_options[i], g_sdk.ssd_type);
		//	assert(ret == 0);
		//}
		g_sdk.dev_handle[i] = kv_nvme_open(g_sdk.dev_id[i]);
		assert(g_sdk.dev_handle[i] != 0);
		log_debug(KV_LOG_INFO, "[%s] dev_handle[%d]=%ld\n",__FUNCTION__, i, g_sdk.dev_handle[i]);
		if(sdk_opt){
			sdk_opt->dev_handle[i] = g_sdk.dev_handle[i];
		}
	}

	return (ret == KV_SUCCESS) ? (ret) : (KV_ERR_SDK_OPEN);
}

int kv_sdk_finalize(){
	int ret = KV_SUCCESS;

	pthread_mutex_lock(&g_init_mutex);
	if(g_kvsdk_ref_count != 1){
		if(g_kvsdk_ref_count > 1){
			g_kvsdk_ref_count--;
		}
		fprintf(stderr, "[%s] kv_sdk reference count : %d\n",__FUNCTION__, g_kvsdk_ref_count);
		pthread_mutex_unlock(&g_init_mutex);
		return KV_SUCCESS;
	}
	g_kvsdk_ref_count--;
	pthread_mutex_unlock(&g_init_mutex);

	if(g_sdk.use_cache){
		ret = kv_cache_finalize();
	}

	ret = kvslab_destroy();

	log_debug(KV_LOG_INFO, "[%s] slab_destroy=%d\n",__FUNCTION__,ret);

	for(int i=0;i<g_sdk.nr_ssd;i++){
		ret = kv_nvme_close(g_sdk.dev_handle[i]);
		log_debug(KV_LOG_INFO, "kv_nvme_close() of %x ret = %d\n", g_sdk.dev_handle[i], ret);
		ret = kv_nvme_finalize(g_sdk.dev_id[i]);
		log_debug(KV_LOG_INFO, "kv_nvme_finalize() of %x ret = %d\n", g_sdk.dev_handle[i], ret);
	}

	log_deinit();
	
	return (ret == KV_SUCCESS) ? (ret) : (KV_ERR_SDK_CLOSE);
}

