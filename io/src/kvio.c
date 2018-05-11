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
#include <errno.h>

#include "kv_apis.h"
#include "kvcache.h"
#include "kvnvme.h"
#include "kvlog.h"
#include "kvconfig_nxx.h"

#undef USE_ITERATE_PREPATCH

extern  kv_sdk g_sdk;

typedef struct sdk_param{
        kv_pair* src;
        kv_pair* dst;
        void (*user_async_cb)();
        void* user_private_data;
}sdk_param;

typedef struct sdk_iterate_param{
	uint64_t handle;
        kv_iterate* src;
        kv_iterate* dst;
        void (*user_async_cb)();
        void* user_private_data;
}sdk_iterate_param;


//to check op parameters' validation
enum {
        op_store = 0,
        op_retrieve = 1,
        op_append = 2,
        op_delete = 3,
}_op_types;
char *_op_name[4]={"kv_store", "kv_retrieve", "kv_append", "kv_delete"};

typedef struct it_readahead{
	uint32_t iterator;
	uint8_t readbuf[KV_SSD_MAX_ITERATE_READ_LEN];
} it_readahead;

int _kv_check_op_param(uint64_t handle, kv_pair* dst, int op_types){
        uint32_t max_sdk_support_length;

        if(handle == 0 || !dst || !dst->key.key){
                fprintf(stderr, "[%s] Invalid Parameter \n", _op_name[op_types]);
                return KV_ERR_SDK_INVALID_PARAM;
        }

        if(op_types!=op_delete){
                if(!dst->value.value){
                        fprintf(stderr, "[%s] Invalid Parameter \n", _op_name[op_types]);
                        return KV_ERR_SDK_INVALID_PARAM;
                }
        } else {
		return KV_SUCCESS;
	}

        if (op_types == op_store && g_sdk.ssd_type == KV_TYPE_SSD){
                max_sdk_support_length = KV_MAX_STORE_VALUE_LEN;
        } else {
                max_sdk_support_length = KV_MAX_VALUE_LEN;
        }

        if((dst->value.length > max_sdk_support_length)||(dst->value.length < KV_MIN_VALUE_LEN)){
                fprintf(stderr, "[%s] value length should be from %u to %u (Value length: %u) \n", _op_name[op_types], KV_MIN_VALUE_LEN, max_sdk_support_length, dst->value.length);
                return KV_ERR_INVALID_VALUE_SIZE;
        }
        if(dst->value.length % KV_ALIGNMENT_UNIT){
                fprintf(stderr, "[%s] Value length should be multiple of %u (Value length: %u) \n", _op_name[op_types], KV_ALIGNMENT_UNIT, dst->value.length);
                return KV_ERR_MISALIGNED_VALUE_SIZE;
        }

        return KV_SUCCESS;
}

int kv_get_dev_idx_on_handle(uint64_t handle){
	for(int i=0;i<g_sdk.nr_ssd;i++){
		if(handle == g_sdk.dev_handle[i]){
			return i;
		}
	}
	return KV_ERR_SDK_INVALID_PARAM;
}

static void sdk_async_store_cb(kv_pair* kv, unsigned int result, unsigned int status){
        log_debug(KV_LOG_DEBUG, "[%s] result=%d status=%d key=%s\n", __FUNCTION__, result, status, kv->key.key);
        sdk_param* param = kv->param.private_data;
        kv_pair* io_kv = param->src;
        kv_pair* dst = param->dst;

        void (*async_cb)() = param->user_async_cb;
        dst->param.private_data = param->user_private_data;

        if(status == KV_SUCCESS){
                if(g_sdk.use_cache){
                        //NOTE: the same key in cache enteies will be overrided
                        int ret = kv_cache_write(io_kv);
                        log_debug(KV_LOG_DEBUG, "[kv_cache_write] ret=%d key=%s\n",ret, io_kv->key.key);
                }
        }

        free(param);
        param = NULL;
	slab_free_pair(io_kv);

        if(async_cb){
                async_cb(dst, result, status);
        }
}


bool context_switch_async_to_sync(int did){
	int core_id;
	cpu_set_t cpuset;

	// set sync core
	core_id = kv_get_sync_core(did);
	if (core_id < 0){
		return false;
	}
	CPU_ZERO(&cpuset);
	CPU_SET(core_id, &cpuset);
	sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);
	return true;
}

bool context_switch_sync_to_async(int did){
	int core_id;
	cpu_set_t cpuset;

	// set async core
	core_id = kv_get_async_core(did);
	if (core_id < 0){
		return false;
	}
	CPU_ZERO(&cpuset);
	CPU_SET(core_id, &cpuset);
	sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);
	return true;
}

static void copy_kv_pair(kv_pair* dst, kv_pair* src, int op_types){
        dst->key.length = src->key.length;
        memcpy(dst->key.key, src->key.key, src->key.length);

	switch(op_types){
		case op_store:
		case op_append:
			memcpy(dst->value.value, src->value.value, src->value.length);
		case op_retrieve:
			dst->value.length = src->value.length;
			dst->value.offset = src->value.offset;
			break;
		case op_delete:
			dst->value.offset = 0;
		default:
			break;
	}

        memcpy((char*)&dst->param, (char*)&src->param, sizeof(src->param));
}

int _kv_store(uint64_t handle, kv_pair* dst){
        int did, ret = KV_SUCCESS;
        if((ret = _kv_check_op_param(handle, dst, op_store)) != KV_SUCCESS){
                goto err;
        }

        if((did = kv_get_dev_idx_on_handle(handle)) == KV_ERR_SDK_INVALID_PARAM){
                ret = KV_ERR_SDK_INVALID_PARAM;
                goto err;
        }

        kv_pair* io_kv = slab_alloc_pair(dst->key.length, dst->value.length, did);

        if(!io_kv){
                fprintf(stderr, "slab_alloc_pair error on kv_store\n");
                ret = KV_ERR_SLAB_ALLOC_FAILURE;
                goto err;
        }

	copy_kv_pair(io_kv, dst, op_store);

	ret = kv_nvme_write(handle, io_kv);
	if(ret == KV_ERR_DD_INVALID_QUEUE_TYPE) {
		if(context_switch_async_to_sync(did)){
			ret = kv_nvme_write(handle, io_kv);
		}
	}

	log_debug(KV_LOG_DEBUG, "[kv_nvme_write] ret=%d key=%s\n",ret, dst->key.key);

	if(ret != KV_SUCCESS){
		slab_free_pair(io_kv);
		goto err;
	}

	if(g_sdk.use_cache){
		//NOTE: the same key in cache enteies will be overrided
		ret += kv_cache_write(io_kv);
		log_debug(KV_LOG_DEBUG, "[kv_cache_write] ret=%d key=%s\n",ret, dst->key.key);
	}

	slab_free_pair(io_kv);

err:
	return ret;
}

int _kv_store_async(uint64_t handle, kv_pair* dst){
        int did, ret = KV_SUCCESS;
        if((ret = _kv_check_op_param(handle, dst, op_store)) != KV_SUCCESS){
                goto err;
        }

        if((did = kv_get_dev_idx_on_handle(handle)) == KV_ERR_SDK_INVALID_PARAM){
                ret = KV_ERR_SDK_INVALID_PARAM;
                goto err;
        }

        kv_pair* io_kv = slab_alloc_pair(dst->key.length, dst->value.length, did);

        if(!io_kv){
                fprintf(stderr, "slab_alloc_pair error on kv_store\n");
                ret = KV_ERR_SLAB_ALLOC_FAILURE;
                goto err;
        }

	copy_kv_pair(io_kv, dst, op_store);

	sdk_param* param = malloc(sizeof(sdk_param));
	if(!param){
		ret = KV_ERR_HEAP_ALLOC_FAILURE;
		fprintf(stderr, "[kv_nvme_write_async]sdk_param malloc err\n");
		slab_free_pair(io_kv);
		goto err;
	}
	param->src = io_kv;
	param->dst = dst;
	param->user_async_cb = dst->param.async_cb;
	param->user_private_data = dst->param.private_data;

	io_kv->param.async_cb = sdk_async_store_cb;
	io_kv->param.private_data = param;

	ret = KV_ERR_IO;
	while(ret) {
		ret = kv_nvme_write_async(handle, io_kv);
		if(ret == KV_ERR_DD_INVALID_QUEUE_TYPE) {
			if(context_switch_sync_to_async(did)){
				ret = kv_nvme_write_async(handle, io_kv);
			}
			else{
				slab_free_pair(io_kv);
				goto err;
			}
		}

		log_debug(KV_LOG_DEBUG, "[kv_nvme_write_async] ret=%d key=%s\n", ret, io_kv->key.key);
		if(ret){
			if(g_sdk.ssd_type != LBA_TYPE_SSD) {
				usleep(g_sdk.polling_interval);
			}
		}
		else{
			break;
		}
	}

err:
	return ret;
}

static void sdk_async_retrieve_cb(kv_pair* kv, unsigned int result, unsigned int status){
        log_debug(KV_LOG_DEBUG, "[%s] result=%d status=%d\n", __FUNCTION__, result, status);
        sdk_param* param = kv->param.private_data;
        kv_pair* io_kv = param->src;
        kv_pair* dst = param->dst;
        void (*async_cb)() = param->user_async_cb;
        dst->param.private_data = param->user_private_data;

        if(status == KV_SUCCESS){
                dst->value.length = result;
                memcpy(dst->value.value, io_kv->value.value, dst->value.length);
                dst->value.offset = io_kv->value.offset;

                if(g_sdk.use_cache){
                        int ret = kv_cache_write(io_kv);
                        log_debug(KV_LOG_DEBUG, "[kv_cache_write] ret=%d io_kv_key=|%s| io_key_value=|%s|\n",ret, io_kv->key.key, io_kv->value.value);
                }
                log_debug(KV_LOG_DEBUG, "[%s]dst->key=%s dst->value=%s\n",__FUNCTION__,dst->key.key, (char*)dst->value.value);
        }

        free(param);
        param = NULL;
	slab_free_pair(io_kv);

        if(async_cb){
                async_cb(dst, result, status);
        }
}


int _kv_retrieve(uint64_t handle, kv_pair* dst){
        int did, ret = KV_SUCCESS;
        if((ret = _kv_check_op_param(handle, dst, op_retrieve)) != KV_SUCCESS){
                goto err;
        }

        if(g_sdk.use_cache){
                ret = kv_cache_read(dst);
                log_debug(KV_LOG_DEBUG, "[kv_cache_read] ret=%d key=%s value=%s\n",ret, dst->key.key, dst->value.value);
                if(ret == KV_CACHE_SUCCESS){
                        return KV_SUCCESS;
                }
        }

        if((did = kv_get_dev_idx_on_handle(handle)) == KV_ERR_SDK_INVALID_PARAM){
                ret = KV_ERR_SDK_INVALID_PARAM;
                goto err;
        }

        kv_pair* io_kv = slab_alloc_pair(dst->key.length, dst->value.length, did);

        if(!io_kv){
                ret = KV_ERR_SLAB_ALLOC_FAILURE;
                fprintf(stderr, "kv_pair slab alloc fail\n");
                goto err;
        }

        copy_kv_pair(io_kv, dst, op_retrieve);

	ret = kv_nvme_read(handle, io_kv);
	if(ret == KV_ERR_DD_INVALID_QUEUE_TYPE) {
		if(context_switch_async_to_sync(did)){
			ret = kv_nvme_read(handle, io_kv);
		}
	}
	log_debug(KV_LOG_DEBUG, "[kv_nvme_read] ret=%d key=%s value=%s\n",ret, io_kv->key.key, io_kv->value.value);
	if(ret != KV_SUCCESS){
		slab_free_pair(io_kv);
		goto err;
	}

	dst->value.length = io_kv->value.length;
	memcpy(dst->value.value, io_kv->value.value, dst->value.length);
	dst->value.offset = io_kv->value.offset;

	if(g_sdk.use_cache){
		ret = kv_cache_write(io_kv);
		log_debug(KV_LOG_DEBUG, "[kv_cache_write] ret=%d key=%s value=%s\n",ret, io_kv->key.key, io_kv->value.value);
	}
	slab_free_pair(io_kv);

err:
	return ret;
}

int _kv_retrieve_async(uint64_t handle, kv_pair* dst){
        int did, ret = KV_SUCCESS;
        if((ret = _kv_check_op_param(handle, dst, op_retrieve)) != KV_SUCCESS){
                goto err;
        }

        if(g_sdk.use_cache){
                ret = kv_cache_read(dst);
                log_debug(KV_LOG_DEBUG, "[kv_cache_read] ret=%d key=%s value=%s\n",ret, dst->key.key, dst->value.value);
                if(ret == KV_CACHE_SUCCESS){
			if(dst->param.async_cb){
				dst->param.async_cb(dst, dst->value.length, ret);
			}
                        return KV_SUCCESS;
                }
        }

        if((did = kv_get_dev_idx_on_handle(handle)) == KV_ERR_SDK_INVALID_PARAM){
                ret = KV_ERR_SDK_INVALID_PARAM;
                goto err;
        }

        kv_pair* io_kv = slab_alloc_pair(dst->key.length, dst->value.length, did);

        if(!io_kv){
                ret = KV_ERR_SLAB_ALLOC_FAILURE;
                fprintf(stderr, "kv_pair slab alloc fail\n");
                goto err;
        }

        copy_kv_pair(io_kv, dst, op_retrieve);

	sdk_param* param = malloc(sizeof(sdk_param));
	if(!param){
		ret = KV_ERR_HEAP_ALLOC_FAILURE;
		fprintf(stderr, "[kv_nvme_read_async]sdk_param malloc err\n");
		slab_free_pair(io_kv);
		goto err;
	}
	param->src = io_kv;
	param->dst = dst;
	param->user_async_cb = dst->param.async_cb;
	param->user_private_data = dst->param.private_data;

	io_kv->param.async_cb = sdk_async_retrieve_cb;
	io_kv->param.private_data = param;

	ret = KV_ERR_IO;
	while(ret){
		ret = kv_nvme_read_async(handle, io_kv);
		if(ret == KV_ERR_DD_INVALID_QUEUE_TYPE) {
			if(context_switch_sync_to_async(did)){
				ret = kv_nvme_read_async(handle, io_kv);
			}
			else{
				slab_free_pair(io_kv);
				goto err;
			}
		}

		log_debug(KV_LOG_DEBUG, "[kv_nvme_read_async] ret=%d key=%s\n", ret, io_kv->key.key);
		if(ret){
			if(g_sdk.ssd_type != LBA_TYPE_SSD) {
				usleep(g_sdk.polling_interval);
			}
		}
		else{
			break;
		}
	}

err:
	return ret;
}

static void sdk_async_delete_cb(kv_pair* kv, unsigned int result, unsigned int status){
        log_debug(KV_LOG_DEBUG, "[%s] result=%d status=%d key=%s\n", __FUNCTION__, result, status, kv->key.key);
        sdk_param* param = kv->param.private_data;
	kv_pair* io_kv = param->src;
        kv_pair* dst = param->dst;

        void (*async_cb)() = param->user_async_cb;
        dst->param.private_data = param->user_private_data;

        if(status == KV_SUCCESS){
                // do something
        }

        free(param);
        param = NULL;
	slab_free_pair(io_kv);

        if(async_cb){
                async_cb(dst, result, status);
        }
}

int _kv_delete(uint64_t handle, kv_pair* dst){
        int did, ret = KV_SUCCESS;

        if((ret = _kv_check_op_param(handle, dst, op_delete)) != KV_SUCCESS){
                goto err;
        }

        if(g_sdk.use_cache){
                ret = kv_cache_delete(dst);
        }

        if((did = kv_get_dev_idx_on_handle(handle)) == KV_ERR_SDK_INVALID_PARAM){
                ret = KV_ERR_SDK_INVALID_PARAM;
                goto err;
        }

	ret = kv_nvme_delete(handle, dst);
	if(ret == KV_ERR_DD_INVALID_QUEUE_TYPE) {
		if(context_switch_async_to_sync(did)){
			ret = kv_nvme_delete(handle, dst);
		}
	}

err:
	return ret;
}

int _kv_delete_async(uint64_t handle, kv_pair* dst){
        int did, ret = KV_SUCCESS;

        if((ret = _kv_check_op_param(handle, dst, op_delete)) != KV_SUCCESS){
                goto err;
        }

        if(g_sdk.use_cache){
                ret = kv_cache_delete(dst);
        }

        if((did = kv_get_dev_idx_on_handle(handle)) == KV_ERR_SDK_INVALID_PARAM){
                ret = KV_ERR_SDK_INVALID_PARAM;
                goto err;
        }

	kv_pair* io_kv = slab_alloc_pair(dst->key.length, 0, did); //value.length = 0

	if(!io_kv){
		ret = KV_ERR_SLAB_ALLOC_FAILURE;
		fprintf(stderr, "kv_pair slab alloc fail\n");
		goto err;
	}

	copy_kv_pair(io_kv, dst, op_delete);

	sdk_param* param = malloc(sizeof(sdk_param));
	if(!param){
		ret = KV_ERR_HEAP_ALLOC_FAILURE;
		fprintf(stderr, "[kv_nvme_delete_async]sdk_param malloc err\n");
		slab_free_pair(io_kv);
		goto err;
	}
	param->src = io_kv;
	param->dst = dst;
	param->user_async_cb = dst->param.async_cb;
	param->user_private_data = dst->param.private_data;

	io_kv->param.async_cb = sdk_async_delete_cb;
	io_kv->param.private_data = param;

	ret = KV_ERR_IO;
	while(ret){
		ret = kv_nvme_delete_async(handle, io_kv);
		if(ret == KV_ERR_DD_INVALID_QUEUE_TYPE) {
			if(context_switch_sync_to_async(did)){
				ret = kv_nvme_delete_async(handle, io_kv);
			}
			else{
				slab_free_pair(io_kv);
				goto err;
			}
		}
		log_debug(KV_LOG_DEBUG, "[kv_nvme_delete_async] ret=%d key=%s\n", ret, dst->key.key);
		if(ret){
			usleep(g_sdk.polling_interval);
		}
		else{
			break;
		}
	}

err:
	return ret;
}

int _kv_check_iterate_param(uint64_t handle, kv_iterate* it){
        uint32_t max_sdk_support_length;

        if(handle == 0 || !it || !it->kv.value.value){
                fprintf(stderr, "[%s] Invalid Parameter \n", __FUNCTION__);
                return KV_ERR_SDK_INVALID_PARAM;
        }

        if((it->kv.value.length > KV_SDK_MAX_ITERATE_READ_LEN)||(it->kv.value.length < KV_SDK_MIN_ITERATE_READ_LEN)){
                fprintf(stderr, "[%s] iterate_read value length should be from %u to %u (Value length: %u) \n", __FUNCTION__, KV_SDK_MIN_ITERATE_READ_LEN, KV_SDK_MAX_ITERATE_READ_LEN, it->kv.value.length);
                return KV_ERR_INVALID_VALUE_SIZE;
        }

        if(it->kv.value.length % KV_ALIGNMENT_UNIT){
                fprintf(stderr, "[%s] Value length should be multiple of %u (Value length: %u) \n", __FUNCTION__, KV_ALIGNMENT_UNIT, it->kv.value.length);
                return KV_ERR_MISALIGNED_VALUE_SIZE;
        }

	return KV_SUCCESS;
}

static void sdk_async_iterate_read_cb(kv_iterate* it, unsigned int result, unsigned int status){
        log_debug(KV_LOG_DEBUG, "[%s] result=%d status=%d\n", __FUNCTION__, result, status);
        sdk_iterate_param* param = it->kv.param.private_data;
        kv_iterate* io_it = param->src;
        kv_iterate* dst = param->dst;
        void (*async_cb)() = param->user_async_cb;
        dst->kv.param.private_data = param->user_private_data;
	uint64_t handle = param->handle;

#ifdef  USE_ITERATE_PREPATCH
        if(status == KV_ERR_ITERATE_READ_EOF){
		//Note: both of io_it->value.length and result variable contain the actual read size.
		int copy_size = (io_it->kv.value.length > dst->kv.value.length) ? dst->kv.value.length : io_it->kv.value.length;
                memcpy(dst->kv.value.value, io_it->kv.value.value, copy_size);
		result = copy_size;
                log_debug(KV_LOG_DEBUG, "[%s]it->iterator=%d dst->value=%llx result=%d status=%d \n",__FUNCTION__,dst->iterator, dst->kv.value.value, result, status);
	}
	else if(status == KV_SUCCESS){ 
		int copy_size = (io_it->kv.value.length > dst->kv.value.length) ? dst->kv.value.length : io_it->kv.value.length;
                memcpy(dst->kv.value.value, io_it->kv.value.value, copy_size);

		int actual_read = copy_size;
		int remain_read_size = dst->kv.value.length - actual_read;
                log_debug(KV_LOG_DEBUG, "[%s]it->iterator=%d dst->value=%llx actual_read=%d status=%d \n",__FUNCTION__,dst->iterator, dst->kv.value.value, actual_read, status);
		if(remain_read_size <= 0){
			result = actual_read;
			goto finalize;
		}

		int did = kv_get_dev_idx_on_handle(handle);
		if(did == KV_ERR_SDK_INVALID_PARAM){
			status = KV_ERR_IO;
			result = 0;
			goto finalize;
		}

		//can issue further iterate_read request for filling buffer
		while(remain_read_size > 0){
			int ssd_it_read_size = KV_SSD_MIN_ITERATE_READ_LEN;
			io_it->kv.value.length = ssd_it_read_size;
			//printf("submit iterate_read: io_it->value.length = %d\n",io_it->value.length);
			int ret = kv_nvme_iterate_read(handle, io_it);
			if(ret == KV_ERR_DD_INVALID_QUEUE_TYPE) {
				if(context_switch_async_to_sync(did)){
					ret = kv_nvme_iterate_read(handle, io_it);
				}
			}

			if(ret != KV_SUCCESS && ret != KV_ERR_ITERATE_READ_EOF){
				status = KV_ERR_IO;
				actual_read = 0;
				break;
			}

			copy_size = (remain_read_size > io_it->kv.value.length) ? io_it->kv.value.length : remain_read_size;
			memcpy(dst->kv.value.value + actual_read, io_it->kv.value.value, copy_size);
			actual_read += copy_size;
			remain_read_size -= copy_size;

			log_debug(KV_LOG_DEBUG, "[%s] ret=%d iterator=%d remain_read_size=%d io_it->value.length=%d actual_read=%d dst->value.value=%llx\n", __FUNCTION__, ret, io_it->iterator, remain_read_size, io_it->kv.value.length, actual_read, dst->kv.value.value+actual_read-copy_size);

			if(ret == KV_ERR_ITERATE_READ_EOF){
				status = KV_ERR_ITERATE_READ_EOF;
				break;
			}
		}
		result = actual_read;
        }
	else{
		result = 0;
	}

#else
	dst->kv.key.length = io_it->kv.key.length;
	if(dst->kv.key.length > 0 && dst->kv.key.key != NULL){
		memcpy(dst->kv.key.key, io_it->kv.value.value, dst->kv.key.length);
	}
	
	dst->kv.value.length = io_it->kv.value.length;
	dst->kv.value.offset = io_it->kv.value.offset;
	if(dst->kv.value.length > 0 && dst->kv.value.value != NULL){
		if(dst->kv.key.length > 0){
			int kv_buffer_offset = 512;
			memcpy(dst->kv.value.value, io_it->kv.value.value+kv_buffer_offset, dst->kv.value.length);
		}
		else{
			memcpy(dst->kv.value.value, io_it->kv.value.value, dst->kv.value.length);
		}
	}

	log_debug(KV_LOG_DEBUG, "[%s] status=%d iterator=%d dst->kv.key.length=%d dst->kv.value.length=%d dst->value.value=%llx\n", __FUNCTION__, status, dst->iterator, dst->kv.key.length, dst->kv.value.length, dst->kv.value.value);
#endif
	
finalize:
	dst->kv.value.length = result;
        dst->kv.value.offset = io_it->kv.value.offset;

        free(param);
        param = NULL;
	slab_free_iterate(io_it);

        if(async_cb){
                async_cb(dst, result, status);
        }
}

int _kv_iterate_read_async(uint64_t handle, kv_iterate* dst){
	int ret = KV_SUCCESS;
	if((ret = _kv_check_iterate_param(handle, dst)) != KV_SUCCESS){
		dst->kv.value.length = 0;
		goto err;
	}

	int did = kv_get_dev_idx_on_handle(handle);
	if(did == KV_ERR_SDK_INVALID_PARAM){
		ret = KV_ERR_SDK_INVALID_PARAM;
		goto err;
	}

	int ssd_it_read_size = KV_SSD_MIN_ITERATE_READ_LEN;

        kv_iterate* io_it = slab_alloc_iterate(KV_MAX_KEY_LEN+1 ,ssd_it_read_size, did);
        if(!io_it){
                fprintf(stderr, "slab_alloc_iterator error on %s\n", __FUNCTION__);
                ret = KV_ERR_SLAB_ALLOC_FAILURE;
                goto err;
	}

        io_it->iterator = dst->iterator;
	io_it->kv.key.length = 0;
	io_it->kv.value.length = ssd_it_read_size;
        io_it->kv.value.offset = 0;
        memcpy((char*)&io_it->kv.param,(char*)&dst->kv.param,sizeof(kv_param));

	sdk_iterate_param* param = malloc(sizeof(sdk_iterate_param));
	if(!param){
		ret = KV_ERR_HEAP_ALLOC_FAILURE;
		fprintf(stderr, "[%s] sdk_iterate_param alloc err\n", __FUNCTION__);
		slab_free_iterate(io_it);
		goto err;
	}

	param->src = io_it;
	param->dst = dst;
	param->user_async_cb = dst->kv.param.async_cb;
	param->user_private_data = dst->kv.param.private_data;
	param->handle = handle;
	
	io_it->kv.param.async_cb = sdk_async_iterate_read_cb;
	io_it->kv.param.private_data = param;

	ret = KV_ERR_IO;
	while(ret) {
		ret = kv_nvme_iterate_read_async(handle, io_it);
		if(ret == KV_ERR_DD_INVALID_QUEUE_TYPE) {
			if(context_switch_sync_to_async(did)){
				ret = kv_nvme_iterate_read_async(handle, io_it);
			}
			else{
				slab_free_iterate(io_it);
				goto err;
			}
		}

		log_debug(KV_LOG_DEBUG, "[%s] submit done. ret=%d iterator id=%d dst->value.length=%d\n", __FUNCTION__, ret, io_it->iterator, dst->kv.value.length);
		if(ret){
			usleep(g_sdk.polling_interval);
		}
		else{
			break;
		}
	}
	
err:
        return ret;

}


int _kv_iterate_read(uint64_t handle, kv_iterate* dst){
	int ret = KV_SUCCESS;
	if((ret = _kv_check_iterate_param(handle, dst)) != KV_SUCCESS){
		dst->kv.value.length = 0;
		goto err;
	}

	int did = kv_get_dev_idx_on_handle(handle);
	if(did == KV_ERR_SDK_INVALID_PARAM){
		ret = KV_ERR_SDK_INVALID_PARAM;
		goto err;
	}

	//int slab_it_read_size = (dst->value.length < KV_SSD_MIN_ITERATE_READ_LEN) ? KV_SSD_MIN_ITERATE_READ_LEN : dst->value.length;
	int remain_read_size = dst->kv.value.length;
	int ssd_it_read_size = KV_SSD_MIN_ITERATE_READ_LEN;

	/*FIXME: key_length */
        kv_iterate* io_it = slab_alloc_iterate(KV_MAX_KEY_LEN+1, ssd_it_read_size, did);
        if(!io_it){
                fprintf(stderr, "slab_alloc_iterator error on %s\n", __FUNCTION__);
                ret = KV_ERR_SLAB_ALLOC_FAILURE;
                goto err;
        }

        io_it->iterator = dst->iterator;
        io_it->kv.value.offset = 0;
        memcpy((char*)&io_it->kv.param,(char*)&dst->kv.param,sizeof(kv_param));

#ifdef USE_ITERATE_PREPATCH
	//async read
	int actual_read = 0;
	do {
		io_it->value.length = ssd_it_read_size;
		//printf("submit iterate_read: io_it->value.length = %d\n",io_it->value.length);
		ret = kv_nvme_iterate_read(handle, io_it);
		if(ret == KV_ERR_DD_INVALID_QUEUE_TYPE) {
			if(context_switch_async_to_sync(did)){
				ret = kv_nvme_iterate_read(handle, io_it);
			}
		}

		if(ret != KV_SUCCESS && ret != KV_ERR_ITERATE_READ_EOF){
			ret = KV_ERR_IO;
			slab_free_iterate(io_it);
			goto err;
		}

		int copy_size = (remain_read_size > io_it->kv.value.length) ? io_it->kv.value.length : remain_read_size;
		memcpy(dst->kv.value.value + actual_read, io_it->kv.value.value, copy_size);
		actual_read += copy_size;
		remain_read_size -= copy_size;

		log_debug(KV_LOG_DEBUG, "[kv_nvme_iterate_read] ret=%d iterator=%d remain_read_size=%d io_it->value.length=%d actual_read=%d dst->value.value=%llx\n", ret, io_it->iterator, remain_read_size, io_it->kv.value.length, actual_read, dst->kv.value.value+actual_read-copy_size);

	}while( remain_read_size > 0 && ret != KV_ERR_ITERATE_READ_EOF);

	dst->kv.value.offset = io_it->kv.value.offset;
	dst->kv.value.length = actual_read;
#else
	io_it->kv.key.length = 0;
	io_it->kv.value.length = ssd_it_read_size;
	ret = kv_nvme_iterate_read(handle, io_it);
	if(ret == KV_ERR_DD_INVALID_QUEUE_TYPE) {
		if(context_switch_async_to_sync(did)){
			ret = kv_nvme_iterate_read(handle, io_it);
		}
	}

	if(ret != KV_SUCCESS && ret != KV_ERR_ITERATE_READ_EOF){
		dst->kv.key.length = 0;
		dst->kv.value.length = 0;
		ret = KV_ERR_IO;
		slab_free_iterate(io_it);
		goto err;
	}

	dst->kv.key.length = io_it->kv.key.length;
	if(dst->kv.key.length > 0 && dst->kv.key.key != NULL){
		memcpy(dst->kv.key.key, io_it->kv.value.value, dst->kv.key.length);
	}
	
	dst->kv.value.length = io_it->kv.value.length;
	dst->kv.value.offset = io_it->kv.value.offset;
	if(dst->kv.value.length > 0 && dst->kv.value.value != NULL){
		if(dst->kv.key.length > 0){
			int kv_buffer_offset = 512;
			memcpy(dst->kv.value.value, io_it->kv.value.value+kv_buffer_offset, dst->kv.value.length);
		}
		else{
			memcpy(dst->kv.value.value, io_it->kv.value.value, dst->kv.value.length);
		}
	}
	log_debug(KV_LOG_DEBUG, "[kv_nvme_iterate_read] ret=%d iterator=%d dst->value.length=%d dst->value.value=%llx\n", ret, io_it->iterator, dst->kv.value.length, dst->kv.value.value);
#endif
	slab_free_iterate(io_it);

err:
        return ret;
}

uint32_t _kv_iterate_open(uint64_t handle, const uint32_t bitmask, const uint32_t prefix, const uint8_t iterate_type){
	uint32_t iterator = KV_INVALID_ITERATE_HANDLE;
	while(1) {
		iterator = kv_nvme_iterate_open(handle, bitmask, prefix, iterate_type);
		if (iterator == KV_ERR_DD_NO_AVAILABLE_RESOURCE) {
			usleep(g_sdk.polling_interval);
		}
		else {
			break;
		}
	}
	return iterator;
}

int _kv_iterate_close(uint64_t handle, const uint8_t iterator){
	int ret = KV_ERR_DD_INVALID_PARAM;
	while(1) {
                ret = kv_nvme_iterate_close(handle, iterator);
                if (ret == KV_ERR_DD_NO_AVAILABLE_RESOURCE) {
                        usleep(g_sdk.polling_interval);
                }
                else {
                        break;
                }
        }
	return ret;
}

int _kv_append(uint64_t handle, kv_pair *kv){
        int ret = KV_SUCCESS;
        if((ret = _kv_check_op_param(handle, kv, op_append)) != KV_SUCCESS){
                goto err;
	}

	int did = kv_get_dev_idx_on_handle(handle);
	if(did == KV_ERR_SDK_INVALID_PARAM){
		ret = KV_ERR_SDK_INVALID_PARAM;
		goto err;
        }

        kv_pair* io_kv = slab_alloc_pair(kv->key.length, kv->value.length, did);

        if(!io_kv){
                fprintf(stderr, "slab_alloc_pair error on kv_append\n");
                ret = KV_ERR_SLAB_ALLOC_FAILURE;
                goto err;
        }

	copy_kv_pair(io_kv, kv, op_append);

	ret = kv_nvme_append(handle, io_kv);
	log_debug(KV_LOG_DEBUG, "[kv_nvme_append] ret=%d key=%s\n",ret, io_kv->key.key);

	slab_free_pair(io_kv);

err:
        return ret;
}
