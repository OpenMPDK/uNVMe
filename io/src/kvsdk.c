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

extern  kv_sdk g_sdk;
extern  int g_kvsdk_ref_count;

extern int _kv_store(uint64_t handle, kv_pair* kv);
extern int _kv_store_async(uint64_t handle, kv_pair* kv);
extern int _kv_retrieve(uint64_t handle, kv_pair* kv);
extern int _kv_retrieve_async(uint64_t handle, kv_pair* kv);
extern int _kv_delete(uint64_t handle, kv_pair* kv);
extern int _kv_delete_async(uint64_t handle, kv_pair* kv);
extern int _kv_exist(uint64_t handle, kv_pair* kv);
extern int _kv_exist_async(uint64_t handle, kv_pair* kv);
extern int _kv_append(uint64_t handle, kv_pair* kv);

extern uint32_t _kv_iterate_open(uint64_t handle, const uint8_t keyspace_id, const uint32_t bitmask, const uint32_t prefix, const uint8_t iterate_type);
extern int _kv_iterate_close(uint64_t handle, const uint8_t iterator);
extern int _kv_iterate_read(uint64_t handle, kv_iterate* it);
extern int _kv_iterate_read_async(uint64_t handle, kv_iterate* it);

int kv_store(uint64_t handle, kv_pair* kv){
	return _kv_store(handle, kv);
}

int kv_store_async(uint64_t handle, kv_pair* kv){
	return _kv_store_async(handle, kv);
}

int kv_retrieve(uint64_t handle, kv_pair* kv){
	return _kv_retrieve(handle, kv);
}

int kv_retrieve_async(uint64_t handle, kv_pair* kv){
	return _kv_retrieve_async(handle, kv);
}

int kv_delete(uint64_t handle, kv_pair* kv){
	return _kv_delete(handle, kv);
}

int kv_delete_async(uint64_t handle, kv_pair* kv){
	return _kv_delete_async(handle, kv);
}

int kv_exist(uint64_t handle, kv_pair* kv){
	return  _kv_exist(handle, kv);
}

int kv_exist_async(uint64_t handle, kv_pair* kv){
	return  _kv_exist_async(handle, kv);
}

int kv_append(uint64_t handle, kv_pair *kv){
	return _kv_append(handle, kv);
}

uint32_t kv_iterate_open(uint64_t handle, const uint8_t keyspace_id, const uint32_t bitmask, const uint32_t prefix, const uint8_t iterate_type){
	return _kv_iterate_open(handle, keyspace_id, bitmask, prefix, iterate_type);
}

int kv_iterate_close(uint64_t handle, const uint8_t iterator){
	return _kv_iterate_close(handle, iterator);
}

int kv_iterate_read(uint64_t handle, kv_iterate* it){
	return _kv_iterate_read(handle, it);
}
int kv_iterate_read_async(uint64_t handle, kv_iterate* it){
	return _kv_iterate_read_async(handle, it);
}

int kv_iterate_info(uint64_t handle, kv_iterate_handle_info* info, int nr_handle){
	return kv_nvme_iterate_info(handle, info, nr_handle);
}

int kv_get_active_iterator(uint64_t handle, uint32_t* nr_open_handle,  uint8_t* arr_open_handle){
	return 0;
}

int kv_format_device(uint64_t handle, int erase_user_data){
	if(handle == 0){
		fprintf(stderr, "[%s] Invalid Parameter\n", __FUNCTION__);
		return KV_ERR_SDK_INVALID_PARAM;
	}

	int ret = KV_SUCCESS;
	ret = kv_nvme_format(handle, erase_user_data);
	if (ret!=KV_SUCCESS){
		fprintf(stderr, "[%s] ret = %d\n", __FUNCTION__, ret);
		return KV_ERR_IO; //need to update
	}

	return ret;
}

/*
kv_get_core_id : returns the number of the CPU on which the calling
thread is currently executing.  -1 is return on error
 */
inline int kv_get_core_id(){
	return sched_getcpu();
}

int kv_get_devices_on_cpu(int core_id, int* nr_device, uint64_t* arr_handle){
	int ret = KV_ERR_IO;
	int i;
	int nr_dev = 0;
	for(i=0;i<g_sdk.nr_ssd;i++){
		if(g_sdk.dd_options[i].core_mask & (1ULL << core_id)){
			arr_handle[nr_dev] = g_sdk.dev_handle[i];
			nr_dev++;
			ret = KV_SUCCESS;
		}
	}
	*nr_device = nr_dev;
	return ret;
}

int kv_get_device_handles(int* nr_device, uint64_t* arr_handle){
	return kv_get_devices_on_cpu(0,nr_device,arr_handle);
}

int kv_get_cpus_on_device(uint64_t handle, int* nr_core, int* arr_core){
	int ret = KV_ERR_IO;
	int i,j;
	int nr_cpu = 0;
	char* bdf = NULL;
	for(i=0;i<g_sdk.nr_ssd;i++){
		if(handle == g_sdk.dev_handle[i]){
			for(j=0;j<MAX_CPU_CORES; j++){
				if(g_sdk.dd_options[i].core_mask & (1ULL << j)){
					arr_core[nr_cpu] = j;
					nr_cpu++;
				}
			}
			*nr_core = nr_cpu;
			ret = KV_SUCCESS;
			break;
		}
	}
	return ret;
}

uint64_t kv_get_total_size(uint64_t handle){
	if (handle == 0) {
		return KV_ERR_SDK_INVALID_PARAM;
	}
	return kv_nvme_get_total_size(handle);
}

uint64_t kv_get_used_size(uint64_t handle){
	if (handle == 0) {
		return KV_ERR_SDK_INVALID_PARAM;
	}
	return kv_nvme_get_used_size(handle);
}

uint64_t kv_get_waf(uint64_t handle){
	if (handle == 0) {
		return KV_ERR_SDK_INVALID_PARAM;
	}
	return kv_nvme_get_waf(handle);
}

uint32_t kv_get_sector_size(uint64_t handle){
	if (handle == 0) {
		return KV_ERR_SDK_INVALID_PARAM;
	}
	return kv_nvme_get_sector_size(handle);
}

uint64_t kv_get_num_sectors(uint64_t handle){
	if (handle == 0) {
		return KV_ERR_SDK_INVALID_PARAM;
	}
	return kv_nvme_get_num_sectors(handle);
}

int kv_get_log_page(uint64_t handle, uint8_t log_id, void* buffer, uint32_t buffer_size) {
	if (handle == 0) {
		return KV_ERR_SDK_INVALID_PARAM;
	}
	return kv_nvme_get_log_page(handle, log_id, buffer, buffer_size);
}

int kv_io_queue_type(uint64_t handle, int core_id){
	if(handle == 0){
		return KV_ERR_SDK_INVALID_PARAM;
	}
	int queue_io_type = kv_nvme_io_queue_type(handle, core_id);
	if (queue_io_type < 0){
		fprintf(stderr, "Invalid I/O Queue type for the CPU Core ID\n");
		return KV_ERR_IO;
	}
	return queue_io_type;
}

int kv_is_sdk_initialized(){
	return (g_kvsdk_ref_count > 0) ? 1 : 0;
}

void kv_sdk_info(){
	kv_nvme_sdk_info();
}

void kv_process_completion(uint64_t handle){
	kv_nvme_process_completion(handle);
}

void kv_process_completion_queue(uint64_t handle, uint32_t queue_id){
	kv_nvme_process_completion_queue(handle, queue_id);
}

