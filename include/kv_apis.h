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
/**
 * @file     kv_apis.h
 * @brief    KV APIs
 */

#ifndef KV_APIS_C_H
#define KV_APIS_C_H

#include "kv_types.h"
#include "spdk/env.h"

#ifdef __cplusplus
extern "C" {
#endif

//
// Device APIs
//

/**
 * @brief Initializes KV SSD and KV cache
 * @param init_from types of initializing SDK
 * @param option configuration file's path or pointer of config structure
 * @return KV_SUCCESS
 * @return KV_ERR_SDK_OPEN
 */
int kv_sdk_init(int init_from, void *option);

/**
 * @brief Initializes KV SSD and KV cache with spdk_env_opts structure
 * @param init_from types of initializing SDK
 * @param option configuration file's path or pointer of config structure
 * @param spdk_opts spdk config structure
 * @return KV_SUCCESS
 * @return KV_ERR_SDK_OPEN
 */
int kv_sdk_init_with_spdk_opts(int init_from, void *option, struct spdk_env_opts *spdk_opts);

/**
 * @brief  Load init option from file(json) or in-memory configuration
 * @return KV_SUCCESS
 * @return KV_ERR_SDK_OPTION_LOAD
 */
int kv_sdk_load_option(kv_sdk *sdk_opt, char* log_path);

/**
 * @brief De-initializes KV SSD and KV cache
 * @return KV_SUCCESS
 * @return KV_ERR_SDK_CLOSE
 */
int kv_sdk_finalize(void);

/**
 * @brief Returns CPU ID
 * @return >=0: # of CPU ID
 * @return  -1: error
 */
int kv_get_core_id(void);

/**
 * @brief Returns the total size of the KV SSD in bytes
 * @param handle device handle
 * @return >0: total size of the device
 * @return KV_ERR_INVALID_VALUE: size read fail
 * @return KV_ERR_SDK_INVALID_PARAM: invalid parameter
 */
uint64_t kv_get_total_size(uint64_t handle);

/**
 * @brief Returns a used ratio of the KV SSD, from 0(0.00%) to 10,000(100.00%)
 * @param handle device handle
 * @return 0~10000: total used ratio of the device
 * @return KV_ERR_INVALID_VALUE: read fail
 * @return KV_ERR_SDK_INVALID_PARAM: invalid parameter
 */
uint64_t kv_get_used_size(uint64_t handle);

/**
 * @brief Returns the write amplification factor of the CPU
 * @param handle device handle
 * @return >0: WAF value from vendor log page
 * @return KV_ERR_INVALID_VALUE: WAF value read fail
 * @return KV_ERR_SDK_INVALID_PARAM: invalid parameter
 */
uint64_t kv_get_waf(uint64_t handle);

/**
 * @brief Returns the sector size of the KV SSD
 * @param handle device handle
 * @return >0: sector size of the device
 * @return 0: size read fail
 * @return KV_ERR_SDK_INVALID_PARAM: invalid parameter
 */
uint32_t kv_get_sector_size(uint64_t handle);

/*
* @brief Returns the number of sectors in the KV SSD
* @param handle device handle
* @return >0: number of sectors in the device
* @return 0: fail to get number of sectors in the device
* @return KV_ERR_SDK_INVALID_PARAM: invalid parameter
*/
uint64_t kv_get_num_sectors(uint64_t handle);

/**
 * @brief Returns the result of get log page Admin Command
 * @param handle device handle
 * @param log ID
 * @param buffer to store log data(OUT)
 * @param buffer size
 * @return KV_SUCCESS
 * @return KV_ERR_SDK_INVALID_PARAM
 * @return KV_ERR_IO
 */
int kv_get_log_page(uint64_t handle, uint8_t log_id, void* buffer, uint32_t buffer_size);


// Key-Value pair

/**
 * @brief Stores a key-value pair into device with sync I/O
 * @param handle device handle
 * @param kv kv_pair structure
 * @return KV_SUCCESS
 * @return KV_ERR_INVALID_VALUE_SIZE
 * @return KV_ERR_INVALID_KEY_SIZE
 * @return KV_ERR_INVALID_OPTION
 * @return KV_ERR_MISALIGNED_VALUE_SIZE
 * @return KV_ERR_MISALIGNED_KEY_SIZE
 * @return KV_ERR_UNRECOVERED_ERROR
 * @return KV_ERR_CAPACITY_EXCEEDED(TBD)
 * @return KV_ERR_IDEMPOTENT_STORE_FAIL(TBD)
 * @return KV_ERR_HEAP_ALLOC_FAILURE
 * @return KV_ERR_SLAB_ALLOC_FAILURE
 * @return KV_ERR_SDK_INVALID_PARAM
 */
int kv_store(uint64_t handle, kv_pair *kv);

/**
 * @brief Stores a key-value pair into device with async I/O
 * @param handle device handle
 * @param kv kv_pair structure
 * @return KV_SUCCESS
 * @return KV_ERR_INVALID_VALUE_SIZE
 * @return KV_ERR_INVALID_KEY_SIZE
 * @return KV_ERR_INVALID_OPTION
 * @return KV_ERR_MISALIGNED_VALUE_SIZE
 * @return KV_ERR_MISALIGNED_KEY_SIZE
 * @return KV_ERR_UNRECOVERED_ERROR
 * @return KV_ERR_CAPACITY_EXCEEDED(TBD)
 * @return KV_ERR_IDEMPOTENT_STORE_FAIL(TBD)
 * @return KV_ERR_HEAP_ALLOC_FAILURE
 * @return KV_ERR_SLAB_ALLOC_FAILURE
 * @return KV_ERR_SDK_INVALID_PARAM
 */
int kv_store_async(uint64_t handle, kv_pair *kv);

/**
 * @brief Retrives a value with the given key, sync I/O
 * @param handle device handle
 * @param kv kv_pair structure
 * @return KV_SUCCESS
 * @return KV_ERR_INVALID_VALUE_SIZE
 * @return KV_ERR_INVALID_VALUE_OFFSET
 * @return KV_ERR_INVALID_KEY_SIZE
 * @return KV_ERR_INVALID_OPTION
 * @return KV_ERR_MISALIGNED_VALUE_SIZE
 * @return KV_ERR_MISALIGNED_VALUE_OFFSET
 * @return KV_ERR_MISALIGNED_KEY_SIZE
 * @return KV_ERR_NOT_EXIST_KEY
 * @return KV_ERR_UNRECOVERED_ERROR
 * @return KV_ERR_HEAP_ALLOC_FAILURE
 * @return KV_ERR_SLAB_ALLOC_FAILURE
 * @return KV_ERR_SDK_INVALID_PARAM
 * @return KV_ERR_DECOMPRESSION (TBD)
 */
int kv_retrieve(uint64_t handle, kv_pair *kv);

/**
 * @brief Retrives a value with the given key, async I/O
 * @param handle device handle
 * @param kv kv_pair structure
 * @return KV_SUCCESS
 * @return KV_ERR_INVALID_VALUE_SIZE
 * @return KV_ERR_INVALID_VALUE_OFFSET
 * @return KV_ERR_INVALID_KEY_SIZE
 * @return KV_ERR_INVALID_OPTION
 * @return KV_ERR_MISALIGNED_VALUE_SIZE
 * @return KV_ERR_MISALIGNED_VALUE_OFFSET
 * @return KV_ERR_MISALIGNED_KEY_SIZE
 * @return KV_ERR_NOT_EXIST_KEY
 * @return KV_ERR_UNRECOVERED_ERROR
 * @return KV_ERR_HEAP_ALLOC_FAILURE
 * @return KV_ERR_SLAB_ALLOC_FAILURE
 * @return KV_ERR_SDK_INVALID_PARAM
 * @return KV_ERR_DECOMPRESSION (TBD)
 */
int kv_retrieve_async(uint64_t handle, kv_pair*kv);

/**
 * @brief Deletes value with given key, sync I/O
 * @param handle device handle
 * @param kv kv_pair structure
 * @return KV_SUCCESS
 * @return KV_ERR_INVALID_OPTION
 * @return KV_ERR_NOT_EXIST_KEY
 * @return KV_ERR_UNRECOVERED_ERROR
 * @return KV_ERR_HEAP_ALLOC_FAILURE
 * @return KV_ERR_SLAB_ALLOC_FAILURE
 * @return KV_ERR_SDK_INVALID_PARAM
 */
int kv_delete(uint64_t handle, kv_pair *kv);

/**
 * @brief Deletes value with given key, async I/O
 * @param handle device handle
 * @param kv kv_pair structure
 * @return KV_SUCCESS
 * @return KV_ERR_INVALID_OPTION
 * @return KV_ERR_NOT_EXIST_KEY
 * @return KV_ERR_UNRECOVERED_ERROR
 * @return KV_ERR_HEAP_ALLOC_FAILURE
 * @return KV_ERR_SLAB_ALLOC_FAILURE
 * @return KV_ERR_SDK_INVALID_PARAM
 */
int kv_delete_async(uint64_t handle, kv_pair *kv);

/**
 * @brief open iterate handle
 * @param handle Handle to the KV NVMe Device
 * @param keyspace_id keyspace_id
 * @param bitmask  bitmask 
 * @param prefix  prefix of matching key set
 * @param iterate_type one of four types (key-only, key-value, key-only with delete, key-value with delete )
 * @return > 0 : iterator id opened
 * @return = UINT8_MAX: fail to open iterator
 */
uint32_t kv_iterate_open(uint64_t handle, const uint8_t keyspace_id, const uint32_t bitmask, const uint32_t prefix, const uint8_t iterate_type);


/**
 * @brief close iterate handle
 * @param handle Handle to the KV NVMe Device
 * @param iterator  iterator handle being closed
 * @return = 0 : Success
 * @return != 0 : Fail to Close iterator
 */
int kv_iterate_close(uint64_t handle, const uint8_t iterator);

/**
 * @brief read matching key set from given iterator
 * @param handle Handle to the KV NVMe Device
 * @param it kv_iterate structure including iterator id and result buffer
 * @return = 0 : Success
 * @return != 0 : Fail to Close iterator
 */
int kv_iterate_read(uint64_t handle, kv_iterate* it);

/**
 * @brief read matching key set from given iterator asynchronously
 * @param handle Handle to the KV NVMe Device
 * @param it kv_iterate structure including iterator id and result buffer
 * @return = 0 : Success
 * @return != 0 : Fail to Close iterator
 */
int kv_iterate_read_async(uint64_t handle, kv_iterate* it);

/**
 * @brief return array describing iterate handle(s)
 * @param handle Handle to the KV NVMe Device
 * @param info iterate handle information array (OUT)
 * @param nr_handle number of iterate handles to get information (IN)
 * @return = 0 : Success
 * @return != 0 : Fail to Close iterator
 */
int kv_iterate_info(uint64_t handle, kv_iterate_handle_info* info, int nr_handle);

/**
 * @brief Appends value to existing value by given key(deprecated)
 * @param kv kv_pair structure
 * @return (deprecated. 2018.05.31) will return KV_ERR_DD_UNSUPPORTED_CMD
 * @return KV_SUCCESS
 * @return KV_ERR_INVALID_VALUE_SIZE
 * @return KV_ERR_INVALID_KEY_SIZE
 * @return KV_ERR_INVALID_OPTION
 * @return KV_ERR_MISALIGNED_VALUE_SIZE
 * @return KV_ERR_MISALIGNED_KEY_SIZE
 * @return KV_ERR_NOT_EXIST_KEY
 * @return KV_ERR_UNRECOVERED_ERROR
 * @return KV_ERR_CAPACITY_EXCEEDED(TBD)
 * @return KV_ERR_MAXIMUM_VALUE_SIZE_LIMIT_EXCEEDED
 * @return KV_ERR_HEAP_ALLOC_FAILURE
 * @return KV_ERR_SLAB_ALLOC_FAILURE
 * @return KV_ERR_SDK_INVALID_PARAM
 */
int kv_append(uint64_t handle, kv_pair *kv);

/**
 * @brief Checks if given key exist and returns status(status code=0 : exist, 0x10=not exist)
 * @param kv kv_pair structure
 * @return KV_SUCCESS
 * @return KV_ERR_NOT_EXIST_KEY
 * @return KV_ERR_SDK_INVALID_PARAM
 * @return KV_ERR_IO
 */
int kv_exist(uint64_t handle, kv_pair* kv);

/**
 * @brief Checks if given key exist and returns status in async manner (status code=0 : exist, 0x10=not exist)
 * @param kv kv_pair structure
 * @return KV_SUCCESS
 * @return KV_ERR_NOT_EXIST_KEY
 * @return KV_ERR_SDK_INVALID_PARAM
 * @return KV_ERR_IO
 */
int kv_exist_async(uint64_t handle, kv_pair* kv);

/**
 * @brief Format all KV SSDs
 * @param handle device handle
 * @param erase_user_data 0=erase map only, 1=erase user data
 * @return KV_SUCCESS
 * @return KV_ERR_SDK_INVALID_PARAM
 * @return KV_ERR_IO
 */
int kv_format_device(uint64_t handle, int erase_user_data);

/**
 * @brief Returns I/O Queue type for current (I/O)thread
 * @param handle device handle
 * @param core_id CPU(=I/O queue) ID
 * @return SYNC_IO_QUEUE (1)
 * @return ASYNC_IO_QUEUE (2)
 * @return <0 ERROR
 */
int kv_io_queue_type(uint64_t handle, int core_id);

/**
 * @brief Returns KV SSD handles and number that is able to access from given core_id
 * @param core_id CPU(=I/O queue) ID
 * @param nr_device the number of the KV SSDs(OUT)
 * @param arr_handle the array containing KV SSDs' handles(OUT)
 * @return KV_SUCCESS
 * @return KV_ERR_IO
 */
int kv_get_devices_on_cpu(int core_id, int* nr_device, uint64_t* arr_handle);

/**
 * @brief Returns KV SSD handles and number that is able to access from the caller process
 * @param nr_device the number of the KV SSDs(OUT)
 * @param arr_handle the array containing KV SSDs' handles(OUT)
 * @return KV_SUCCESS
 * @return KV_ERR_IO
 */
int kv_get_device_handles(int* nr_device, uint64_t* arr_handle);

/**
 * @brief Returns core_id array that is able to access on given ssd handle
 * @param handle  device handle
 * @param nr_core the number of core(OUT)
 * @param arr_core the array containing core_ids(OUT)
 * @return KV_SUCCESS
 * @return KV_ERR_IO
*/
int kv_get_cpus_on_device(uint64_t handle, int* nr_core, int* arr_core);

/**
 @brief Returns whether SDK is initialized.
	when not initialized(=return 0), most of SDK APIs will not work
 @return 1 = initialized , 0 = not initialized
*/
int kv_is_sdk_initialized(void);

/**
 * @breif Returns a KV SSD device id for the given handle
 * @param handle device handle
 * @return device id
 */
int kv_get_dev_idx_on_handle(uint64_t handle);

/**
 * @brief Set affinity for sync IO
 * @param did device id
 * @return true = switched context, false = cannot switch context
 */
bool context_switch_async_to_sync(int did);

/**
 * @brief Set affinity for async IO
 * @param did device id
 * @return true = switched context, false = cannot switch context
 */
bool context_switch_sync_to_async(int did);

/**
 * @brief Show API Info (buildtime / system info)
 */
void kv_sdk_info(void);

/**
 * @brief process completion on given ssd handle
 * @param handle device handle
 */
void kv_process_completion(uint64_t handle);

/**
 * @brief process completion on given queue id of ssd handle
 * @param handle device handle
 * @param queue_id ID of an I/O Queue to process
 */
void kv_process_completion_queue(uint64_t handle, uint32_t queue_id);

#ifdef __cplusplus
} // extern "C"
#endif

#endif /* KV_APIS_C_H */
