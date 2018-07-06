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
#include <assert.h>
#include "kv_types.h"
#include "kv_apis.h"

volatile int is_async_io_complete;
void sample_async_cb(kv_pair* kv, unsigned int result, unsigned int status) {
	assert(status == KV_SUCCESS);
	assert(kv != NULL);
	is_async_io_complete++;
}

int simple_ut() {
	printf("%s start\n", __FUNCTION__);

	int key_length = 16;
	int value_size = 4096;

	kv_pair kv_one;
	int ret;

	int nr_handle = 0;
	uint64_t arr_handle[NR_MAX_SSD];

	//SDK init
	kv_sdk sdk_opt = { 0, };
	sprintf(sdk_opt.dev_id[0], "0000:02:00.0");
	ret = kv_sdk_init(KV_SDK_INIT_FROM_STR, &sdk_opt);

	// you can initialize sdk on this way
	//ret = kv_sdk_init(KV_SDK_INIT_FROM_JSON, "kv_sdk_simple_config.json");
	assert(ret == KV_SUCCESS);

	//Get handles of devices
	ret = kv_get_device_handles(&nr_handle, arr_handle);
	assert(ret == KV_SUCCESS);

	//Prepare a key-value pair
	kv_one.key.length = key_length;
	kv_one.key.key = malloc(key_length + 1);
	kv_one.keyspace_id = KV_KEYSPACE_IODATA;
	assert(kv_one.key.key != NULL);
	memset(kv_one.key.key, 0, key_length) + 1;

	kv_one.value.value = malloc(value_size);
	assert(kv_one.value.value != NULL);
	memset(kv_one.value.value, 'a', value_size);

	kv_one.param.private_data = NULL;

	//Do I/O, handle by handle
	for (int handle_idx = 0; handle_idx < nr_handle; handle_idx++) {
		uint64_t handle = arr_handle[handle_idx];

		//Store
		kv_one.value.offset = 0;
		kv_one.value.length = value_size;
		kv_one.param.io_option.store_option = KV_STORE_DEFAULT;
		ret = kv_store(arr_handle[handle_idx], &kv_one);
		assert(ret == KV_SUCCESS);

		//Retrieve - Async
		kv_one.value.offset = 0;
		kv_one.value.length = value_size;
		memset(kv_one.value.value, 0, value_size); // value to be zeros
		kv_one.param.io_option.retrieve_option = KV_RETRIEVE_DEFAULT;
		kv_one.param.async_cb = sample_async_cb;
		ret = kv_retrieve_async(handle, &kv_one); // submit retrieve command
		assert(ret == KV_SUCCESS);
		while (!is_async_io_complete); // check whether the command is completed or not
		is_async_io_complete = 0; // init IO complete flag

		//Delete
		kv_one.value.offset = 0;
		kv_one.value.length = 0;
		kv_one.param.io_option.delete_option = KV_DELETE_DEFAULT;
		kv_one.param.async_cb = sample_async_cb;
		kv_delete_async(handle, &kv_one);
		assert(ret == KV_SUCCESS);
		while (!is_async_io_complete); // check whether the command is completed or not
		is_async_io_complete = 0; // init IO complete flag
	}

	//Clean memory
	if (kv_one.key.key) free(kv_one.key.key);
	if (kv_one.value.value) free(kv_one.value.value);

	printf("%s done\n", __FUNCTION__);

	//Finalize SDK
	ret = kv_sdk_finalize();

	return ret;
}

int main(void) {
	kv_sdk_info();
	int ret = simple_ut();

	return ret;
}

