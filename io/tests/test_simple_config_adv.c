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
#include "kvutil.h"
#include "latency_stat.h"
#define MAX_NTHREAD (320)

typedef struct {
	int tid;
	int core_id;
	uint64_t device_handle;
	struct timeval *t_start;
	struct timeval *t_end;
} thread_param;

typedef struct {
	struct timeval start;
	struct timeval end;
	int tid;
} time_stamp;

struct time_stamp *stamps;
int insert_count;
kv_pair** kv;
int nthread_per_device;
int key_length;
int value_size;

int* is_async_io_complete;
pthread_mutex_t* g_mutex;
void sample_async_cb(kv_pair* kv, unsigned int result, unsigned int status) {
	assert(status == KV_SUCCESS);
	assert(kv != NULL);
	gettimeofday(&((time_stamp *) kv->param.private_data)->end, NULL);
	int tid = ((time_stamp *) kv->param.private_data)->tid;
	pthread_mutex_lock(&g_mutex[tid]);
	is_async_io_complete[tid]++;
	pthread_mutex_unlock(&g_mutex[tid]);
}

int (*kv_store_fptr[2])(uint64_t, kv_pair*) = {kv_store_async, kv_store };
int (*kv_retrieve_fptr[2])(uint64_t, kv_pair*) = {kv_retrieve_async, kv_retrieve };
int (*kv_delete_fptr[2])(uint64_t, kv_pair*) = {kv_delete_async, kv_delete };

void* simple_write(void* data) {
	thread_param *param = (thread_param*) data;
	int ret, tid = param->tid;
	uint64_t handle = param->device_handle;
	int is_sync;

	time_stamp* p_time_stamp;

	fprintf(stderr, "[tid=%d][handle=%lu]kv_store(_async): ", tid, handle);
	//prepare
	for (int i = 0 + (tid * insert_count); i < insert_count + (tid * insert_count); i++) {
		kv[i]->value.offset = 0;
		kv[i]->value.length = value_size;
		kv[i]->param.async_cb = sample_async_cb;
		kv[i]->param.io_option.store_option = KV_STORE_DEFAULT;
		((time_stamp *) kv[i]->param.private_data)->tid = tid;
	}

	//IO
	gettimeofday(param->t_start, NULL);
	for (int i = 0 + (tid * insert_count); i < insert_count + (tid * insert_count); i++) {
		is_sync = i & 0x1;
		if (!((i - (tid * insert_count)) % 10000)) {
			fprintf(stderr, "%d ", (i - (tid * insert_count)));
		}
		gettimeofday(&((time_stamp *) kv[i]->param.private_data)->start, NULL);
		ret = kv_store_fptr[is_sync](handle, kv[i]);
		assert(ret == KV_SUCCESS);
		if(is_sync) {
			gettimeofday(&((time_stamp *) kv[i]->param.private_data)->end, NULL);
		}
	}

	while (is_async_io_complete[tid] < insert_count / 2) usleep(1);
	fprintf(stderr, "Done\n");
	gettimeofday(param->t_end, NULL);

	return NULL;
}

void* simple_read(void* data) {
	thread_param *param = (thread_param*) data;
	int ret, tid = param->tid;
	uint64_t handle = param->device_handle;
	int is_sync;

	time_stamp* p_time_stamp;

	fprintf(stderr, "[tid=%d][handle=%lu]kv_retrieve(_async): ", tid, handle);
	//prepare
	for (int i = 0 + (tid * insert_count); i < insert_count + (tid * insert_count); i++) {
		kv[i]->value.offset = 0;
		kv[i]->value.length = value_size;
		kv[i]->param.async_cb = sample_async_cb;
		kv[i]->param.io_option.retrieve_option = KV_RETRIEVE_DEFAULT;
		((time_stamp *) kv[i]->param.private_data)->tid = tid;
		memset(kv[i]->value.value, 0, value_size);
	}

	//IO
	gettimeofday(param->t_start, NULL);
	for (int i = 0 + (tid * insert_count); i < insert_count + (tid * insert_count); i++) {
		is_sync = i & 0x1;
		if (!((i - (tid * insert_count)) % 10000)) {
			fprintf(stderr, "%d ", (i - (tid * insert_count)));
		}
		gettimeofday(&((time_stamp *) kv[i]->param.private_data)->start, NULL);
		ret = kv_retrieve_fptr[is_sync](handle, kv[i]);
		assert(ret == KV_SUCCESS);
		if(is_sync) {
			gettimeofday(&((time_stamp *) kv[i]->param.private_data)->end, NULL);
		}
	}

	while (is_async_io_complete[tid] < insert_count / 2) usleep(1);
	fprintf(stderr, "Done\n");
	gettimeofday(param->t_end, NULL);

	return NULL;
}

void* simple_delete(void* data) {
	thread_param *param = (thread_param*) data;
	int ret, tid = param->tid;
	uint64_t handle = param->device_handle;
	int is_sync;

	time_stamp* p_time_stamp;

	fprintf(stderr, "[tid=%d][handle=%lu]kv_delete(_async): ", tid, handle);
	//prepare
	for (int i = 0 + (tid * insert_count); i < insert_count + (tid * insert_count); i++) {
		kv[i]->value.offset = 0;
		kv[i]->value.length = 0;
		kv[i]->param.async_cb = sample_async_cb;
		kv[i]->param.io_option.delete_option = KV_DELETE_DEFAULT;
		((time_stamp *) kv[i]->param.private_data)->tid = tid;
	}

	//IO
	gettimeofday(param->t_start, NULL);
	for (int i = 0 + (tid * insert_count); i < insert_count + (tid * insert_count); i++) {
		is_sync = i & 0x1;
		if (!((i - (tid * insert_count)) % 10000)) {
			fprintf(stderr, "%d ", (i - (tid * insert_count)));
		}
		gettimeofday(&((time_stamp *) kv[i]->param.private_data)->start, NULL);
		ret = kv_delete_fptr[is_sync](handle, kv[i]);
		assert(ret == KV_SUCCESS);
		if(is_sync) {
			gettimeofday(&((time_stamp *) kv[i]->param.private_data)->end, NULL);
		}
	}

	while (is_async_io_complete[tid] < insert_count / 2) usleep(1);
	fprintf(stderr, "Done\n");
	gettimeofday(param->t_end, NULL);

	return NULL;
}


int simple_ut() {
	printf("%s start\n", __FUNCTION__);

	key_length = 16;
	value_size = 4096;
	nthread_per_device = 32;

	int ret;
	insert_count = 10000;
	struct latency_stat stat;

	int nr_handle = 0;
	uint64_t arr_handle[NR_MAX_SSD];

	pthread_t t[MAX_NTHREAD];
	thread_param p[MAX_NTHREAD];
	int status[MAX_NTHREAD];
	struct timeval thread_start[MAX_NTHREAD];
	struct timeval thread_end[MAX_NTHREAD];

	//SDK init
	kv_sdk sdk_opt = { 0, };
	sprintf(sdk_opt.dev_id[0], "0000:02:00.0");
	ret = kv_sdk_init(KV_SDK_INIT_FROM_STR, &sdk_opt);

	// ret = kv_sdk_init(KV_SDK_INIT_FROM_JSON, "kv_sdk_simple_config.json");
	assert(ret == KV_SUCCESS);

	//Get handles of devices
	ret = kv_get_device_handles(&nr_handle, arr_handle);
	assert(ret == KV_SUCCESS);

	//Prepare threads
	for(int i = 0; i < nr_handle * nthread_per_device; i++) {
		p[i].tid = i;
		p[i].device_handle = arr_handle[i / nthread_per_device];
		p[i].t_start = &thread_start[i];
		p[i].t_end = &thread_end[i];
	}
	is_async_io_complete = (int*)calloc(nthread_per_device * nr_handle, sizeof(int));
	assert(is_async_io_complete);
	g_mutex = (pthread_mutex_t *)calloc(nthread_per_device * nr_handle, sizeof(pthread_mutex_t));
	assert(g_mutex);
	for (int i = 0; i < nthread_per_device * nr_handle; i++) {
		g_mutex[i] = (pthread_mutex_t )PTHREAD_MUTEX_INITIALIZER;
	}

	//Prepare key-value pairs
	kv = (kv_pair**) malloc(sizeof(kv_pair*) * insert_count * nthread_per_device * nr_handle);
	assert(kv);
	for (int i = 0; i < insert_count * nthread_per_device * nr_handle; i++) {
		kv[i] = (kv_pair*) malloc(sizeof(kv_pair));
		assert(kv[i]);
		kv[i]->key.length = key_length;
		kv[i]->key.key = malloc(key_length + 1);
		assert(kv[i]->key.key);
		sprintf(kv[i]->key.key, "mou%012d", i);
		kv[i]->keyspace_id = KV_KEYSPACE_IODATA;

		kv[i]->value.value = malloc(value_size);
		assert(kv[i]->value.value);
		memset(kv[i]->value.value, 'a' + (i % 25), value_size);

		kv[i]->param.private_data = malloc(sizeof(time_stamp));
		assert(kv[i]->param.private_data);
	}

	//Do I/O, handle by handle
	//Store
	for(int tid = 0; tid < nthread_per_device * nr_handle; tid++) {
		assert(0 <= pthread_create(&t[tid], NULL, simple_write, &p[tid]));
	}

	for(int tid = 0; tid < nthread_per_device * nr_handle; tid++) {
		pthread_join(t[tid],(void**)&status[tid]);
	}
	reset_latency_stat(&stat);
	for (int i = 0; i < insert_count * nthread_per_device * nr_handle; i++) {
		time_stamp* p_time_stamp = (time_stamp*) kv[i]->param.private_data;
		add_latency_stat(&stat, &p_time_stamp->start, &p_time_stamp->end);
	}
	show_elapsed_time_cumulative(thread_start, thread_end, nthread_per_device * nr_handle, "simple_ut_2_store", insert_count, value_size, &stat);
	for(int tid = 0; tid < nthread_per_device * nr_handle; tid++) {
		is_async_io_complete[tid] = 0;
	}

	//Retrieve
	for(int tid = 0; tid < nthread_per_device * nr_handle; tid++) {
		assert(0 <= pthread_create(&t[tid], NULL, simple_read, &p[tid]));
	}

	for(int tid = 0; tid < nthread_per_device * nr_handle; tid++) {
		pthread_join(t[tid],(void**)&status[tid]);
	}
	reset_latency_stat(&stat);
	for (int i = 0; i < insert_count * nthread_per_device * nr_handle; i++) {
		time_stamp* p_time_stamp = (time_stamp*) kv[i]->param.private_data;
		add_latency_stat(&stat, &p_time_stamp->start, &p_time_stamp->end);
	}
	show_elapsed_time_cumulative(thread_start, thread_end, nthread_per_device * nr_handle, "simple_ut_2_retrieve", insert_count, value_size, &stat);
	for(int tid = 0; tid < nthread_per_device * nr_handle; tid++) {
		is_async_io_complete[tid] = 0;
	}

	//Delete
	for(int tid = 0; tid < nthread_per_device * nr_handle; tid++) {
		assert(0 <= pthread_create(&t[tid], NULL, simple_delete, &p[tid]));
	}

	for(int tid = 0; tid < nthread_per_device * nr_handle; tid++) {
		pthread_join(t[tid],(void**)&status[tid]);
	}
	reset_latency_stat(&stat);
	for (int i = 0; i < insert_count * nthread_per_device * nr_handle; i++) {
		time_stamp* p_time_stamp = (time_stamp*) kv[i]->param.private_data;
		add_latency_stat(&stat, &p_time_stamp->start, &p_time_stamp->end);
	}
	show_elapsed_time_cumulative(thread_start, thread_end, nthread_per_device * nr_handle, "simple_ut_2_delete", insert_count, value_size, &stat);
	for(int tid = 0; tid < nthread_per_device * nr_handle; tid++) {
		is_async_io_complete[tid] = 0;
	}

	//Clean memory
	for (int i = 0; i < insert_count * nthread_per_device * nr_handle; i++) {
		if (kv[i]->key.key) free(kv[i]->key.key);
		if (kv[i]->value.value) free(kv[i]->value.value);
		if (kv[i]->param.private_data) free(kv[i]->param.private_data);
		if (kv[i]) free(kv[i]);
	}
	if (kv) free(kv);
	if (is_async_io_complete) free(is_async_io_complete);
	if (g_mutex) free(g_mutex);

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

