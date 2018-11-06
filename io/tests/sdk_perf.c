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
#include <unistd.h>
#include <sys/time.h>

#include "kvutil.h"
#include "kv_types.h"
#include "kv_apis.h"
#include "kvnvme.h"
#include "latency_stat.h"
#define DISPLAY_CNT (10000)
#define SECTOR_SIZE (512)

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
kv_sdk sdk_opt;
kv_pair** kv;
int g_exist_answer;

unsigned long long seed = 0x0102030405060708LL;
int is_miscompare(unsigned long long *v1, unsigned long long *v2) {
	return !((v1[0] == v2[0]) && (v1[1] == v2[1]));
}

void* sdk_write(void* data) {
	thread_param *param = (thread_param*)data;
	int i, tid = param->tid;
	int core_id = param->core_id;
	uint64_t handle = param->device_handle;
	int ret = KV_SUCCESS;

	//set affinity as core_id
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(core_id, &cpuset);
	sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

	fprintf(stderr, "[cid=%d][tid=%d][handle=%lu]kv_store: ", core_id, tid, handle);
	//prepare
	for (i = 0 + (tid * insert_count); i < insert_count + (tid * insert_count); i++) {
                if(sdk_opt.ssd_type == LBA_TYPE_SSD) {
                        *((uint64_t*)(kv[i]->key.key)) = i * (kv[i]->value.length / SECTOR_SIZE);
			kv[i]->key.length = 8;
                } else {
                        memcpy(kv[i]->key.key, &i, MIN((size_t)kv[i]->key.length, sizeof(i)));
                }
		kv[i]->param.io_option.store_option = KV_STORE_DEFAULT;
		kv[i]->param.private_data = NULL;
		kv[i]->param.async_cb = NULL;
	}

	//IO
	gettimeofday(param->t_start, NULL);
	for (i = 0 + (tid * insert_count); i < insert_count + (tid * insert_count); i++) {
		if (!((i - (tid * insert_count)) % DISPLAY_CNT)) {
			fprintf(stderr, "%d ", (i - (tid * insert_count)));
		}
		gettimeofday(&stamps[i].start, NULL);
		ret = kv_store(handle, kv[i]);
		if(ret != KV_SUCCESS){
			fprintf(stderr,"kv_store failed ret = %d\n",ret);
		}
		gettimeofday(&stamps[i].end, NULL);
	}
	gettimeofday(param->t_end, NULL);
	fprintf(stderr, "Done\n");
}

void* sdk_read(void* data) {
        thread_param *param = (thread_param*)data;
        int i, tid = param->tid;
        int core_id = param->core_id;
        uint64_t handle = param->device_handle;
	int ret = KV_SUCCESS;

	//set affinity as core_id
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(core_id, &cpuset);
	sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

	fprintf(stderr, "[cid=%d][tid=%d][handle=%lu]kv_retrieve: ", core_id, tid, handle);
	//prepare
	for (i = insert_count + (tid * insert_count) - 1; i >= 0 + (tid * insert_count); i--) {
                if(sdk_opt.ssd_type == LBA_TYPE_SSD) {
                        *((uint64_t*)(kv[i]->key.key)) = i * (kv[i]->value.length / SECTOR_SIZE);
			kv[i]->key.length = 8;
                } else {
                        memcpy(kv[i]->key.key, &i, MIN((size_t)kv[i]->key.length, sizeof(i)));
                }
		kv[i]->param.io_option.retrieve_option = KV_RETRIEVE_DEFAULT;
		kv[i]->param.private_data = NULL;
		kv[i]->param.async_cb = NULL;
	}

	//IO
	gettimeofday(param->t_start, NULL);
	for (i = insert_count + (tid * insert_count) - 1; i >= 0 + (tid * insert_count); i--) {
		if (!((i - (tid * insert_count)) % DISPLAY_CNT)) {
			fprintf(stderr, "%d ", (i - (tid * insert_count)));
		}
		gettimeofday(&stamps[i].start, NULL);
		ret = kv_retrieve(handle, kv[i]);
		if(ret != KV_SUCCESS){
			fprintf(stderr,"kv_retrieve failed ret = %d\n",ret);
		}
		gettimeofday(&stamps[i].end, NULL);
	}
	gettimeofday(param->t_end, NULL);
	fprintf(stderr, "Done\n");
}

void* sdk_exist(void* data) {
        thread_param *param = (thread_param*)data;
        int i, tid = param->tid;
        int core_id = param->core_id;
        uint64_t handle = param->device_handle;
	int ret = KV_SUCCESS;

	//set affinity as core_id
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(core_id, &cpuset);
	sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

	fprintf(stderr, "[cid=%d][tid=%d][handle=%lu]kv_exist: ", core_id, tid, handle);
	//prepare
	for (i = 0 + (tid * insert_count); i < insert_count + (tid * insert_count); i++) {
		memcpy(kv[i]->key.key, &i, MIN((size_t)kv[i]->key.length, sizeof(i)));
		kv[i]->param.io_option.exist_option = KV_EXIST_DEFAULT;
		kv[i]->param.private_data = NULL;
		kv[i]->param.async_cb = NULL;
	}

	//IO
	gettimeofday(param->t_start, NULL);
	for (i = 0 + (tid * insert_count); i < insert_count + (tid * insert_count); i++) {
		if (!((i - (tid * insert_count)) % DISPLAY_CNT)) {
			fprintf(stderr, "%d ", (i - (tid * insert_count)));
		}
		gettimeofday(&stamps[i].start, NULL);
		ret = kv_exist(handle, kv[i]);
		if(g_exist_answer != ret) {
			fprintf(stderr, "%s fail: returned status(0x%x) != expected value(0x%x)\n", __FUNCTION__, ret, g_exist_answer);
			//exit(1);
		}
		gettimeofday(&stamps[i].end, NULL);
	}
	gettimeofday(param->t_end, NULL);
	fprintf(stderr, "Done\n");
}

void* sdk_delete(void* data) {
        thread_param *param = (thread_param*)data;
        int i, tid = param->tid;
        int core_id = param->core_id;
        uint64_t handle = param->device_handle;
	int ret = KV_SUCCESS;

	//set affinity as core_id
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(core_id, &cpuset);
	sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

	fprintf(stderr, "[cid=%d][tid=%d][handle=%lu]kv_delete: ", core_id, tid, handle);
	//prepare
	for (i = 0 + (tid * insert_count); i < insert_count + (tid * insert_count); i++) {
		if(sdk_opt.ssd_type == LBA_TYPE_SSD) {
			*((uint64_t*)(kv[i]->key.key)) = i * (kv[i]->value.length / SECTOR_SIZE);
			kv[i]->key.length = 8;
		} else {
                        memcpy(kv[i]->key.key, &i, MIN((size_t)kv[i]->key.length, sizeof(i)));
		}
		kv[i]->param.io_option.delete_option = KV_DELETE_DEFAULT;
		kv[i]->param.private_data = NULL;
		kv[i]->param.async_cb = NULL;
	}

	//IO
	gettimeofday(param->t_start, NULL);
	for (i = 0 + (tid * insert_count); i < insert_count + (tid * insert_count); i++) {
		if (!((i - (tid * insert_count)) % DISPLAY_CNT)) {
			fprintf(stderr, "%d ", (i - (tid * insert_count)));
		}
		gettimeofday(&stamps[i].start, NULL);
		ret = kv_delete(handle, kv[i]);
		if(ret != KV_SUCCESS){
			fprintf(stderr,"kv_delete failed ret = %d\n",ret);
		}
		gettimeofday(&stamps[i].end, NULL);
	}
	gettimeofday(param->t_end, NULL);
	fprintf(stderr, "Done\n");
}

int sdk_perf() {
	fprintf(stderr, "%s start\n", __FUNCTION__);

	int key_length = 16;
	int value_size = 4096;
	insert_count = 10 * 10000;
	int check_miscompare = 0;

	int i;
	int core_id;
	int ret;
	uint64_t handle = 0;

	struct timeval start;
	struct timeval end;
	struct latency_stat stat;

	int nthreads = 0;
	int set_cores[MAX_CPU_CORES];
	int nr_handle = 0;
	uint64_t arr_handle[NR_MAX_SSD];
	memset((char*) set_cores, 0, sizeof(set_cores));
	memset((char*) arr_handle, 0, sizeof(arr_handle));

	pthread_t t[MAX_CPU_CORES * NR_MAX_SSD];
	thread_param p[MAX_CPU_CORES * NR_MAX_SSD];
	int status[MAX_CPU_CORES * NR_MAX_SSD];
	struct timeval thread_start[MAX_CPU_CORES * NR_MAX_SSD];
	struct timeval thread_end[MAX_CPU_CORES * NR_MAX_SSD];

	fprintf(stderr, "%s value_size=%d insert_count=%d\n", __FUNCTION__, value_size, insert_count);

	//SDK init
	gettimeofday(&start, NULL);
	ret = kv_sdk_load_option(&sdk_opt, "./kv_sdk_sync_config.json"); //load information from config.json to sdk_opt
	if(ret != KV_SUCCESS){
		fprintf(stderr,"kv_sdk_load_option failed ret = %d\n",ret);
		return -EIO;
	}
	ret = kv_sdk_init(KV_SDK_INIT_FROM_STR, &sdk_opt);
	if(ret != KV_SUCCESS){
		fprintf(stderr,"kv_sdk_init failed ret = %d\n",ret);
		return -EIO;
	}

	for (i = 0; i < sdk_opt.nr_ssd; i++) {
		for (int j = 0; j < MAX_CPU_CORES; j++) {
			if ((sdk_opt.dd_options[i].core_mask & (1ULL << j)) == 0) continue;
			if (!set_cores[j]) {
				set_cores[j] = 1;
				//get accesible cores by current core j
				ret = kv_get_devices_on_cpu(j, &nr_handle, arr_handle);
				fprintf(stderr, "current core(%d) is allowed to access on handle [", j);
				//make threads which number is same with the number of accessible devices
				for (int k = 0; k < nr_handle; k++) {
					printf("%ld,", arr_handle[k]);
					p[nthreads].tid = nthreads;
					p[nthreads].core_id = j;
					p[nthreads].device_handle = arr_handle[k];
					p[nthreads].t_start = &thread_start[nthreads];
					p[nthreads].t_end = &thread_end[nthreads];
					nthreads++;
				}
				fprintf(stderr, "]\n");
			}
		}
	}
	gettimeofday(&end, NULL);
	show_elapsed_time(&start, &end, "kv_sdk_init", 0, 0, NULL);

	int fd = open("/dev/srandom", O_RDONLY);
	if (fd < 3) {
		fd = open("/dev/urandom", O_RDONLY);
	}
	unsigned long long (*hash_value)[2] = (unsigned long long (*)[2]) malloc(sizeof(unsigned long long[2]) * insert_count * nthreads);

	//Prepare App Memory
	fprintf(stderr, "Setup App: ");
	kv = (kv_pair**) malloc(sizeof(kv_pair*) * insert_count * nthreads);
	if(!kv){
		fprintf(stderr,"fail to alloc kv_pair**\n");
		return -ENOMEM;
	}

	gettimeofday(&start, NULL);
	for (i = 0; i < insert_count * nthreads; i++) {
		if (!(i % DISPLAY_CNT)) {
			fprintf(stderr, "%d ", i);
		}
		kv[i] = (kv_pair*) malloc(sizeof(kv_pair));
		if(!kv){
			fprintf(stderr,"fail to alloc kv_pair\n");
			return -ENOMEM;
		}

		kv[i]->keyspace_id = KV_KEYSPACE_IODATA;
		kv[i]->key.key = malloc(key_length);
		if(kv[i]->key.key == NULL){
			fprintf(stderr,"fail to alloc key buffer\n");
			return -ENOMEM;
		}
		kv[i]->key.length = key_length;
		memset(kv[i]->key.key, 0, key_length);

		kv[i]->value.value = malloc(value_size);
		if(kv[i]->value.value == NULL){
			fprintf(stderr,"fail to alloc value buffer\n");
			return -ENOMEM;
		}
		kv[i]->value.length = value_size;
		kv[i]->value.actual_value_size = 0;
		kv[i]->value.offset = 0;
		if (check_miscompare) {
			if(read(fd, kv[i]->value.value, value_size) == value_size){
				fprintf(stderr,"fail to read miscompare value\n");
				return -EIO;
			}
			Hash128_2_P128(kv[i]->value.value, value_size, seed, hash_value[i]);
		} else {
			memset(kv[i]->value.value, 'a' + (i % 26), value_size);
		}
	}
	gettimeofday(&end, NULL);
	fprintf(stderr, "Done\n");
	show_elapsed_time(&start, &end, "Setup App", insert_count, 0, NULL);
	close(fd);

	//Print device information before test
	for (i = 0; i < sdk_opt.nr_ssd; i++) {
		uint64_t handle, total_size, used_size;
		double waf;

		handle = sdk_opt.dev_handle[i];
		total_size = kv_get_total_size(handle);

		fprintf(stderr, "[DEVICE #%d information before test]\n", i);
		if (total_size != KV_ERR_INVALID_VALUE) {
			fprintf(stderr, "  -Total Size of the NVMe Device: %lld MB\n", (unsigned long long) total_size / MB);
		}
		used_size = kv_get_used_size(handle);
		if (used_size != KV_ERR_INVALID_VALUE) {
	                if(sdk_opt.ssd_type == KV_TYPE_SSD){
                                printf("  -Used Size of the NVMe Device: %.2f %s \n", (float)used_size / 100, "%");
                        }
                        else{
                                printf("  -Used Size of the NVMe Device: %lld MB\n", (unsigned long long)used_size / MB);
                        }
		}
		waf = kv_get_waf(handle);
		if (waf != KV_ERR_INVALID_VALUE) {
			fprintf(stderr, "  -WAF Before doing I/O: %f\n", waf / 10);
		}
		fprintf(stderr, "\n");
	}

	//Store
	stamps = (struct time_stamp *) malloc(sizeof(struct time_stamp) * insert_count * nthreads);
	for (i = 0; i < nthreads; i++) {
		ret = pthread_create(&t[i], NULL, sdk_write, &p[i]);
		if(ret != KV_SUCCESS){
			fprintf(stderr, " fail to create writer thread(%d)\n", i);
			return -EIO;
		}
	}

	for (i = 0; i < nthreads; i++) {
		pthread_join(t[i], (void**) &status[i]);
	}
	reset_latency_stat(&stat);
	for (i = 0; i < insert_count * nthreads; i++) {
		add_latency_stat(&stat, &stamps[i].start, &stamps[i].end);
	}
	show_elapsed_time_cumulative(thread_start, thread_end, nthreads, "kv_store", insert_count, value_size, &stat);

	//Retrieve
	for (i = 0; i < nthreads; i++) {
		ret = pthread_create(&t[i], NULL, sdk_read, &p[i]);
		if(ret != KV_SUCCESS){
			fprintf(stderr, " fail to create reader thread(%d)\n", i);
			return -EIO;
		}
	}

	for (i = 0; i < nthreads; i++) {
		pthread_join(t[i], (void**) &status[i]);
	}
	reset_latency_stat(&stat);
	for (i = 0; i < insert_count * nthreads; i++) {
		add_latency_stat(&stat, &stamps[i].start, &stamps[i].end);
	}
	show_elapsed_time_cumulative(thread_start, thread_end, nthreads, "kv_retrieve", insert_count, value_size, &stat);

	if (check_miscompare) {
		printf("Checking miscompare count....");
		unsigned int miscompare_cnt = 0;
		for (i = 0; i < insert_count * nthreads; i++) {
			unsigned long long tmp_hash[2];
			Hash128_2_P128(kv[i]->value.value, kv[i]->value.length, seed, tmp_hash);
			if (is_miscompare(tmp_hash, hash_value[i])) {
				miscompare_cnt++;
			}
		}
		printf("Done.\n");
		printf("Miscompare count: %u\n\n\n", miscompare_cnt);
	}

	// Exist
	if (sdk_opt.ssd_type == KV_TYPE_SSD) {
		g_exist_answer = KV_SUCCESS;
		for (i = 0; i < nthreads; i++) {
			ret = pthread_create(&t[i], NULL, sdk_exist, &p[i]);
			if(ret != KV_SUCCESS){
				fprintf(stderr, " fail to create exist thread(%d)\n", i);
				return -EIO;
			}
		}

		for (i = 0; i < nthreads; i++) {
			pthread_join(t[i], (void**) &status[i]);
		}
		reset_latency_stat(&stat);
		for (i = 0; i < insert_count * nthreads; i++) {
			add_latency_stat(&stat, &stamps[i].start, &stamps[i].end);
		}
		show_elapsed_time_cumulative(thread_start, thread_end, nthreads, "kv_exist", insert_count, value_size, &stat);
	}

	// Delete
	for (i = 0; i < nthreads; i++) {
		ret = pthread_create(&t[i], NULL, sdk_delete, &p[i]);
		if(ret != KV_SUCCESS){
			fprintf(stderr, " fail to create delete thread(%d)\n", i);
			return -EIO;
		}
	}

	for (i = 0; i < nthreads; i++) {
		pthread_join(t[i], (void**) &status[i]);
	}
	reset_latency_stat(&stat);
	for (i = 0; i < insert_count * nthreads; i++) {
		add_latency_stat(&stat, &stamps[i].start, &stamps[i].end);
	}
	show_elapsed_time_cumulative(thread_start, thread_end, nthreads, "kv_delete", insert_count, value_size, &stat);


	// Exist after delete
	if (sdk_opt.ssd_type == KV_TYPE_SSD) {
		g_exist_answer = KV_ERR_NOT_EXIST_KEY;
		for (i = 0; i < nthreads; i++) {
			ret = pthread_create(&t[i], NULL, sdk_exist, &p[i]);
			if(ret != KV_SUCCESS){
				fprintf(stderr, " fail to create delete thread(%d)\n", i);
				return -EIO;
			}
		}

		for (i = 0; i < nthreads; i++) {
			pthread_join(t[i], (void**) &status[i]);
		}
		reset_latency_stat(&stat);
		for (i = 0; i < insert_count * nthreads; i++) {
			add_latency_stat(&stat, &stamps[i].start, &stamps[i].end);
		}
		show_elapsed_time_cumulative(thread_start, thread_end, nthreads, "kv_exist", insert_count, value_size, &stat);
	}

	//Print device information after test
	for (i = 0; i < sdk_opt.nr_ssd; i++) {
		uint64_t handle, total_size, used_size;
		double waf;

		handle = sdk_opt.dev_handle[i];
		total_size = kv_get_total_size(handle);

		fprintf(stderr, "[DEVICE #%d information after test]\n", i);
		if (total_size != KV_ERR_INVALID_VALUE) {
			fprintf(stderr, "  -Total Size of the NVMe Device: %lld MB\n", (unsigned long long) total_size / MB);
		}
		used_size = kv_get_used_size(handle);
		if (used_size != KV_ERR_INVALID_VALUE) {
                        if(sdk_opt.ssd_type == KV_TYPE_SSD){
                                fprintf(stderr, "  -Used Size of the NVMe Device: %.2f %s \n", (float)used_size / 100, "%");
                        }
                        else{
                                fprintf(stderr, "  -Used Size of the NVMe Device: %lld MB\n", (unsigned long long)used_size / MB);
                        }
		}
		waf = kv_get_waf(handle);
		if (waf != KV_ERR_INVALID_VALUE) {
			fprintf(stderr, "  -WAF After doing I/O: %f\n", waf / 10);
		}
		fprintf(stderr, "\n");
	}

	//Teardown Memory
	fprintf(stderr, "Teardown Memory: ");
	gettimeofday(&start, NULL);
	for (i = 0; i < insert_count * nthreads; i++) {
		if (!(i % DISPLAY_CNT)) {
			fprintf(stderr, "%d ", i);
		}
		if (kv[i]->key.key) free(kv[i]->key.key);
		if (kv[i]->value.value) free(kv[i]->value.value);
		if (kv[i]) free(kv[i]);
	}
	if(kv) free(kv);
	gettimeofday(&end, NULL);
	fprintf(stderr, "Done\n");
	show_elapsed_time(&start, &end, "Teardown Memory", insert_count, 0, NULL);
	free(hash_value);
	free(stamps);

	//Finalize SDK
	gettimeofday(&start, NULL);
	kv_sdk_finalize();
	gettimeofday(&end, NULL);
	show_elapsed_time(&start, &end, "kv_sdk_finalize", 0, 0, NULL);

	return 0;
}

int main(void) {
	kv_sdk_info();
	int ret = sdk_perf();
	return ret;
}

