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
#include "kvcache.h"
#include "latency_stat.h"

#include <pthread.h>

#define SECTOR_SIZE (512)

typedef struct {
	int tid;
	int core_id;
	uint64_t device_handle;
        struct timeval *t_start;
        struct timeval *t_end;
}thread_param;

typedef struct {
	struct timeval start;
	struct timeval end;
	int tid;
}time_stamp;

#define DISPLAY_CNT (10000)

kv_pair** kv;
int key_length;
int value_size;
int insert_count;
kv_sdk sdk_opt;

int g_exist_answer;

int* g_write_complete_count;
int* g_read_complete_count;
int* g_delete_complete_count;
int* g_exist_complete_count;

pthread_mutex_t* g_write_mutex; /*to protect complete cnt*/
pthread_mutex_t* g_read_mutex;
pthread_mutex_t* g_delete_mutex;
pthread_mutex_t* g_exist_mutex;

unsigned long long seed = 0x0102030405060708LL;
int is_miscompare(unsigned long long *v1, unsigned long long *v2) {
        return !((v1[0] == v2[0]) && (v1[1] == v2[1]));
}

/*callback for Write*/
void async_write_cb(kv_pair* kv, unsigned int result, unsigned int status){
	if (!kv) {
		fprintf(stderr, "%s fails: kv_pair is NULL\n", __FUNCTION__);
        	exit(1);
        }
	if(status != KV_SUCCESS){
		fprintf(stderr, "[%s] error. key=%s status code=%d value.length=%d value.actual_value_size=%d\n",
			__FUNCTION__, (char*)kv->key.key, status, kv->value.length, kv->value.actual_value_size);
	}

	time_stamp *p_time_stamp = (time_stamp *)kv->param.private_data;
        int tid = p_time_stamp->tid;
	gettimeofday(&p_time_stamp->end, NULL);

	pthread_mutex_lock(&g_write_mutex[tid]);
        g_write_complete_count[tid]++;
	pthread_mutex_unlock(&g_write_mutex[tid]);
}

/*callback for Read*/
void async_read_cb(kv_pair* kv, unsigned int result, unsigned int status){
        if (!kv) {
		fprintf(stderr, "%s fails: kv_pair is NULL\n", __FUNCTION__);
        	exit(1);
        }
	if(status != KV_SUCCESS){
		fprintf(stderr, "[%s] error. key=%s status code=%d value.length=%d value.actual_value_size=%d\n",
			__FUNCTION__, (char*)kv->key.key, status, kv->value.length, kv->value.actual_value_size);
	}
        time_stamp *p_time_stamp = (time_stamp *)kv->param.private_data;
        int tid = p_time_stamp->tid;
        gettimeofday(&p_time_stamp->end, NULL);

	pthread_mutex_lock(&g_read_mutex[tid]);
        g_read_complete_count[tid]++;
	pthread_mutex_unlock(&g_read_mutex[tid]);
}

/*callback for Exist*/
void async_exist_cb(kv_pair* kv, unsigned int result, unsigned int status){
	if(status != g_exist_answer) {
		fprintf(stderr, "%s fail: returned status(0x%x) != expected value(0x%x)\n", __FUNCTION__, status, g_exist_answer);
	}
	if (!kv) {
		fprintf(stderr, "%s fails: kv_pair is NULL\n", __FUNCTION__);
		exit(1);
	}
	time_stamp *p_time_stamp = (time_stamp *) kv->param.private_data;
	int tid = p_time_stamp->tid;
	gettimeofday(&p_time_stamp->end, NULL);

	pthread_mutex_lock(&g_exist_mutex[tid]);
	g_exist_complete_count[tid]++;
	pthread_mutex_unlock(&g_exist_mutex[tid]);
}

/*callback for Delete*/
void async_delete_cb(kv_pair* kv, unsigned int result, unsigned int status){
        if (!kv) {
		fprintf(stderr, "%s fails: kv_pair is NULL\n", __FUNCTION__);
        	exit(1);
        }
	if(status != KV_SUCCESS){
		fprintf(stderr, "[%s] error. key=%s status code=%d\n",
			__FUNCTION__, (char*)kv->key.key, status);
	}
        time_stamp *p_time_stamp = (time_stamp *)kv->param.private_data;
        int tid = p_time_stamp->tid;
        gettimeofday(&p_time_stamp->end, NULL);

	pthread_mutex_lock(&g_delete_mutex[tid]);
        g_delete_complete_count[tid]++;
	pthread_mutex_unlock(&g_delete_mutex[tid]);
}

void* sdk_write(void* data){
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

	time_stamp* p_time_stamp;

	fprintf(stderr,"[cid=%d][tid=%d][handle=%lu]kv_store_async: ", core_id, tid, handle);
	//prepare
        for(i=0+(tid*insert_count);i<insert_count+(tid*insert_count);i++){
		if(sdk_opt.ssd_type == LBA_TYPE_SSD) {
			*((uint64_t*)(kv[i]->key.key)) = i * (kv[i]->value.length / SECTOR_SIZE);
		} else {
			memcpy(kv[i]->key.key, &i, MIN((size_t)kv[i]->key.length, sizeof(i)));
		}
		kv[i]->param.async_cb = async_write_cb;
		kv[i]->param.io_option.store_option = KV_STORE_DEFAULT;
		((time_stamp *)kv[i]->param.private_data)->tid = tid;
        }

	//IO
        gettimeofday(param->t_start, NULL);
        for(i=0+(tid*insert_count);i<insert_count+(tid*insert_count);i++){
		if(!((i-(tid*insert_count))%DISPLAY_CNT)){
                        fprintf(stderr,"%d ", (i-(tid*insert_count)));
                }
		gettimeofday(&((time_stamp *)kv[i]->param.private_data)->start, NULL);
                ret = kv_store_async(handle, kv[i]);
		if(ret != KV_SUCCESS){
			fprintf(stderr,"kv_store_async failed ret = %d\n",ret);
		}
        }
	fprintf(stderr,"Done\n");
	
	while(g_write_complete_count[tid] != insert_count){
		usleep(1);
	}
        gettimeofday(param->t_end, NULL);
}


void* sdk_read(void* data){
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

	fprintf(stderr,"[cid=%d][tid=%d][handle=%lu]kv_retrieve_async: ", core_id, tid, handle);
	//prepare
        for(i=insert_count+(tid*insert_count)-1; i>=0+(tid*insert_count);i--){
		if(sdk_opt.ssd_type == LBA_TYPE_SSD) {
			*((uint64_t*)(kv[i]->key.key)) = i * (kv[i]->value.length / SECTOR_SIZE);
		} else {
			memcpy(kv[i]->key.key, &i, MIN((size_t)kv[i]->key.length, sizeof(i)));

		}
		kv[i]->param.async_cb = async_read_cb;
		kv[i]->param.io_option.retrieve_option= KV_RETRIEVE_DEFAULT;
		((time_stamp *)kv[i]->param.private_data)->tid = tid;
        }

	//IO
        gettimeofday(param->t_start, NULL);
        for(i=insert_count+(tid*insert_count)-1; i>=0+(tid*insert_count);i--){
		if(!((i-(tid*insert_count))%DISPLAY_CNT)){
                        fprintf(stderr,"%d ", (i-(tid*insert_count)));
                }
		gettimeofday(&((time_stamp *)kv[i]->param.private_data)->start, NULL);
	        ret = kv_retrieve_async(handle, kv[i]);
		if(ret != KV_SUCCESS){
			fprintf(stderr,"kv_retreive_async failed ret = %d\n",ret);
		}
        }
	fprintf(stderr,"Done\n");

	while(g_read_complete_count[tid] != insert_count){
		usleep(1);
	}
        gettimeofday(param->t_end, NULL);
}

void* sdk_exist(void* data) {
	thread_param *param = (thread_param*) data;
	int i, tid = param->tid;
	int core_id = param->core_id;
	uint64_t handle = param->device_handle;
	int ret =  KV_SUCCESS;

	//set affinity as core_id
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(core_id, &cpuset);
	sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

	fprintf(stderr, "[cid=%d][tid=%d][handle=%lu]kv_exist_async: ", core_id, tid, handle);
	//prepare
	for (i = 0 + (tid * insert_count); i < insert_count + (tid * insert_count); i++) {
		memcpy(kv[i]->key.key, &i, MIN((size_t)kv[i]->key.length, sizeof(i)));

		kv[i]->param.async_cb = async_exist_cb;
		kv[i]->param.io_option.exist_option = KV_EXIST_DEFAULT;
		((time_stamp *) kv[i]->param.private_data)->tid = tid;
	}

	//IO
	gettimeofday(param->t_start, NULL);
	for (i = 0 + (tid * insert_count); i < insert_count + (tid * insert_count); i++) {
		if (!((i - (tid * insert_count)) % DISPLAY_CNT)) {
			fprintf(stderr, "%d ", (i - (tid * insert_count)));
		}
		gettimeofday(&((time_stamp *) kv[i]->param.private_data)->start, NULL);
		ret = kv_exist_async(handle, kv[i]);
		if(ret != KV_SUCCESS){
			fprintf(stderr,"kv_exist_async failed ret = %d\n",ret);
		}
	}
	fprintf(stderr, "Done\n");

	while (g_exist_complete_count[tid] != insert_count) {
		usleep(1);
	}
	gettimeofday(param->t_end, NULL);
}

void* sdk_delete(void* data){
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

	fprintf(stderr,"[cid=%d][tid=%d][handle=%lu]kv_delete_async: ", core_id, tid, handle);
	//prepare
        for(i=0+(tid*insert_count);i<insert_count+(tid*insert_count);i++){
		if(sdk_opt.ssd_type == LBA_TYPE_SSD) {
			*((uint64_t*)(kv[i]->key.key)) = i * (kv[i]->value.length / SECTOR_SIZE);
		} else {
			memcpy(kv[i]->key.key, &i, MIN((size_t)kv[i]->key.length, sizeof(i)));
		}
		kv[i]->param.async_cb = async_delete_cb;
		kv[i]->param.io_option.delete_option = KV_DELETE_DEFAULT;
		((time_stamp *)kv[i]->param.private_data)->tid = tid;
        }

	//IO
        gettimeofday(param->t_start, NULL);
        for(i=0+(tid*insert_count);i<insert_count+(tid*insert_count);i++){
                if(!((i-(tid*insert_count))%DISPLAY_CNT)){
                        fprintf(stderr,"%d ", (i-(tid*insert_count)));
                }
		gettimeofday(&((time_stamp *)kv[i]->param.private_data)->start, NULL);
	        ret = kv_delete_async(handle, kv[i]);
		if(ret != KV_SUCCESS){
			fprintf(stderr,"kv_delete_async failed ret = %d\n",ret);
		}
        }
	fprintf(stderr,"Done\n");

	while(g_delete_complete_count[tid] != insert_count){
		usleep(1);
	}
        gettimeofday(param->t_end, NULL);
}


int test(void){
	printf("%s start\n",__FUNCTION__);

	key_length = 16;
	value_size = 4096;
	insert_count = 10 * 10000;
	int nthreads = 0;
	int set_cores[MAX_CPU_CORES];
	int nr_handle = 0;
	uint64_t arr_handle[NR_MAX_SSD];
	memset((char*)set_cores,0,sizeof(set_cores));
	memset((char*)arr_handle,0,sizeof(arr_handle));

	int check_miscompare = 0;
	int i, ret = 0;

	struct timeval start, end;
	struct latency_stat stat;
	time_stamp* p_time_stamp;

        pthread_t t[MAX_CPU_CORES * NR_MAX_SSD];
        thread_param p[MAX_CPU_CORES * NR_MAX_SSD];
        int status[MAX_CPU_CORES * NR_MAX_SSD];
	struct timeval thread_start[MAX_CPU_CORES * NR_MAX_SSD];
	struct timeval thread_end[MAX_CPU_CORES * NR_MAX_SSD];

	//Init SDK
	gettimeofday(&start, NULL);
	//ret = kv_sdk_init(KV_SDK_INIT_FROM_JSON, "./kv_sdk_async_config.json");
	ret = kv_sdk_load_option(&sdk_opt, "./kv_sdk_async_config.json");//load information from config.json to sdk_opt
	if(ret != KV_SUCCESS){
		fprintf(stderr,"kv_sdk_load_option failed ret = %d\n",ret);
		return -EIO;
	}
        ret = kv_sdk_init(KV_SDK_INIT_FROM_STR, &sdk_opt);
	if(ret != KV_SUCCESS){
		fprintf(stderr,"kv_sdk_init failed ret = %d\n",ret);
		return -EIO;
	}

        for(i = 0; i < sdk_opt.nr_ssd; i++){
                for(int j = 0 ; j < MAX_CPU_CORES; j++){
			if ((sdk_opt.dd_options[i].core_mask & (1ULL << j)) == 0) continue;
			if (!set_cores[j]){
				set_cores[j] = 1;
				//get accesible cores by current core j
				ret = kv_get_devices_on_cpu(j, &nr_handle, arr_handle);
				fprintf(stderr, "current core(%d) is allowed to access on handle [", j);
				//make threads which number is same with the number of accessible devices
				for(int k = 0 ; k < nr_handle ; k++){
					printf("%ld,",arr_handle[k]);
					p[nthreads].tid = nthreads;
	                                p[nthreads].core_id = j;
					p[nthreads].device_handle = arr_handle[k];
                                        p[nthreads].t_start = &thread_start[nthreads];
                                        p[nthreads].t_end = &thread_end[nthreads];
	                                nthreads++;
				}
				fprintf(stderr,"]\n");
                        }
                }
        }

	kv = (kv_pair**)malloc(sizeof(kv_pair*)*insert_count*nthreads);
	if(!kv){
		fprintf(stderr,"fail to alloc kv_pair**\n");
		return -ENOMEM;
	}

	fprintf(stderr,"[TEST INFO] value_size=%d insert_count=%d, num_threads=%d\n",value_size,insert_count, nthreads);

	gettimeofday(&end, NULL);
	show_elapsed_time(&start,&end,"kv_sdk_init",0,0,NULL);

	//Init complete_count variable and mutex(for complete count)
	g_write_complete_count = (int *)calloc(nthreads, sizeof(int));
	g_read_complete_count = (int *)calloc(nthreads, sizeof(int));
	g_delete_complete_count = (int *)calloc(nthreads, sizeof(int));
	g_exist_complete_count = (int *)calloc(nthreads, sizeof(int));
	if(!g_write_complete_count || !g_read_complete_count || !g_delete_complete_count || !g_exist_complete_count){
		fprintf(stderr,"fail to alloc global counter\n");
		return -ENOMEM;
	}
	g_write_mutex = (pthread_mutex_t *)calloc(nthreads, sizeof(pthread_mutex_t));
	g_read_mutex = (pthread_mutex_t *)calloc(nthreads, sizeof(pthread_mutex_t));
	g_delete_mutex = (pthread_mutex_t *)calloc(nthreads, sizeof(pthread_mutex_t));
	g_exist_mutex = (pthread_mutex_t *)calloc(nthreads, sizeof(pthread_mutex_t));
	if(!g_write_mutex || !g_read_mutex || !g_delete_mutex || !g_exist_mutex){
		fprintf(stderr,"fail to alloc global counter mutex\n");
		return -ENOMEM;
	}

	for(i=0;i<nthreads;i++){
		g_write_mutex[i] = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
		g_read_mutex[i] = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
		g_delete_mutex[i] = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;		
	}

	int fd = open("/dev/srandom", O_RDONLY);
        if (fd < 3){
                fd = open("/dev/urandom", O_RDONLY);
        }
	unsigned long long (*hash_value)[2] = (unsigned long long(*)[2])malloc(sizeof(unsigned long long [2]) * insert_count * nthreads);

	//Prepare App Memory 
	fprintf(stderr," Setup App: ");
	gettimeofday(&start, NULL);
	for(i=0;i<insert_count*nthreads;i++){
	        if(!(i%10000)){
	                fprintf(stderr," %d ",i);
	        }
	        kv[i] = (kv_pair*)malloc(sizeof(kv_pair));
		if(!kv){
			fprintf(stderr,"fail to alloc kv_pair\n");
			return -ENOMEM;
		}

		kv[i]->keyspace_id = KV_KEYSPACE_IODATA;
		kv[i]->key.key = malloc(key_length);//16
		if(kv[i]->key.key == NULL){
			fprintf(stderr,"fail to alloc key buffer\n");
			return -ENOMEM;
		}
		kv[i]->key.length = key_length;
		memset(kv[i]->key.key,0,key_length);

	        kv[i]->value.value = malloc(value_size);
		if(kv[i]->value.value == NULL){
			fprintf(stderr,"fail to alloc value buffer\n");
			return -ENOMEM;
		}
	        kv[i]->value.length = value_size;
	        kv[i]->value.actual_value_size = 0;
		kv[i]->value.offset = 0;

		kv[i]->param.private_data = malloc(sizeof(time_stamp));

		if (check_miscompare) {
			if (read(fd, kv[i]->value.value, value_size) != value_size) {
				fprintf(stderr,"fail to read miscompare value\n");
				return -EIO;
			}
			Hash128_2_P128(kv[i]->value.value, value_size, seed, hash_value[i]);
		} else {
		        memset(kv[i]->value.value,'a'+(i%26),value_size);
		}
	}
	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	show_elapsed_time(&start,&end,"Setup App",insert_count*nthreads, 0, NULL);
	close(fd);

	reset_latency_stat(&stat);

	//Print device information before test
	for(i=0;i<sdk_opt.nr_ssd;i++){
		uint64_t handle, total_size, used_size;
		double waf;
		
		handle = sdk_opt.dev_handle[i];
	        total_size = kv_get_total_size(handle);
		
		fprintf(stderr, "[DEVICE #%d information before test]\n", i);
        	if (total_size != KV_ERR_INVALID_VALUE){
                	fprintf(stderr, "  -Total Size of the NVMe Device: %lld MB\n", (unsigned long long)total_size / MB);
	        }
	        used_size = kv_get_used_size(handle);
        	if (used_size != KV_ERR_INVALID_VALUE){
	                if(sdk_opt.ssd_type == KV_TYPE_SSD){
		                fprintf(stderr,"  -Used Size of the NVMe Device: %.2f %s \n", (float)used_size / 100, "%");
			}
			else{
				fprintf(stderr,"  -Used Size of the NVMe Device: %lld MB\n", (unsigned long long)used_size / MB);
			}
		}
	        waf = kv_get_waf(handle);
        	if (waf != KV_ERR_INVALID_VALUE) {
                	fprintf(stderr, "  -WAF Before doing I/O: %f\n", waf / 10);
	        }
		fprintf(stderr, "\n");
	}

	//Run Write Thread
	for(i=0;i<nthreads;i++){
		ret = pthread_create(&t[i], NULL, sdk_write, &p[i]);
		if(ret != KV_SUCCESS){
			fprintf(stderr, " fail to create writer thread(%d)\n", i);
			return -EIO;
		}
	}

	for(i=0;i<nthreads;i++){
		pthread_join(t[i],(void**)&status[i]);
	}
	reset_latency_stat(&stat);
	for(i=0;i<insert_count*nthreads;i++){
		p_time_stamp = (time_stamp *)kv[i]->param.private_data;
		add_latency_stat(&stat, &p_time_stamp->start, &p_time_stamp->end);
	}
	show_elapsed_time_cumulative(thread_start, thread_end, nthreads, "kv_sdk_multi_store", insert_count, value_size, &stat);

	//Clear all key_buffer 
	for(i=0;i<insert_count*nthreads;i++){
		memset(kv[i]->key.key,0,key_length);
		memset(kv[i]->value.value,0,value_size);
	}

	//Run Read Thread
	for(i=0;i<nthreads;i++){
		p[i].tid = i;
		ret = pthread_create(&t[i], NULL, sdk_read, &p[i]);
		if(ret != KV_SUCCESS){
			fprintf(stderr, " fail to create reader thread(%d)\n", i);
			return -EIO;
		}
	}

	for(i=0;i<nthreads;i++){
		pthread_join(t[i],(void**)&status[i]);
	}
	reset_latency_stat(&stat);
	for(i=0;i<insert_count*nthreads;i++){
		p_time_stamp = (time_stamp *)kv[i]->param.private_data;
		add_latency_stat(&stat, &p_time_stamp->start, &p_time_stamp->end);
	}
	show_elapsed_time_cumulative(thread_start, thread_end, nthreads, "kv_sdk_multi_retrieve", insert_count, value_size, &stat);

	if (check_miscompare) {
		fprintf(stderr, "Checking miscompare count....");
                unsigned int miscompare_cnt = 0;
                for (i = 0; i < insert_count * nthreads; i++) {
                        unsigned long long tmp_hash[2];
                        Hash128_2_P128(kv[i]->value.value, kv[i]->value.length, seed, tmp_hash);
                        if (is_miscompare(tmp_hash, hash_value[i])) {
                                miscompare_cnt++;
                        }
                }
                fprintf(stderr, "Done.\n");
                fprintf(stderr, "Miscompare count: %u\n\n", miscompare_cnt);
        }

        //Clear all key_buffer 
        for(i=0;i<insert_count*nthreads;i++){
                memset(kv[i]->key.key,0,key_length);
        }

	//Run Exist Thread
	if(sdk_opt.ssd_type == KV_TYPE_SSD) {
		//Run Exist Thread after delete
		g_exist_answer = KV_SUCCESS;
		for(i=0;i<nthreads;i++){
			p[i].tid = i;
			ret = pthread_create(&t[i], NULL, sdk_exist, &p[i]);
			if(ret != KV_SUCCESS){
				fprintf(stderr, " fail to create exist thread(%d)\n", i);
				return -EIO;
			}
		}

		for(i=0;i<nthreads;i++){
			pthread_join(t[i],(void**)&status[i]);
		}
		reset_latency_stat(&stat);
		for(i=0;i<insert_count*nthreads;i++){
			p_time_stamp = (time_stamp *)kv[i]->param.private_data;
			add_latency_stat(&stat, &p_time_stamp->start, &p_time_stamp->end);
		}
		show_elapsed_time_cumulative(thread_start, thread_end, nthreads, "kv_sdk_multi_exist", insert_count, value_size, &stat);
	}

	//Run Delete Thread
	for(i=0;i<nthreads;i++){
		p[i].tid = i;
		ret = pthread_create(&t[i], NULL, sdk_delete, &p[i]);
		if(ret != KV_SUCCESS){
			fprintf(stderr, " fail to create delete thread(%d)\n", i);
			return -EIO;
		}
	}

	for(i=0;i<nthreads;i++){
		pthread_join(t[i],(void**)&status[i]);
	}
	reset_latency_stat(&stat);
	for(i=0;i<insert_count*nthreads;i++){
		p_time_stamp = (time_stamp *)kv[i]->param.private_data;
		add_latency_stat(&stat, &p_time_stamp->start, &p_time_stamp->end);
	}
	show_elapsed_time_cumulative(thread_start, thread_end, nthreads, "kv_sdk_multi_delete", insert_count, value_size, &stat);

	//Run Exist Thread after delete
	if(sdk_opt.ssd_type == KV_TYPE_SSD) {
		g_exist_answer = KV_ERR_NOT_EXIST_KEY;
		for(i=0;i<nthreads;i++){
			g_exist_complete_count[i] = 0;
		}

		for(i=0;i<nthreads;i++){
			p[i].tid = i;
			ret = pthread_create(&t[i], NULL, sdk_exist, &p[i]);
			if(ret != KV_SUCCESS){
				fprintf(stderr, " fail to create exist thread(%d)\n", i);
				return -EIO;
			}
		}

		for(i=0;i<nthreads;i++){
			pthread_join(t[i],(void**)&status[i]);
		}
		reset_latency_stat(&stat);
		for(i=0;i<insert_count*nthreads;i++){
			p_time_stamp = (time_stamp *)kv[i]->param.private_data;
			add_latency_stat(&stat, &p_time_stamp->start, &p_time_stamp->end);
		}
		show_elapsed_time_cumulative(thread_start, thread_end, nthreads, "kv_sdk_multi_exist_after_delete", insert_count, value_size, &stat);
	}

	//Print device information after test
	for(i=0;i<sdk_opt.nr_ssd;i++){
		uint64_t handle, total_size, used_size;
		double waf;
		
		handle = sdk_opt.dev_handle[i];
	        total_size = kv_get_total_size(handle);
		
		fprintf(stderr, "[DEVICE #%d information after test]\n", i);
        	if (total_size != KV_ERR_INVALID_VALUE){
                	fprintf(stderr, "  -Total Size of the NVMe Device: %lld MB\n", (unsigned long long)total_size / MB);
	        }
	        used_size = kv_get_used_size(handle);
        	if (used_size != KV_ERR_INVALID_VALUE){
	                if(sdk_opt.ssd_type == KV_TYPE_SSD){
				printf("  -Used Size of the NVMe Device: %.2f %s \n", (float)used_size / 100, "%");
                        }
                        else{
                                printf("  -Used Size of the NVMe Device: %lld MB\n", (unsigned long long)used_size / MB);
                        }
	        }
	        waf = kv_get_waf(handle);
        	if (waf != KV_ERR_INVALID_VALUE) {
                	fprintf(stderr, "  -WAF After doing I/O: %f\n", waf / 10);
	        }
		fprintf(stderr, "\n");
        }

	//Finalized Memory
	gettimeofday(&start, NULL);
	fprintf(stderr,"Teardown Memory: ");
	for(i=0;i<insert_count*nthreads;i++){
		if(!(i%10000)){
			fprintf(stderr,"%d ",i);
		}
		if(kv[i]->key.key) free(kv[i]->key.key);
		if(kv[i]->value.value) free(kv[i]->value.value);
		if(kv[i]->param.private_data) free(kv[i]->param.private_data);
		if(kv[i]) free(kv[i]);
	}
	free(kv);
	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	show_elapsed_time(&start,&end,"Teardown Memory",insert_count, 0, NULL);
	free(hash_value);

	//Finalize SDK
	gettimeofday(&start, NULL);
	kv_sdk_finalize();
	gettimeofday(&end, NULL);
	show_elapsed_time(&start,&end,"kv_sdk_finalize",0, 0, NULL);

	//Finalize complete_count variable and mutex(for complete count)
        free(g_write_complete_count);
        free(g_read_complete_count);
        free(g_delete_complete_count);
        free(g_exist_complete_count);
        free(g_write_mutex);
        free(g_read_mutex);
        free(g_delete_mutex);
        free(g_exist_mutex);

	fprintf(stderr, "%s done\n",__FUNCTION__);

	return 0;
}


int main(void)
{
	kv_sdk_info();
	int ret = test();
	return ret;
}
