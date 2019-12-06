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

#include "kv_types.h"
#include "kvnvme.h"
#include "kvutil.h"

int ut() {
	printf("%s start\n",__FUNCTION__);

	int i;
	int ret = -EINVAL;

	//NOTE : check pci information first using lspci -v
	char *nvme_pci_dev = "0000:02:00.0";

	int ssd_type = KV_TYPE_SSD;
	int key_length = 16;
	int value_size = 4096;
	int insert_count = 1 * 2;
	double waf = .0;
	int qid = DEFAULT_IO_QUEUE_ID;

	kv_nvme_io_options options = {0};
	options.core_mask = 1; // Use CPU 0
	options.sync_mask = 1; // Use Sync I/O mode
	options.num_cq_threads = 1; // Use only one CQ Processing Thread
	options.cq_thread_mask = 2;// Use CPU 7
	options.mem_size_mb = 256;

	struct timeval start = {0};
	struct timeval end = {0};

	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(0, &cpuset); // CPU 0

	sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

	gettimeofday(&start, NULL);
	kv_env_init(options.mem_size_mb);
	ret = kv_nvme_init(nvme_pci_dev, &options, ssd_type);
	if(ret)
		return ret;

	uint64_t handle = kv_nvme_open(nvme_pci_dev);
	if(!handle)
		return ret;

	printf("handle = %ld\n",handle);
	gettimeofday(&end, NULL);
	show_elapsed_time(&start, &end, "nvme_init", 0, 0, NULL);

	uint64_t total_size = kv_nvme_get_total_size(handle);
	uint64_t used_size = kv_nvme_get_used_size(handle);
	printf("Total Size of the NVMe Device: %lld MB\n", (unsigned long long)total_size / MB);
	if(ssd_type == KV_TYPE_SSD){
		printf("Used Size of the NVMe Device: %.2f %s \n", (float)used_size / 100, "%");
	}
	else{
		printf("Used Size of the NVMe Device: %lld MB\n", (unsigned long long)used_size / MB);
	}


	//retrieve log
	char logbuf[512];
	int line=16;
	int log_id = 0xC0;
	ret = kv_nvme_get_log_page(handle, log_id, logbuf, sizeof(logbuf));
	if(ret == KV_SUCCESS){
		int i;
		char* p = logbuf;
		for(i=0;i<sizeof(logbuf);i++){
			if(!(i%line)){
				printf("%04x: ", i);
			}
			printf("%02x ", *(unsigned char*)(p+i));

			if(i>0 && !((i+1)%line)){
				printf("\n");
			}
		}
	}

        waf = (double)kv_nvme_get_waf(handle) / 10;
        printf("WAF Before doing I/O: %f\n", waf);
	
	kv_pair** kv = (kv_pair**)malloc(sizeof(kv_pair*) * insert_count);
	if(!kv){
		return -ENOMEM;
	}

	//Prepare App Memory 
	gettimeofday(&start, NULL);
	for(i = 0;i < insert_count; i++){
		if(!(i % 10000)){
			fprintf(stderr,"Setup App: %d\n",i);
		}

		kv[i] = (kv_pair*)malloc(sizeof(kv_pair));
		if(!kv[i]){
			for(int j = 0 ; j < i; j++){
                free(kv[j]);
                kv[j] = NULL;
            }
            free(kv);
            kv = NULL;
			return -ENOMEM;
		}

		kv[i]->keyspace_id = KV_KEYSPACE_IODATA;
		kv[i]->key.key = malloc(key_length + 1);
		if(!kv[i]->key.key){
			for(int j = 0 ; j <= i; j++){
                free(kv[j]);
                kv[j] = NULL;
            }
            free(kv);
            kv = NULL;
			return -ENOMEM;
		}
		kv[i]->key.length = key_length;
		sprintf(kv[i]->key.key, "mountain%08x", i);

		kv[i]->value.value = kv_zalloc(value_size);
		if(!kv[i]->value.value){
            for(int j = 0 ; j <= i ; j++){
                free(kv[j]);
                kv[j] = NULL;
            }
            free(kv);
            kv = NULL;
            return -ENOMEM;
		}
		kv[i]->value.length = value_size;
		kv[i]->value.offset = 0;
		memset(kv[i]->value.value, 'a'+(i % 26), value_size);
	}
	gettimeofday(&end, NULL);
	show_elapsed_time(&start, &end, "Setup App", insert_count, 0, NULL);


	//NVME Write
	gettimeofday(&start, NULL);
	for(i = 0; i < insert_count; i++){
		if(!(i % 10000)){
			fprintf(stderr, "kv_nvme_write: %d\n", i);
		}
		kv[i]->param.io_option.store_option = KV_STORE_DEFAULT;
		//Store
		if(kv_nvme_write(handle, qid, kv[i]))
			return -EINVAL;
	}
	gettimeofday(&end, NULL);
	show_elapsed_time(&start,&end, "kv_nvme_write", insert_count, value_size, NULL);

	//NVME Append
	// it is monitored that sending an append cmd make kv_ssd hanng which need to download fw again. so fix it not to send to avoid it . (2017/09/29)
	/*
	for(i = 0; i < insert_count; i++){
		memset(kv[i]->key.key,0,key_length);
	}

	gettimeofday(&start, NULL);
	for(i = 0; i < insert_count; i++){
		if(!(i % 10000)){
			fprintf(stderr, "kv_nvme_append: %d\n", i);
		}
		sprintf(kv[i]->key.key, "mountain%08x", i);
		kv[i]->param.io_option.append_option = KV_APPEND_DEFAULT;

		//Append
		if(kv_nvme_append(handle, qid, kv[i]))
			return -EINVAL;
	}
	gettimeofday(&end, NULL);
	show_elapsed_time(&start,&end, "kv_nvme_append", insert_count, value_size, NULL);
	*/

	//NVMe Read
	for(i = 0; i < insert_count; i++){
		memset(kv[i]->key.key,0,key_length);
		memset(kv[i]->value.value,0,value_size);
		sprintf(kv[i]->key.key, "mountain%08x", i);
	}

	gettimeofday(&start, NULL);
	for(i = 0; i < insert_count; i++){
		if(!(i % 10000)){
			fprintf(stderr, "kv_nvme_read: %d\n", i);
		}
		kv[i]->param.io_option.retrieve_option = KV_RETRIEVE_DEFAULT;
		if(kv_nvme_read(handle, qid, kv[i]))
			return -EINVAL;
	}
	gettimeofday(&end, NULL);
	show_elapsed_time(&start,&end, "kv_nvme_read", insert_count, value_size, NULL);

	//Exist Before Delete
	printf("Exist Before Delete\n");
	for(i = 0; i < insert_count; i++){
		memset(kv[i]->key.key,0,key_length);
		sprintf(kv[i]->key.key, "mountain%08x",i);
		kv[i]->param.io_option.exist_option = KV_EXIST_DEFAULT;
	}
	gettimeofday(&start, NULL);
	for(i = 0; i < insert_count; i++){
		if(!(i%10000)){
			fprintf(stderr,"kv_nvme_exist: %d\n",i);
		}
		if(kv_nvme_exist(handle, qid, kv[i]) != KV_SUCCESS)
			return -EINVAL;
	}
	gettimeofday(&end, NULL);
	show_elapsed_time(&start,&end,"kv_nvme_exist",insert_count, value_size, NULL);

	//NVMe Delete
	for(i = 0; i < insert_count; i++){
		memset(kv[i]->key.key,0,key_length);
		sprintf(kv[i]->key.key, "mountain%08x",i);
		kv[i]->param.io_option.delete_option = KV_DELETE_DEFAULT;
	}
	gettimeofday(&start, NULL);
	for(i = 0; i < insert_count; i++){
		if(!(i%10000)){
			fprintf(stderr,"kv_nvme_delete: %d\n",i);
		}
		if(kv_nvme_delete(handle, qid, kv[i]))
			return -EINVAL;
	}
	gettimeofday(&end, NULL);
	show_elapsed_time(&start,&end,"kv_nvme_delete",insert_count, value_size, NULL);

	//Exist After Delete
	printf("Exist After Delete\n");
	for(i = 0; i < insert_count; i++){
		memset(kv[i]->key.key,0,key_length);
		sprintf(kv[i]->key.key, "mountain%08x",i);
		kv[i]->param.io_option.exist_option = KV_EXIST_DEFAULT;
	}
	gettimeofday(&start, NULL);
	for(i = 0; i < insert_count; i++){
		if(!(i%10000)){
			fprintf(stderr,"kv_nvme_exist: %d\n",i);
		}
		if(kv_nvme_exist(handle, qid, kv[i]) != KV_ERR_NOT_EXIST_KEY)
			return -EINVAL;
	}
	gettimeofday(&end, NULL);
	show_elapsed_time(&start,&end,"kv_nvme_exist",insert_count, value_size, NULL);

	//Teardown Memory
	gettimeofday(&start, NULL);
	for(i = 0;i < insert_count; i++){
		if(!(i % 10000)){
			fprintf(stderr, "Teardown Memory: %d\n", i);
		}
		if(kv[i]->value.value) kv_free(kv[i]->value.value);
		if(kv[i]->key.key) free(kv[i]->key.key);
		if(kv[i]) free(kv[i]);
	}

	free(kv);
	gettimeofday(&end, NULL);
	show_elapsed_time(&start, &end, "Teardown Memory", insert_count, 0, NULL);

        waf = (double)kv_nvme_get_waf(handle) / 10;
        printf("WAF After doing I/O: %f\n", waf);

	//Init Cache
	gettimeofday(&start, NULL);
	ret = kv_nvme_close(handle);
	if(ret)
		return ret;

	ret = kv_nvme_finalize(nvme_pci_dev);
	if(ret)
		return ret;
	gettimeofday(&end, NULL);
	show_elapsed_time(&start, &end, "nvme_finalize", 0, 0, NULL);
	return 0;
}

int main(void) {
	int ret = -EINVAL;

	ret = ut();

	return ret;
}

