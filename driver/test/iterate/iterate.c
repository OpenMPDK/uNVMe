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

int iterate() {
	fprintf(stderr, "%s start\n",__FUNCTION__);

	int i;
	int ret = -EINVAL;

	//NOTE : check pci information first using lspci -v
	char *nvme_pci_dev = "0000:02:00.0";

	int ssd_type = KV_TYPE_SSD;
	int key_length = 16;
	int value_size = 4096;
	int iterate_read_size = 32 * 1024;
	int iterate_read_count = 10;
	int insert_count = 1000;
	int qid = DEFAULT_IO_QUEUE_ID;
	double waf = .0;

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

	fprintf(stderr, "handle = %ld\n",handle);
	gettimeofday(&end, NULL);
	show_elapsed_time(&start, &end, "nvme_init", 0, 0, NULL);

	uint64_t total_size = kv_nvme_get_total_size(handle);
	uint64_t used_size = kv_nvme_get_used_size(handle);
	fprintf(stderr, "Total Size of the NVMe Device: %lld MB\n", (unsigned long long)total_size / MB);
	if(ssd_type == KV_TYPE_SSD){
		fprintf(stderr, "Used Size of the NVMe Device: %.2f %s \n", (float)used_size / 100, "%");
	}
	else{
		fprintf(stderr, "Used Size of the NVMe Device: %lld MB\n", (unsigned long long)used_size / MB);
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
				fprintf(stderr, "%04x: ", i);
			}
			fprintf(stderr, "%02x ", *(unsigned char*)(p+i));

			if(i>0 && !((i+1)%line)){
				fprintf(stderr, "\n");
			}
		}
	}

	waf = (double)kv_nvme_get_waf(handle) / 10;
	fprintf(stderr, "WAF Before doing I/O: %f\n", waf);

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
			for(int j = 0 ; j < i ; j++){
                free(kv[j]);
                kv[j] = NULL;
            }
            free(kv);
            kv = NULL;
			return -ENOMEM;
		}

		kv[i]->keyspace_id = KV_KEYSPACE_IODATA;
		int key_buffer_size = key_length;
		if(key_length%4){
			key_buffer_size += (4-key_length%4);
		}
		kv[i]->key.key = kv_zalloc(key_buffer_size);
		if(!kv[i]->key.key){
			for(int j = 0 ; j <= i ; j++){
                free(kv[j]);
                kv[j] = NULL;
            }
            free(kv);
            kv = NULL;
            return -ENOMEM;
		}
		memcpy(kv[i]->key.key + ((size_t)key_length > sizeof(int) ? (key_length - sizeof(i)) : 0), &i, MIN((size_t)key_length, sizeof(int)));
		kv[i]->key.length = key_length;

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

	kv_iterate* it = malloc(sizeof(kv_iterate));
	if(!it){
		return -ENOMEM;	
	}
	it->kv.key.key=NULL;
	it->kv.key.length=0;
	it->kv.value.value = kv_zalloc(iterate_read_size);
	if(!it->kv.value.value){
		kv_free(it);
		return -ENOMEM;
	}
	memset(it->kv.value.value,0,iterate_read_size);	
	
	gettimeofday(&end, NULL);
	show_elapsed_time(&start, &end, "Setup App", insert_count, 0, NULL);

	//NVME Write
	for(i = 0; i < insert_count; i++){
		kv[i]->param.io_option.store_option = KV_STORE_DEFAULT;
	}

	gettimeofday(&start, NULL);
	for(i = 0; i < insert_count; i++){
		if(!(i % 10000)){
			fprintf(stderr, "kv_nvme_write: %d\n", i);
		}
		if(kv_nvme_write(handle, qid, kv[i]))
			return -EINVAL;
	}
	gettimeofday(&end, NULL);
	show_elapsed_time(&start,&end, "kv_nvme_write", insert_count, value_size, NULL);

	for(i = 0; i < insert_count; i++){
		memset(kv[i]->key.key,0,key_length);
		memset(kv[i]->value.value,0,value_size);
	}

	//NVMe Read
	for(i = 0; i < insert_count; i++){
		kv[i]->param.io_option.store_option = KV_RETRIEVE_DEFAULT;
	}

	gettimeofday(&start, NULL);
	for(i = 0; i < insert_count; i++){
		if(!(i % 10000)){
			fprintf(stderr, "kv_nvme_read: %d\n", i);
		}
		if(kv_nvme_read(handle, qid, kv[i]))
			return -EINVAL;
	}
	gettimeofday(&end, NULL);
	show_elapsed_time(&start,&end, "kv_nvme_read", insert_count, value_size, NULL);


	for(i = 0; i < insert_count; i++){
		memset(kv[i]->value.value,0,value_size);
	}

	//check if iterator is already opened, and close it if so.
	int nr_iterate_handle = KV_MAX_ITERATE_HANDLE;
	kv_iterate_handle_info info[KV_MAX_ITERATE_HANDLE];
	ret = kv_nvme_iterate_info(handle, info, nr_iterate_handle);
	if(ret == KV_SUCCESS){
		fprintf(stderr, "iterate_handle count=%d\n",nr_iterate_handle);
		for(i=0;i<nr_iterate_handle;i++){
			fprintf(stderr, "iterate_handle_info[%d] : info.handle_id=%d info.status=%d info.type=%d info.prefix=%08x info.bitmask=%08x info.is_eof=%d\n",
				i+1, info[i].handle_id, info[i].status, info[i].type, info[i].prefix, info[i].bitmask, info[i].is_eof);
			if(info[i].status == ITERATE_HANDLE_OPENED){
				fprintf(stderr, "close iterate_handle : %d\n", info[i].handle_id);
				kv_nvme_iterate_close(handle, info[i].handle_id);
			}
		}
	}

	//TODO : option
	//Iterate
	fprintf(stderr, "Iterate Open\n");
	uint32_t bitmask = 0xFFFF0000;
	//uint32_t prefix = 0x1234;
	uint32_t prefix;
	memcpy(&prefix,kv[0]->key.key,2);
	uint32_t iterator = KV_INVALID_ITERATE_HANDLE;
	uint8_t keyspace_id = KV_KEYSPACE_IODATA;
	fprintf(stderr, "[%s] keyspace_id=%d bitmask=%x bit_pattern=%x\n",__FUNCTION__,keyspace_id, bitmask, prefix);
	gettimeofday(&start, NULL);
	iterator = kv_nvme_iterate_open(handle, keyspace_id, bitmask, prefix, KV_KEY_ITERATE);
	//iterator = kv_nvme_iterate_open(handle, keyspace_id, bitmask, prefix, KV_KEY_ITERATE_WITH_RETRIEVE);  // Note: general kv ssd no longer supports KV_KEY_ITERATE_WITH_RETRIEVE
	gettimeofday(&end, NULL);
	show_elapsed_time(&start,&end,"kv_iterate_open",1, 0, NULL);
	if(iterator != KV_INVALID_ITERATE_HANDLE && iterator != KV_ERR_ITERATE_ERROR){
		fprintf(stderr, "Iterate open success : iterator id=%d\n", iterator);
		fprintf(stderr, "Iterate Read\n");

		gettimeofday(&start, NULL);
		while(iterate_read_count--){
			it->iterator = iterator;
			it->kv.param.io_option.iterate_read_option = KV_ITERATE_READ_DEFAULT;
			it->kv.value.length = iterate_read_size;
			it->kv.value.offset = 0;
			memset(it->kv.value.value, 0, it->kv.value.length);
			ret = kv_nvme_iterate_read(handle, qid, it);
			fprintf(stderr, "Iterate Read Result: it->value.length=%d ret=%d\n", it->kv.value.length, ret);
			fprintf(stderr, "Iterate Read Result: it->value.value=%s\n", (char*)it->kv.value.value);
		}
		gettimeofday(&end, NULL);
		show_elapsed_time(&start,&end,"kv_iterate_read",1, 0, NULL);

		fprintf(stderr, "Iterate Close\n");
		gettimeofday(&start, NULL);
		ret = kv_nvme_iterate_close(handle, iterator);
		gettimeofday(&end, NULL);
		show_elapsed_time(&start,&end,"kv_iterate_close",1, 0, NULL);
		fprintf(stderr, "iterate Close Result: ret=%d\n", ret);
	}
	else{
		fprintf(stderr, "Iterate open failed : iterator =%d\n", iterator);
	}

	//NVMe Delete
	for(i = 0; i < insert_count; i++){
		kv[i]->param.io_option.store_option = KV_DELETE_DEFAULT;
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

	//Teardown Memory
	gettimeofday(&start, NULL);
	if(it->kv.value.value) kv_free(it->kv.value.value);
	if(it) free(it);

	
	for(i = 0;i < insert_count; i++){
		if(!(i % 10000)){
			fprintf(stderr, "Teardown Memory: %d\n", i);
		}
		if(kv[i]->value.value) kv_free(kv[i]->value.value);
		if(kv[i]->key.key) kv_free(kv[i]->key.key);
		if(kv[i]) free(kv[i]);
	}

	if(kv) free(kv);
	
	gettimeofday(&end, NULL);
	show_elapsed_time(&start, &end, "Teardown Memory", insert_count, 0, NULL);

	waf = (double)kv_nvme_get_waf(handle) / 10;
	fprintf(stderr, "WAF After doing I/O: %f\n", waf);

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

int main() {
	int ret = -EINVAL;
	kv_nvme_sdk_info();
	ret = iterate();

	return ret;
}

