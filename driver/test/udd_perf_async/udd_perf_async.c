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

#include "latency_stat.h"
#include "kvutil.h"

#define SECTOR_SIZE (512)

unsigned int miscompare_cnt = 0;

unsigned int write_submit_cnt = 0;
unsigned int read_submit_cnt = 0;
unsigned int delete_submit_cnt = 0;

unsigned int write_complete_cnt = 0;
unsigned int read_complete_cnt = 0;
unsigned int delete_complete_cnt = 0;


void udd_write_cb(kv_pair *kv, unsigned int result, unsigned int status) {
	if(status != KV_SUCCESS){
		fprintf(stderr, "[%s] error. key=%s option=%d value.length=%d value.actual_value_size=%d value.offset=%d status code=%d\n",
			__FUNCTION__, (char*)kv->key.key, kv->param.io_option.store_option, kv->value.length, kv->value.actual_value_size, kv->value.offset, status);
		//exit(1);
	}
	write_complete_cnt++;
	struct time_stamp *stamp = kv->param.private_data;
	gettimeofday(&stamp->end, NULL);
}

void udd_read_cb(kv_pair *kv, unsigned int result, unsigned int status) {
	if(status != KV_SUCCESS){
		fprintf(stderr, "[%s] error. key=%s option=%d value.length=%d value.actual_value_size=%d value.offset=%d status code=%d\n",
			__FUNCTION__, (char*)kv->key.key, kv->param.io_option.retrieve_option, kv->value.length, kv->value.actual_value_size, kv->value.offset, status);
		//exit(1);
	}
	read_complete_cnt++;
	struct time_stamp *stamp = kv->param.private_data;
	gettimeofday(&stamp->end, NULL);
}

void udd_delete_cb(kv_pair *kv, unsigned int result, unsigned int status) {
	if(status != KV_SUCCESS){
		fprintf(stderr, "[%s] error. key=%s option=%d value.length=%d value.offset=%d status code=%d\n",
			__FUNCTION__, (char*)kv->key.key, kv->param.io_option.delete_option, kv->value.length, kv->value.offset, status);
		//exit(1);
	}
	delete_complete_cnt++;
	struct time_stamp *stamp = kv->param.private_data;
	gettimeofday(&stamp->end, NULL);
}

int udd_perf_async() {
	fprintf(stderr, "%s start\n",__FUNCTION__);

	int i;
	int ret = -EINVAL;
	//NOTE : check pci information first using lspci -v 
	char *nvme_pci_dev = "0000:02:00.0";

	int value_size = 4096;	//NOTE: value_size: 0~2048KB
	int key_length = 16;	//NOTE: key_length: 4~255B
	int qid = DEFAULT_IO_QUEUE_ID;
	unsigned int ssd_type = KV_TYPE_SSD;
	int insert_count = 10 * 10000;
	int host_hash = 0;/* 0:NONE, 1: Hash128_1_P128, 2: Hash128_2_P128, 3: MurmurHash3_x64_128 */
	int check_miscompare = 1;
	unsigned long long seed = 0x0102030405060708LL;
	struct latency_stat stat;

	kv_nvme_io_options options = {0};
	options.core_mask = 1; // Use CPU 0
	options.sync_mask = 0; // Use Async I/O mode
	options.num_cq_threads = 1; // Use only one CQ Processing Thread
	options.cq_thread_mask = 2; // Use CPU 7
	options.queue_depth = 256; // MAX QD
	options.mem_size_mb = 1024; // MAX -1

	struct timeval start = {0};
	struct timeval end = {0};

	if(host_hash == 1){
		check_miscompare = 1;
	}

	gettimeofday(&start, NULL);
	kv_env_init(options.mem_size_mb);

	ret = kv_nvme_init(nvme_pci_dev, &options, ssd_type);
	if(ret){
		fprintf(stderr,"kv_nvme_init ret=%d\n",ret);
		return ret;
	}

	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(0, &cpuset); // CPU 0
	sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

	uint64_t handle = kv_nvme_open(nvme_pci_dev);
	if(!handle){
		fprintf(stderr,"kv_nvme_open handle=%ld\n",handle);
		return ret;
	}
	gettimeofday(&end, NULL);
	show_elapsed_time(&start, &end, "nvme_init", 0, 0, NULL);

	uint64_t total_size = kv_nvme_get_total_size(handle);
	if(total_size != KV_ERR_INVALID_VALUE){
		fprintf(stderr, "Total Size of the NVMe Device: %lld MB\n", (unsigned long long)total_size / MB);
	}

	uint64_t used_size = kv_nvme_get_used_size(handle);
	if(used_size != KV_ERR_INVALID_VALUE){
		if(ssd_type == KV_TYPE_SSD){
			fprintf(stderr, "Used Size of the NVMe Device: %.2f %s \n", (float)used_size / 100, "%");
		}
		else{
			fprintf(stderr, "Used Size of the NVMe Device: %lld MB\n", (unsigned long long)used_size / MB);
		}
	}

	double waf = kv_nvme_get_waf(handle) / 10;
	if(waf != KV_ERR_INVALID_VALUE){
		fprintf(stderr, "WAF Before doing I/O: %f\n", waf);
	}

	kv_pair** kv = (kv_pair**)malloc(sizeof(kv_pair*) * insert_count);
	unsigned long long (*hash_value)[2] = (unsigned long long(*)[2])malloc(sizeof(unsigned long long [2]) * insert_count);
	struct time_stamp* stamps = (struct time_stamp *)malloc(sizeof(struct time_stamp) * insert_count);
	if(!kv || !hash_value || !stamps){
		if(kv) free(kv);
		if(hash_value) free(hash_value);
		if(stamps) free(stamps);
		fprintf(stderr,"fail to alloc variables\n");
		return -ENOMEM;
	}

	fprintf(stderr, "%s key_length = %d value_size = %d insert_count = %d\n", __FUNCTION__, key_length,  value_size, insert_count);

	int fd = open("/dev/srandom", O_RDONLY);
	if (fd < 3){
		fd = open("/dev/urandom", O_RDONLY);
	}
	//Prepare App Memory 
	fprintf(stderr,"Setup App: ");
	gettimeofday(&start, NULL);
	for(i = 0;i < insert_count; i++){
		if(!(i % 10000)){
			fprintf(stderr,"%d ",i);
		}

		//alloc pair
		kv[i] = (kv_pair*)malloc(sizeof(kv_pair));
		if(!kv[i]){
			fprintf(stderr,"fail to alloc kv pair\n");
			return -ENOMEM;
		}
		kv[i]->keyspace_id = KV_KEYSPACE_IODATA;
		if(value_size){
			int value_buffer_size = value_size;
			if(value_size%4){
				value_buffer_size += (4-value_size%4);
			}
			kv[i]->value.value = kv_zalloc(value_buffer_size);
			if(!kv[i]->value.value){
				fprintf(stderr,"fail to alloc value buffer\n");
				return -ENOMEM;
			}
			memset(kv[i]->value.value, 'a'+(i%26), value_size);
		}
		else{
			kv[i]->value.value = NULL;
		}
		kv[i]->value.length = value_size;
		kv[i]->value.actual_value_size = 0;
		kv[i]->value.offset = 0;

		if(check_miscompare){
			if(read(fd, kv[i]->value.value, value_size) != value_size){
				fprintf(stderr,"fail to read miscompare value\n");
				return -EIO;
			}
			Hash128_2_P128(kv[i]->value.value, value_size, seed, hash_value[i]);
		}
		//alloc key
		int key_buffer_size = key_length;
		if(key_length%4){
			key_buffer_size += (4-key_length%4);
		}
		kv[i]->key.key = kv_zalloc(key_buffer_size);
		if(!kv[i]->key.key){
			fprintf(stderr,"fail to alloc key buffer\n");
			return -ENOMEM;
		}

		kv[i]->key.length = key_length;
		if (host_hash == 0) {
                       if(ssd_type == LBA_TYPE_SSD) {
                               *(uint64_t*)(kv[i]->key.key) = i * (value_size / SECTOR_SIZE);
                       } else {
                               memcpy(kv[i]->key.key, &i, MIN((size_t)key_length, sizeof(i)));
                       }
		} else {
			memcpy(kv[i]->key.key, hash_value[i], key_length);
		}
		kv[i]->param.private_data = NULL;
	}
	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	show_elapsed_time(&start, &end, "Setup App", insert_count, 0, NULL);
	close(fd);

	//NVME Write
	fprintf(stderr, "kv_nvme_write_async: ");

	//prepare writing
	for (i = 0; i < insert_count; i++) {
		kv[i]->param.async_cb = udd_write_cb;
		kv[i]->param.private_data = &stamps[i];
		kv[i]->param.io_option.store_option = KV_STORE_DEFAULT;
	}

	gettimeofday(&start, NULL);
	for(i = 0; i < insert_count; i++){
		if(!(i % 10000)){
			fprintf(stderr, "%d ", i);
		}

		ret = -EINVAL;
		while(ret) {
			gettimeofday(&stamps[i].start, NULL);
			ret = kv_nvme_write_async(handle, qid, kv[i]);
			//show_key(&key, &h_key);
			if(ret) {
				usleep(1);
			} else {
				write_submit_cnt++;
				break;
			}
		}
	}

	while(write_complete_cnt < write_submit_cnt) {
		usleep(1);
	}

	gettimeofday(&end, NULL);
	fprintf(stderr, "Done\n");

	reset_latency_stat(&stat);
	for (i = 0; i < insert_count; i++) {
		add_latency_stat(&stat, &stamps[i].start, &stamps[i].end);
	}
	show_elapsed_time(&start,&end, "kv_nvme_write_async", insert_count, value_size, &stat);
	fprintf(stderr, "Received Write Submit Count: %d, Write Complete Count: %d\n", write_submit_cnt, write_complete_cnt);

	//Prepare reading
	for(i = 0; i < insert_count; i++){
		memset(kv[i]->value.value, 0, value_size);
		kv[i]->param.async_cb = udd_read_cb;
		kv[i]->param.private_data = &stamps[i];
		kv[i]->param.io_option.retrieve_option = KV_RETRIEVE_DEFAULT;
	}

	//NVMe Read
	fprintf(stderr, "kv_nvme_read_async: ");
	gettimeofday(&start, NULL);
	for(i = 0; i < insert_count; i++){
		if(!(i % 10000)){
			fprintf(stderr, "%d ", i);
		}

		//printf("key=%s\n",(char*)key[i]->key);

		ret = -EINVAL;
		while(ret) {
			gettimeofday(&stamps[i].start, NULL);
			ret = kv_nvme_read_async(handle, qid, kv[i]);
			//show_key(&key, &h_key);
			if(ret) {
				usleep(1);
			} else {
				read_submit_cnt++;
				break;
			}
		}
	}

	while(read_complete_cnt < read_submit_cnt) {
		usleep(1);
	}

	gettimeofday(&end, NULL);
	fprintf(stderr, "Done\n");

	reset_latency_stat(&stat);
	for (i = 0; i < insert_count; i++) {
		add_latency_stat(&stat, &stamps[i].start, &stamps[i].end);
	}
	show_elapsed_time(&start,&end, "kv_nvme_read_async", insert_count, value_size, &stat);
	printf("Received Read Submit Count: %d, Read Complete Count: %d\n", read_submit_cnt, read_complete_cnt);

	if(check_miscompare){
		for (i = 0; i < insert_count; i++) {
			unsigned long long tmp_hash[2];
			Hash128_2_P128(kv[i]->value.value, value_size, seed, tmp_hash);
			if (memcmp((void*)tmp_hash, (void*)hash_value[i],sizeof(tmp_hash)))
				miscompare_cnt++;
		}
		fprintf(stderr, "Miscompare count: %u\n", miscompare_cnt);
	}

	//Prepare deleting
	for(i = 0; i < insert_count; i++){
		memset(kv[i]->value.value, 0, value_size);
		kv[i]->param.async_cb = udd_delete_cb;
		kv[i]->param.private_data = &stamps[i];
		kv[i]->param.io_option.delete_option = KV_DELETE_DEFAULT;
	}

	//NVMe Delete
	fprintf(stderr,"kv_nvme_delete_async: ");
	gettimeofday(&start, NULL);
	for(i=0;i<insert_count;i++){
		if(!(i%10000)){
			fprintf(stderr,"%d ",i);
		}

		ret = -EINVAL;
		while(ret) {
			gettimeofday(&stamps[i].start, NULL);
			ret = kv_nvme_delete_async(handle, qid, kv[i]);
			if(ret) {
				usleep(1);
			} else {
				delete_submit_cnt++;
				break;
			}
		}
		//printf("value=%s\n",(char*)value[i]->value);
	}
	while(delete_complete_cnt < delete_submit_cnt) {
		usleep(1);
	}

	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");

	reset_latency_stat(&stat);
	for (i = 0; i < insert_count; i++) {
		add_latency_stat(&stat, &stamps[i].start, &stamps[i].end);
	}
	show_elapsed_time(&start,&end,"kv_nvme_delete_async", insert_count, value_size, &stat);
	fprintf(stderr, "Received Delete Submit Count: %d, Delete Complete Count: %d\n", delete_submit_cnt, delete_complete_cnt);

	//Teardown Memory
	fprintf(stderr, "Teardown Memory: ");
	gettimeofday(&start, NULL);
	for(i = 0;i < insert_count; i++){
		if(!(i % 10000)){
			fprintf(stderr, "%d ", i);
		}
		if(kv[i]->value.value) kv_free(kv[i]->value.value);
		if(kv[i]->key.key) kv_free(kv[i]->key.key);
		if(kv[i]) free(kv[i]);
	}
	free(kv);
	free(hash_value);
	free(stamps);
	gettimeofday(&end, NULL);
	fprintf(stderr, "Done\n");
	show_elapsed_time(&start, &end, "Teardown Memory", insert_count, 0, NULL);

	waf = kv_nvme_get_waf(handle) / 10;
	if(waf != KV_ERR_INVALID_VALUE){
		printf("WAF After doing I/O: %f\n", waf);
	}

	//Init Cache
	gettimeofday(&start, NULL);
	ret = kv_nvme_close(handle);
	if(ret){
		fprintf(stderr,"kv_nvme_close ret=%d\n", ret);
	}

	ret = kv_nvme_finalize(nvme_pci_dev);
	if(ret){
		fprintf(stderr,"kv_nvme_finalize ret=%d\n", ret);
	}
	gettimeofday(&end, NULL);
	show_elapsed_time(&start, &end, "nvme_finalize", 0, 0, NULL);

	return 0;
}

int main(void) {
	int ret = -EINVAL;
	kv_nvme_sdk_info();
	ret = udd_perf_async();

	return ret;
}

