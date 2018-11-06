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

#define fail_unless(_c) do{     \
        if(!(_c)){              \
                fprintf(stderr, "fail at LINE: %d\n", __LINE__); \
                exit(1);	\
        }                       \
}while(0)

unsigned int g_store_complete_count = 0;
unsigned int g_retrieve_complete_count = 0;
unsigned int g_delete_complete_count = 0;
unsigned int g_iterate_read_complete_count = 0;

void async_iterate_read_cb(kv_iterate* it, unsigned int result, unsigned int status){
	if((status != KV_SUCCESS)&&(status != KV_ERR_ITERATE_READ_EOF)){
		fprintf(stderr, "[%s] error. result=%d status=%d\n", __FUNCTION__, result, status);
		exit(1);
	}
	if(!it){
		fprintf(stderr, "%s fails: kv_iterate is NULL\n", __FUNCTION__);
		exit(1);
	}
	struct time_stamp* stamp = (struct time_stamp*)it->kv.param.private_data;
	gettimeofday(&stamp->end, NULL);
	g_iterate_read_complete_count++;
	if(status == KV_ERR_ITERATE_READ_EOF){
		g_iterate_read_complete_count = INT32_MAX;
	}

	fprintf(stderr,"Iterate Read Result: it->kv.key.length=%d it->kv.value.length=%d status=%d\n", it->kv.key.length, it->kv.value.length, status);
}

void async_store_cb(kv_pair* kv, unsigned int result, unsigned int status){
        if(status != KV_SUCCESS){
                fprintf(stderr, "[%s] error. key=%s option=%d value.length=%d value.offset=%d status code=%d\n",
                        __FUNCTION__, (char*)kv->key.key, kv->param.io_option.delete_option, kv->value.length, kv->value.offset, status);
                exit(1);
        }
        struct time_stamp *stamp = (struct time_stamp*)kv->param.private_data;
        gettimeofday(&stamp->end, NULL);
        g_store_complete_count++;
}

void async_retrieve_cb(kv_pair* kv, unsigned int result, unsigned int status){
        if(status != KV_SUCCESS){
                fprintf(stderr, "[%s] error. key=%s option=%d value.length=%d value.offset=%d status code=%d\n",
                        __FUNCTION__, (char*)kv->key.key, kv->param.io_option.delete_option, kv->value.length, kv->value.offset, status);
                exit(1);
        }
        struct time_stamp *stamp = (struct time_stamp*)kv->param.private_data;
        gettimeofday(&stamp->end, NULL);
	g_retrieve_complete_count++;
}

void async_delete_cb(kv_pair *kv, unsigned int result, unsigned int status) {
        if(status != KV_SUCCESS){
                fprintf(stderr, "[%s] error. key=%s option=%d value.length=%d value.offset=%d status code=%d\n",
                        __FUNCTION__, (char*)kv->key.key, kv->param.io_option.delete_option, kv->value.length, kv->value.offset, status);
                exit(1);
        }
        struct time_stamp *stamp = (struct time_stamp*)kv->param.private_data;
        gettimeofday(&stamp->end, NULL);
        g_delete_complete_count++;
}


int sdk_iterate_async(void){
	fprintf(stderr,"%s start\n",__FUNCTION__);

	srand(time(NULL)); //for random prefix
	int key_length = 16;
	int value_size = 4096;
	int insert_count = 10 * 10000;
	int iterate_read_count = 20;
	int iterate_buffer_size = 32 * 1024; //32KB
	int key_prefix = rand() % 0xFFFF; //4Bytes

	int i;
	int ret;
	struct timeval start;
	struct timeval end;
	struct latency_stat stat;
	struct time_stamp* stamp;

	/*sdk init from seperate data stucture*/
	gettimeofday(&start, NULL);
	kv_sdk sdk_opt;
	memset(&sdk_opt,0, sizeof(kv_sdk));
	strcpy(sdk_opt.dev_id[0], "0000:02:00.0");
	sdk_opt.dd_options[0].core_mask = 0x1;
	sdk_opt.dd_options[0].sync_mask = 0x0;
	sdk_opt.dd_options[0].cq_thread_mask = 0x2;

	sdk_opt.ssd_type=KV_TYPE_SSD;
	ret = kv_sdk_init(KV_SDK_INIT_FROM_STR, &sdk_opt);
	fail_unless(ret == KV_SUCCESS);
	gettimeofday(&end, NULL);
	show_elapsed_time(&start,&end,"sdk_init",0, 0, NULL);

	int nr_handle = 0;
	uint64_t arr_handle[MAX_CPU_CORES];
	memset((char*)arr_handle, 0, sizeof(arr_handle));
	ret = kv_get_device_handles(&nr_handle, arr_handle);
	fail_unless(ret == KV_SUCCESS);
	uint64_t handle = arr_handle[0];
	
	fprintf(stderr, "%s value_size=%d insert_count=%d\n",__FUNCTION__,value_size,insert_count);

	//Prepare App Memory 
	fprintf(stderr,"Setup App: ");
	gettimeofday(&start, NULL);
        kv_pair** kv = (kv_pair**)malloc(sizeof(kv_pair*)*insert_count);
        fail_unless(kv != NULL);
        for(i=0;i<insert_count;i++){
                if(!(i%10000)){
                        fprintf(stderr,"%d ",i);
                }
                kv[i] = (kv_pair*)malloc(sizeof(kv_pair));
		fail_unless(kv[i] != NULL);

		kv[i]->keyspace_id = KV_KEYSPACE_IODATA;
                kv[i]->key.key = malloc(key_length);
		fail_unless(kv[i]->key.key != NULL);
		kv[i]->key.length = key_length;
                memset(kv[i]->key.key,0,key_length);

                kv[i]->value.value = malloc(value_size*2); //for retrieve appended kv pair
		fail_unless(kv[i]->value.value != NULL);
                kv[i]->value.length = value_size;
		kv[i]->value.offset = 0;
		memset(kv[i]->value.value,'a'+(i%26), value_size);

		kv[i]->param.private_data = malloc(sizeof(struct time_stamp));
		fail_unless(kv[i]->param.private_data != NULL);
		memset(kv[i]->param.private_data, 0, sizeof(struct time_stamp));
        }

	kv_iterate** it = (kv_iterate**)malloc(sizeof(kv_iterate*) * iterate_read_count);

        for(i=0;i<iterate_read_count;i++){
		it[i] = (kv_iterate*)malloc(sizeof(kv_iterate));
		it[i]->iterator = KV_INVALID_ITERATE_HANDLE;
		it[i]->kv.key.key = malloc(key_length);
		fail_unless(it[i]->kv.key.key);
		it[i]->kv.key.length = key_length;
		memset(it[i]->kv.key.key, 0, it[i]->kv.key.length);

		it[i]->kv.value.value = malloc(iterate_buffer_size);
		fail_unless(it[i]->kv.value.value);
		it[i]->kv.value.length = iterate_buffer_size;
		it[i]->kv.value.offset = 0;
		memset(it[i]->kv.value.value, 0, it[i]->kv.value.length);

		it[i]->kv.param.private_data = malloc(sizeof(struct time_stamp));
		fail_unless(it[i]->kv.param.private_data != NULL);
		memset(it[i]->kv.param.private_data, 0, sizeof(struct time_stamp));
	}
        gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
        show_elapsed_time(&start,&end,"Setup App",insert_count, 0, NULL);


	//SDK Store
	fprintf(stderr,"kv_store_async: ");
	for(i=0;i<insert_count;i++){
		memset(kv[i]->key.key, 0, key_length);
		memcpy(kv[i]->key.key + ((size_t)key_length > sizeof(int) ? (key_length - sizeof(i)) : 0), &i, MIN((size_t)key_length, sizeof(int)));
		kv[i]->key.length = key_length;
		kv[i]->param.io_option.store_option = KV_STORE_DEFAULT;
		kv[i]->param.async_cb = async_store_cb;
		memset(kv[i]->param.private_data, 0, sizeof(struct time_stamp));
		//fprintf(stderr,"WRITE k=%s v=%s\n",(char*)kv[i]->key.key,(char*)kv[i]->value.value);
	}

	gettimeofday(&start, NULL);
	for(i=0;i<insert_count;i++){
		if(!(i % 10000)){
			fprintf(stderr, "%d ", i);
		}
		stamp = kv[i]->param.private_data;
		gettimeofday(&stamp->start, NULL);
		fail_unless(kv_store_async(handle, kv[i]) == KV_SUCCESS);
	}
	
	while(g_store_complete_count < insert_count){
		usleep(1);
	}
	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	reset_latency_stat(&stat);
	for (i = 0; i < insert_count; i++) {
		stamp = (struct time_stamp*)kv[i]->param.private_data;
		add_latency_stat(&stat, &stamp->start, &stamp->end);
	}
	show_elapsed_time(&start,&end,"kv_store_async",insert_count, value_size, &stat);

	//SDK Retrieve
	fprintf(stderr,"kv_retrieve_async: ");
	for(i=0;i<insert_count;i++){
		memset(kv[i]->key.key, 0, key_length);
		memcpy(kv[i]->key.key + ((size_t)key_length > sizeof(int) ? (key_length - sizeof(i)) : 0), &i, MIN((size_t)key_length, sizeof(int)));
		kv[i]->key.length = key_length;
		kv[i]->param.io_option.retrieve_option = KV_RETRIEVE_DEFAULT;
		kv[i]->param.async_cb = async_retrieve_cb;
		memset(kv[i]->param.private_data, 0, sizeof(struct time_stamp));
		//fprintf(stderr,"READ k=%s v=%s\n",(char*)kv[i]->key.key,(char*)kv[i]->value.value);
	}

	gettimeofday(&start, NULL);
	for(i=0;i<insert_count;i++){
		if(!(i % 10000)){
			fprintf(stderr, "%d ", i);
		}
		stamp = (struct time_stamp*)kv[i]->param.private_data;
		gettimeofday(&stamp->start, NULL);
		fail_unless(kv_retrieve_async(handle, kv[i]) == KV_SUCCESS);
	}
	
	while(g_retrieve_complete_count < insert_count){
		usleep(1);
	}
	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	reset_latency_stat(&stat);
	for (i = 0; i < insert_count; i++) {
		stamp = (struct time_stamp*)kv[i]->param.private_data;
		add_latency_stat(&stat, &stamp->start, &stamp->end);
	}
	show_elapsed_time(&start,&end,"kv_retrieve_async",insert_count, value_size, &stat);


        //SDK Iterate
	//check if iterator is already opened, and close it if so.
        int nr_iterate_handle = KV_MAX_ITERATE_HANDLE;
        kv_iterate_handle_info info[KV_MAX_ITERATE_HANDLE];
	memset((char*)&info,0,sizeof(info));
        ret = kv_iterate_info(handle, info, nr_iterate_handle);
        if(ret == KV_SUCCESS){
                fprintf(stderr, "iterate_handle count=%d\n",nr_iterate_handle);
                for(i=0;i<nr_iterate_handle;i++){
                        fprintf(stderr, "Retrieve iterate_handle_info[%d] : info.handle_id=%d info.status=%d info.type=%d info.keyspace_id=%d info.prefix=%08x info.bitmask=%08x info.is_eof=%d\n",
                                i+1, info[i].handle_id, info[i].status, info[i].type, info[i].keyspace_id, info[i].prefix, info[i].bitmask, info[i].is_eof);
                        if(info[i].status == ITERATE_HANDLE_OPENED){
                                fprintf(stderr,"close iterate_handle : %d\n", info[i].handle_id);
                                kv_iterate_close(handle, info[i].handle_id);
                        }
                }
        }
	
        fprintf(stderr,"Iterate Open\n");
        uint32_t bitmask = 0xFFFFFFFF;
	uint32_t prefix = 0;
        memcpy(&prefix,kv[0]->key.key,4);
        uint32_t iterator = KV_INVALID_ITERATE_HANDLE;
        uint8_t keyspace_id = KV_KEYSPACE_IODATA;
        fprintf(stderr,"[%s] keyspace_id=%d bitmask=%x bit_pattern=%x\n",__FUNCTION__,keyspace_id, bitmask, prefix);

        gettimeofday(&start, NULL);
        iterator = kv_iterate_open(handle, keyspace_id, bitmask, prefix, KV_KEY_ITERATE);
        //iterator = kv_iterate_open(handle, keyspace_id, bitmask, prefix, KV_KEY_ITERATE_WITH_RETRIEVE);
        gettimeofday(&end, NULL);

	memset((char*)&info,0,sizeof(info));
        ret = kv_iterate_info(handle, info, nr_iterate_handle);
	if(ret == KV_SUCCESS){
		for(i=0;i<nr_iterate_handle;i++){
                        fprintf(stderr, "Retrieve iterate_handle_info[%d] : info.handle_id=%d info.status=%d info.type=%d info.keyspace_id=%d info.prefix=%08x info.bitmask=%08x info.is_eof=%d\n",
                                i+1, info[i].handle_id, info[i].status, info[i].type, info[i].keyspace_id, info[i].prefix, info[i].bitmask, info[i].is_eof);
		}
	}

        show_elapsed_time(&start,&end,"kv_iterate_open",1, 0, NULL);
        if(iterator != KV_INVALID_ITERATE_HANDLE && iterator != KV_ERR_ITERATE_ERROR){
                fprintf(stderr,"Iterate open success : iterator id=%d\n", iterator);
		for(i=0;i<iterate_read_count;i++){
                        it[i]->iterator = iterator;
                        it[i]->kv.param.io_option.iterate_read_option = KV_ITERATE_READ_DEFAULT;
                        it[i]->kv.param.async_cb = async_iterate_read_cb;
                        memset(it[i]->kv.param.private_data, 0, sizeof(struct time_stamp));
		}
                gettimeofday(&start, NULL);
		for(i=0;i<iterate_read_count;i++){
                	fprintf(stderr,"Iterate Read Request=%d\n",it[i]->kv.value.length);
                	stamp = (struct time_stamp*)it[i]->kv.param.private_data;
                	gettimeofday(&stamp->start, NULL);
                	ret = kv_iterate_read_async(handle, it[i]);

			//  in case EOF,
			if(g_iterate_read_complete_count > iterate_read_count){
				break;
			}
                }

		while(g_iterate_read_complete_count < iterate_read_count){
			usleep(1);
		}
                gettimeofday(&end, NULL);
		reset_latency_stat(&stat);
		for(i=0;i<iterate_read_count;i++){
			stamp = it[i]->kv.param.private_data;
			add_latency_stat(&stat, &stamp->start, &stamp->end);
		}
		show_elapsed_time(&start,&end,"kv_iterate_read_async", 0, 0, &stat);

                fprintf(stderr,"Iterate Close\n");
                gettimeofday(&start, NULL);
                ret = kv_iterate_close(handle, iterator);
                gettimeofday(&end, NULL);
                show_elapsed_time(&start,&end,"kv_iterate_close",1, 0, NULL);
                fprintf(stderr,"iterate Close Result: ret=%d\n", ret);
        }
        else{
                fprintf(stderr,"Iterate open failed : iterator id=%d\n", iterator);
        }

finalize:
	//SDK Delete
	fprintf(stderr,"kv_delete_async: ");
	for(i=0;i<insert_count;i++){
		memset(kv[i]->key.key, 0, key_length);
		memcpy(kv[i]->key.key + ((size_t)key_length > sizeof(int) ? (key_length - sizeof(i)) : 0), &i, MIN((size_t)key_length, sizeof(int)));
		kv[i]->key.length = key_length;
		kv[i]->param.io_option.delete_option = KV_DELETE_DEFAULT;
		kv[i]->param.async_cb = async_delete_cb;
		memset(kv[i]->param.private_data, 0, sizeof(struct time_stamp));
	}

	gettimeofday(&start, NULL);
	for(i=0;i<insert_count;i++){
		if(!(i % 10000)){
			fprintf(stderr, "%d ", i);
		}
		stamp = kv[i]->param.private_data;
		gettimeofday(&stamp->start, NULL);
		fail_unless(kv_delete_async(handle, kv[i]) == KV_SUCCESS);
	}

	while(g_delete_complete_count < insert_count){
		usleep(1);
	}
	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	reset_latency_stat(&stat);
	for(i=0;i<insert_count;i++){
		stamp = kv[i]->param.private_data;
		add_latency_stat(&stat, &stamp->start, &stamp->end);
	}
	show_elapsed_time(&start,&end,"kv_delete_async",insert_count, value_size, &stat);

	//Teardown Memory
	fprintf(stderr,"Teardown Memory: ");
	gettimeofday(&start, NULL);
	for(i=0;i<insert_count;i++){
		if(!(i%10000)){
			fprintf(stderr,"%d ",i);
		}
		if(kv[i]->key.key) free(kv[i]->key.key);
		if(kv[i]->value.value) free(kv[i]->value.value);
		if(kv[i]->param.private_data) free(kv[i]->param.private_data);
		if(kv[i]) free(kv[i]);
	}
	if(kv) free(kv);

	for(i=0;i<iterate_read_count;i++){
		if(it[i]->kv.key.key) free(it[i]->kv.key.key);
		if(it[i]->kv.value.value) free(it[i]->kv.value.value);
		if(it[i]->kv.param.private_data) free(it[i]->kv.param.private_data);
		if(it[i]) free(it[i]);
	}
	if(it) free(it);

	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	show_elapsed_time(&start,&end,"Teardown Memory",insert_count, 0, NULL);

	//Finalize SDK
	gettimeofday(&start, NULL);
	kv_sdk_finalize();
	gettimeofday(&end, NULL);
	show_elapsed_time(&start,&end,"kv_sdk_finalize",0, 0, NULL);
}

int main(void)
{
	kv_sdk_info();
	int ret = sdk_iterate_async();
	return ret;
}
