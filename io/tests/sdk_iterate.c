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

int sdk_iterate(void){
	fprintf(stderr,"%s start\n",__FUNCTION__);

	srand(time(NULL)); //for random prefix
	int key_length = 16;
	int value_size = 4096;
	int insert_count = 20;
	int iterate_buffer_size = 32 * 1024; //32KB
	int key_prefix = rand() % 0xFFFF; //4Bytes

	int i;
	int ret;
	struct timeval start;
	struct timeval end;

	/*sdk init from seperate data stucture*/
	gettimeofday(&start, NULL);
	kv_sdk sdk_opt;
	memset(&sdk_opt,0, sizeof(kv_sdk));
	strcpy(sdk_opt.dev_id[0], "0000:02:00.0");
	sdk_opt.dd_options[0].core_mask = 0x1;
	sdk_opt.dd_options[0].sync_mask = 0x1;
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

                kv[i]->value.value = malloc(value_size);
		fail_unless(kv[i]->value.value != NULL);
                kv[i]->value.length = value_size;
		kv[i]->value.offset = 0;
		memset(kv[i]->value.value,'a'+(i%26), value_size);
        }

	kv_iterate* it = (kv_iterate*)malloc(sizeof(kv_iterate));
	fail_unless(it != NULL);
	it->iterator = KV_INVALID_ITERATE_HANDLE;
	it->kv.key.key = malloc(key_length);
	fail_unless(it->kv.key.key != NULL);
	it->kv.key.length = key_length;
	memset(it->kv.key.key, 0, it->kv.key.length);

	it->kv.value.value = malloc(iterate_buffer_size);
	fail_unless(it->kv.value.value != NULL);
	it->kv.value.length = iterate_buffer_size;
	it->kv.value.offset = 0;
	memset(it->kv.value.value, 0, it->kv.value.length);

        gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
        show_elapsed_time(&start,&end,"Setup App",insert_count, 0, NULL);

	//SDK Write
	fprintf(stderr,"kv_store: ");
	for(i=0;i<insert_count;i++){
		memset(kv[i]->key.key, 0, key_length);
		memcpy(kv[i]->key.key + ((size_t)key_length > sizeof(int) ? (key_length - sizeof(i)) : 0), &i, MIN((size_t)key_length, sizeof(int)));
		kv[i]->key.length = key_length;
		kv[i]->param.io_option.store_option = KV_STORE_DEFAULT;
		kv[i]->param.private_data = NULL;
		kv[i]->param.async_cb = NULL;
	}

	gettimeofday(&start, NULL);
	for(i=0;i<insert_count;i++){
		if(!(i%10000)){
			fprintf(stderr,"%d ", i);
		}
		fail_unless(kv_store(handle, kv[i]) == KV_SUCCESS);
		//fprintf(stderr,"WRITE k=%s v=%s\n",(char*)kv[i]->key.key,(char*)kv[i]->value.value);
	}
	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	show_elapsed_time(&start,&end,"kv_store",insert_count, value_size, NULL);

	//SDK Read
	fprintf(stderr,"kv_retrieve: ");
	for(i=0;i<insert_count;i++){
		memset(kv[i]->key.key, 0, key_length);
		memcpy(kv[i]->key.key + ((size_t)key_length > sizeof(int) ? (key_length - sizeof(i)) : 0), &i, MIN((size_t)key_length, sizeof(int)));
		kv[i]->key.length = key_length;
		kv[i]->param.io_option.retrieve_option = KV_RETRIEVE_DEFAULT;
		kv[i]->param.private_data = NULL;
		kv[i]->param.async_cb = NULL;
	}

	gettimeofday(&start, NULL);
	for(i=0;i<insert_count;i++){
		if(!(i%10000)){
			fprintf(stderr,"%d ",i);
		}
		fail_unless(kv_retrieve(handle, kv[i]) == KV_SUCCESS);
		//fprintf(stderr,"READ k=%s v=%s\n",(char*)kv[i]->key.key,(char*)kv[i]->value.value);
	}
	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	show_elapsed_time(&start,&end,"kv_retrieve",insert_count, value_size, NULL);

        //Iterate
	//check if iterator is already opened, and close it if so.
        int nr_iterate_handle = KV_MAX_ITERATE_HANDLE;
        kv_iterate_handle_info info[KV_MAX_ITERATE_HANDLE];
	memset((char*)&info,0,sizeof(info));
        ret = kv_iterate_info(handle, info, nr_iterate_handle);
        if(ret == KV_SUCCESS){
                fprintf(stderr,"iterate_handle count=%d\n",nr_iterate_handle);
                for(i=0;i<nr_iterate_handle;i++){
                        fprintf(stderr, "Retrieve iterate_handle_info[%d] : info.handle_id=%d info.status=%d info.type=%d info.keyspace_id=%d info.prefix=%08x info.bitmask=%08x info.is_eof=%d\n",
                                i+1, info[i].handle_id, info[i].status, info[i].type, info[i].keyspace_id, info[i].prefix, info[i].bitmask, info[i].is_eof);
                        if(info[i].status == ITERATE_HANDLE_OPENED){
                                fprintf(stderr, "close iterate_handle : %d\n", info[i].handle_id);
                                kv_iterate_close(handle, info[i].handle_id);
                        }
                }
        }
	
	fprintf(stderr,"Iterate Open ");
	uint32_t bitmask = 0xFFFFFFFF;
	uint32_t prefix = 0;
	memcpy(&prefix,kv[0]->key.key,4);
        uint32_t iterator = KV_INVALID_ITERATE_HANDLE;
	uint8_t keyspace_id = KV_KEYSPACE_IODATA;
	fprintf(stderr,"DONE\n");
	fprintf(stderr,"keyspace_id=%d bitmask=0x%x bit_pattern=0x%x\n",keyspace_id, bitmask, prefix);

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
		int num_keys_read = 0;
		int it_read_success_count = 0;
                gettimeofday(&start, NULL);
		do{
			int request_read_size = iterate_buffer_size;
			int cur_num_keys_read = 0;
                        it->iterator = iterator;
			it->kv.keyspace_id = 0;
                        it->kv.param.io_option.iterate_read_option = KV_ITERATE_READ_DEFAULT;
                        it->kv.value.length = request_read_size;
                        it->kv.value.offset = 0;
                        memset(it->kv.key.key, 0, key_length);
                        memset(it->kv.value.value, 0, it->kv.value.length);
			fprintf(stderr,"Iterate Read Request=%d\n",it->kv.value.length);
			ret = kv_iterate_read(handle, it);
                        fprintf(stderr,"Iterate Read Result: it->kv.key.length=%d it->kv.value.length=%d ret=0x%x\n", it->kv.key.length, it->kv.value.length, ret);

			//KV_KEY_ITERATE case
			if(it->kv.key.length == 0 && it->kv.value.length > 0){
				memcpy(&cur_num_keys_read, it->kv.value.value, KV_ITERATE_READ_BUFFER_OFFSET);
				num_keys_read += cur_num_keys_read;
			}

			if(ret == KV_SUCCESS){
				it_read_success_count++;
			}

                }while(ret == KV_SUCCESS);
		fail_unless(ret == KV_ERR_ITERATE_READ_EOF); // && num_keys_read == insert_count);
                gettimeofday(&end, NULL);
                show_elapsed_time(&start,&end,"kv_iterate_read",1, 0, NULL);
                fprintf(stderr,"it_read_success_count = %d\n", it_read_success_count);
		if(num_keys_read > 0){
			fprintf(stderr,"num_keys_read = %d\n", num_keys_read);
		}

                fprintf(stderr,"Iterate Close ");
                gettimeofday(&start, NULL);
                ret = kv_iterate_close(handle, iterator);
                gettimeofday(&end, NULL);
		fail_unless(ret == KV_SUCCESS);
		fprintf(stderr,"DONE\n");
                fprintf(stderr,"Iterate Close Result: ret=0x%x\n", ret);
                show_elapsed_time(&start,&end,"kv_iterate_close",1, 0, NULL);
        }
        else{
                fprintf(stderr,"Iterate open failed : iterator id=%d\n", iterator);
        }


finalize:
	//SDK Delete
	fprintf(stderr,"kv_delete: ");
	for(i=0;i<insert_count;i++){
		memset(kv[i]->key.key, 0, key_length);
		memcpy(kv[i]->key.key + ((size_t)key_length > sizeof(int) ? (key_length - sizeof(i)) : 0), &i, MIN((size_t)key_length, sizeof(int)));
		kv[i]->key.length = key_length;
		kv[i]->param.io_option.delete_option = KV_DELETE_DEFAULT;
		kv[i]->param.private_data = NULL;
		kv[i]->param.async_cb = NULL;
	}

	gettimeofday(&start, NULL);
	for(i=0;i<insert_count;i++){
		if(!(i%10000)){
			fprintf(stderr,"%d ",i);
		}
		fail_unless(0 == kv_delete(handle, kv[i]));
	}
	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	show_elapsed_time(&start,&end,"kv_delete",insert_count, value_size, NULL);

	//Teardown Memory
	fprintf(stderr,"Teardown Memory: ");
	gettimeofday(&start, NULL);
	for(i=0;i<insert_count;i++){
		if(!(i%10000)){
			fprintf(stderr,"%d ",i);
		}
		if(kv[i]->key.key) free(kv[i]->key.key);
		if(kv[i]->value.value) free(kv[i]->value.value);
		if(kv[i]) free(kv[i]);
	}
	if(kv) free(kv);

	if(it->kv.key.key) free(it->kv.key.key);
	if(it->kv.value.value) free(it->kv.value.value);
	if(it) free(it);

	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	show_elapsed_time(&start,&end,"Teardown Memory",insert_count, 0, NULL);

	//Finalize SDK
	gettimeofday(&start, NULL);
	kv_sdk_finalize();
	gettimeofday(&end, NULL);
	show_elapsed_time(&start,&end,"kv_sdk_finalize",0, 0, NULL);

	return 0;
}

int main(void)
{
	kv_sdk_info();
	int ret = sdk_iterate();
	return ret;
}

