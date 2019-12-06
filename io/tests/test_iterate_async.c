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
#include <assert.h>

#include "kvutil.h"
#include "kv_types.h"
#include "kv_apis.h"
#include "kvnvme.h"

#define NUM_CORES_USED (4)
#define PREFIX_LENGTH (4)
#define BIT_MASK (0xFFFFFFFF)
#define KEY_FORMAT "%016d"
#define KEY_LENGTH (16)
#define VALUE_LENGTH (4*KB)
#define DISPLAY_INTERVAL (1)
#define DISPLAY_CNT(_i) do{\
	if (!(_i % DISPLAY_INTERVAL)){\
		fprintf(stderr, "%-5d\b\b\b\b\b", _i);\
		fflush(stderr);\
	}\
}while(0)
#define TEST_VALUE_LENGTH(l, idx) ((l) - ((idx) % 3 * KB))

typedef struct{
	uint64_t dev_handle;
	uint32_t prefix;
	int key_length;
	int value_length;
	int test_cnt;
	int iterate_type;
	int tid;
}test_thread_param; //for multi iterate read


typedef struct{
	uint32_t prefix;
	int key_length;
	int read_key_cnt;
	int read_complete_cnt;
	int eof_flag;
	int iterate_type;
	uint8_t* key_exist;
}iterate_param;

void generate_key(char* key, int key_length, uint32_t prefix, int idx){
	snprintf(key, key_length+1, KEY_FORMAT, idx);
	prefix = htobe32(prefix);
        memcpy(key, &prefix, PREFIX_LENGTH); //change low 4B to the prefix
}

int get_key_idx(char* origin_key){
        char key[13] = {0, };
        memcpy(key, origin_key + PREFIX_LENGTH, KEY_LENGTH - PREFIX_LENGTH);
        return strtol(key, NULL, 10);
}

void validate_retrieved_value_length(kv_pair* kv){
        int idx = get_key_idx((char*)kv->key.key);
        assert(kv->value.length == TEST_VALUE_LENGTH(VALUE_LENGTH, idx));
}

int validate_iterate_read(uint8_t* key_exist, kv_iterate* it, int key_length, uint32_t prefix, int type){
        char* cur_key;
        int idx = 0;
        int num_keys = 0;

        prefix = htobe32(prefix);
        if (type == KV_KEY_ITERATE) {
                char* key_buf = (char*)it->kv.value.value;
                int key_buf_length = (int)it->kv.value.length;
                if (!key_buf_length) {
                        goto exit;
                }

                memcpy(&num_keys, key_buf, KV_ITERATE_READ_BUFFER_OFFSET);

                key_buf += KV_ITERATE_READ_BUFFER_OFFSET;
                for(int i = 0; i < num_keys; i++){
                        int cur_key_length;
                        memcpy(&cur_key_length, key_buf, KV_ITERATE_READ_BUFFER_OFFSET);
                        assert(cur_key_length == key_length);

                        cur_key = key_buf + KV_ITERATE_READ_BUFFER_OFFSET;
                        assert(memcmp(cur_key, &prefix, PREFIX_LENGTH) == 0);
                        idx = get_key_idx(cur_key);
                        if (key_exist[idx]) {
                                printf("(key only it)duplicated key upcoming: prefix=0x%x, key=%u(%u)\n", prefix, idx, key_exist[idx]+1);
                                //assert(0);
                        }
                        key_exist[idx]++;

			key_buf += KV_ITERATE_READ_BUFFER_OFFSET + key_length;
                }
        }
exit:
	return num_keys;
}


void test_close_iterate_handle(uint64_t handle){
        int nr_iterate_handle = KV_MAX_ITERATE_HANDLE;
        kv_iterate_handle_info info[KV_MAX_ITERATE_HANDLE];
        int ret = kv_iterate_info(handle, info, nr_iterate_handle);
        if (ret == KV_SUCCESS) {
                for (int i = 0; i < nr_iterate_handle; i++) {
                        if (info[i].status == ITERATE_HANDLE_OPENED) {
                                fprintf(stderr, "close iterate_handle : %d\n", info[i].handle_id);
                                kv_iterate_close(handle, info[i].handle_id);
                        }
                }
        }
}

uint64_t test_init(char *bdf){
	int ret;
        int nr_dev_handle = 0;
        uint64_t arr_dev_handle[MAX_CPU_CORES];
	kv_sdk sdk_opt;
	memset(&sdk_opt,0, sizeof(kv_sdk));

	srand(time(NULL)); // to generate random prefix

	strcpy(sdk_opt.dev_id[0], bdf);
	//set all cores to async
        sdk_opt.dd_options[0].core_mask = (uint64_t)((1<<NUM_CORES_USED) - 1); //0xF
        sdk_opt.dd_options[0].sync_mask = 0x0;
        sdk_opt.dd_options[0].cq_thread_mask = (uint64_t)(1<<(NUM_CORES_USED));
	sdk_opt.dd_options[0].queue_depth = 128;

	ret = kv_sdk_init(KV_SDK_INIT_FROM_STR, &sdk_opt);
	assert(ret == KV_SUCCESS);

        memset((char*)arr_dev_handle, 0, sizeof(arr_dev_handle));
        ret = kv_get_device_handles(&nr_dev_handle, arr_dev_handle);
        assert(ret == KV_SUCCESS);

	test_close_iterate_handle(arr_dev_handle[0]); //close handle if there's alread opened

        return arr_dev_handle[0]; //return dev[0]
}


void test_repeat_get_iterate_info(uint64_t handle, int test_cnt){
	printf("◆ Test kv_iterate_info %d times...", test_cnt);

        uint32_t bitmask = BIT_MASK;
	uint32_t prefix;
	uint8_t keyspace_id = KV_KEYSPACE_IODATA;
	uint32_t iterator;
        int nr_iterate_handle = 1;
	int ret;

        kv_iterate_handle_info info;

        for(uint32_t i = 0; i < test_cnt; i++) {
                // validate kv_iterate_info() when a iterator opened
		int iterate_type = KV_KEY_ITERATE;
                prefix = rand() & BIT_MASK;
                iterator = kv_iterate_open(handle, keyspace_id, bitmask, prefix, iterate_type);
		assert(iterator != KV_INVALID_ITERATE_HANDLE);
                ret = kv_iterate_info(handle, &info, nr_iterate_handle);
                assert(ret  == KV_SUCCESS);
                assert(info.status == ITERATE_HANDLE_OPENED); // check opend status
                assert(info.type == iterate_type); // check iterator's type
                assert(info.prefix == prefix); // check iterator's prefix
                assert(info.bitmask == bitmask); // check iterator's bitmask

                // validate kv_iterate_info() when the given iterator closed
                ret = kv_iterate_close(handle, iterator); // check closed status
                assert(ret == KV_SUCCESS);
                ret = kv_iterate_info(handle, &info, nr_iterate_handle);
                assert(ret == KV_SUCCESS);
                assert(info.status == ITERATE_HANDLE_CLOSED);
        }

        printf("Done.\n\n");
}

void test_repeat_itearte_open_close(uint64_t handle, int test_cnt){
	printf("◆ Test repeating open and close %d times...", test_cnt);

        uint32_t bitmask = BIT_MASK;
        uint32_t prefix = 0x30303030; //"0000"
	uint8_t keyspace_id = KV_KEYSPACE_IODATA;
	uint32_t iterator;
	int ret;

        for(uint32_t i = 0; i < test_cnt; i++){
                prefix += i;
                iterator = kv_iterate_open(handle, keyspace_id, bitmask, prefix, KV_KEY_ITERATE);
                assert(iterator != KV_INVALID_ITERATE_HANDLE);
                ret = kv_iterate_close(handle, iterator);
                assert(ret == KV_SUCCESS);
        }

        printf("Done.\n\n");
}

void test_iterate_open_wrong_condition(uint64_t handle){
	printf("◆ Test wrong open and close...");

	uint32_t bitmask = BIT_MASK;
        uint32_t prefix = 0x30303030; //"0000"
	uint8_t keyspace_id = KV_KEYSPACE_IODATA;
	uint32_t iterator;

        iterator = kv_iterate_open(handle, keyspace_id, bitmask, prefix, KV_KEY_ITERATE); //valid
        assert(iterator != KV_INVALID_ITERATE_HANDLE);
        //duplicated open
        uint32_t invalid_iterator = kv_iterate_open(handle, keyspace_id, bitmask, prefix, KV_KEY_ITERATE);
        assert(invalid_iterator == KV_ERR_ITERATE_HANDLE_ALREADY_OPENED);
	//invalid iterate type; not working properly
	/*
	invalid_iterator = kv_iterate_open(handle, bitmask, prefix, 0xFF);
	assert(invalid_iterator == KV_ERR_DD_INVALID_PARAM);
	*/
	
        //close
        assert(kv_iterate_close(handle, iterator-1) != KV_SUCCESS);
        assert(kv_iterate_close(handle, iterator+1) != KV_SUCCESS);
        assert(kv_iterate_close(handle, iterator) == KV_SUCCESS);
	
        printf("Done.\n\n");
}

void async_store_cb(kv_pair* kv, unsigned int result, unsigned int status){
        assert(status == KV_SUCCESS);
        assert(kv != NULL);
	assert(kv->value.value != NULL);

	int* complete_cnt = (int*)kv->param.private_data;
	(*complete_cnt)++;
}

void async_retrieve_cb(kv_pair* kv, unsigned int result, unsigned int status){
        assert(status == KV_SUCCESS);
        assert(kv != NULL);
	assert(kv->value.value != NULL);

	/* check value length */
	validate_retrieved_value_length(kv);

	int* complete_cnt = (int*)kv->param.private_data;
	(*complete_cnt)++;
}

void async_delete_cb(kv_pair* kv, unsigned int result, unsigned int status){
        assert(status == KV_SUCCESS);
        assert(kv != NULL);

	int* complete_cnt = (int*)kv->param.private_data;
	(*complete_cnt)++;
}

void async_iterate_read_cb(kv_iterate* it, unsigned int result, unsigned int status){
        assert(it != NULL);
	iterate_param* p = (iterate_param*)it->kv.param.private_data;
	p->read_complete_cnt++;

        assert(status == KV_SUCCESS || status == KV_ERR_ITERATE_READ_EOF);

        // async callback completion sequence validation logic
        if(status == KV_ERR_ITERATE_READ_EOF) {
                if(!p->eof_flag) { // when first eof read, set eof_flag
                        p->eof_flag = 1;
                } else {
			if (it->kv.value.length != 0) { // when key read after first eof; err
				printf("    Keys are read after ITERATE_READ_EOF\n");
				assert(0);
			}
                }
        }

	if (it->kv.value.length) {
		p->read_key_cnt += validate_iterate_read(p->key_exist, it, p->key_length, p->prefix, p->iterate_type);
	}
        free(it->kv.key.key);
        free(it->kv.value.value);
        free(it);
}

void test_iterate_read_wrong_condition(uint64_t handle){
	printf("◆ Test iterate_read with invalid value length and option..");

        uint32_t bitmask = BIT_MASK;
        uint32_t prefix = 0x30303030; //"0000"
	uint8_t keyspace_id = KV_KEYSPACE_IODATA;
	uint32_t iterator;
	int key_length = KEY_LENGTH;
	int iterate_buffer_size = KV_ITERATE_READ_BUFFER_SIZE; //32KB
	kv_iterate* it;
	int ret;

        iterator = kv_iterate_open(handle, keyspace_id, bitmask, prefix, KV_KEY_ITERATE);
        assert(iterator != KV_INVALID_ITERATE_HANDLE);

        it = (kv_iterate*)malloc(sizeof(kv_iterate));
        assert(it != NULL);
        it->iterator = iterator;
        it->kv.key.key = malloc(key_length+1);
        assert(it->kv.key.key != NULL);
        memset(it->kv.key.key,0,key_length+1);
        it->kv.value.value = malloc(iterate_buffer_size);
        assert(it->kv.value.value != NULL);
        memset(it->kv.value.value,0,iterate_buffer_size);

        //invalid value length
        int arr_invalid_length[] = {1025, 16*1024, 32*1024-1, 32*1024+1, 64*1024};
        for(int i=0; i<sizeof(arr_invalid_length)/sizeof(arr_invalid_length[0]); i++){
                it->kv.value.length = arr_invalid_length[i];
                it->kv.value.offset = 0;
                it->kv.param.io_option.iterate_read_option = KV_ITERATE_READ_DEFAULT;
		it->kv.param.async_cb = async_iterate_read_cb;
		it->kv.param.private_data = NULL;
                ret = kv_iterate_read_async(handle, it);
                assert(ret == KV_ERR_INVALID_VALUE_SIZE);
        }

        //invalid option; not working properly
	/*
        it->kv.value.length = iterate_buffer_size;
        it->kv.value.offset = 0;
        it->kv.param.io_option.iterate_read_option = 0xFF; //invalid option
        ret = kv_iterate_read(handle, it);
        assert(ret != KV_SUCCESS && ret != KV_ERR_ITERATE_READ_EOF);
	*/

        ret = kv_iterate_close(handle, iterator);
        assert(ret == KV_SUCCESS);

        free(it->kv.key.key);
        free(it->kv.value.value);
        free(it);

        printf("Done.\n\n");
}

void test_iterate_open_max_handles(uint64_t handle){
	printf("◆ Test iterate open for MAX(%d) times...", KV_MAX_ITERATE_HANDLE);

        uint32_t bitmask = BIT_MASK;
        uint32_t prefix = 0x30303030; //"0000"
	uint8_t keyspace_id = KV_KEYSPACE_IODATA;
        uint32_t iterator[KV_MAX_ITERATE_HANDLE];
        int ret;

        test_close_iterate_handle(handle);

        for(uint32_t i = 0; i < KV_MAX_ITERATE_HANDLE; i++){
                prefix += i;
                iterator[i] = kv_iterate_open(handle, keyspace_id, bitmask, prefix, KV_KEY_ITERATE);
                assert(iterator != KV_INVALID_ITERATE_HANDLE);
        }

        uint32_t tmp_iterator = kv_iterate_open(handle, keyspace_id, bitmask, prefix+1, KV_KEY_ITERATE);
        assert(tmp_iterator ==  KV_ERR_ITERATE_NO_AVAILABLE_HANDLE);

        for(uint32_t i = 0; i < KV_MAX_ITERATE_HANDLE; i++){
                ret = kv_iterate_close(handle, iterator[i]);
                assert(ret == KV_SUCCESS);
        }

        printf("Done.\n\n");
}

void test_store_async(uint64_t handle, uint32_t prefix, kv_pair** kv, int key_length, int value_length, int test_cnt){
        fprintf(stderr, "  Store...");
	int test_value_length;
	int complete_cnt = 0;

        for(int i = 0; i < test_cnt; i++){
                DISPLAY_CNT(i);
                generate_key(kv[i]->key.key, key_length, prefix, i);
		test_value_length = TEST_VALUE_LENGTH(value_length, i);
                kv[i]->key.length = key_length;
                kv[i]->value.length = test_value_length;
                kv[i]->value.offset = 0;
                memset(kv[i]->value.value,'a'+(i%26), test_value_length);
                kv[i]->param.io_option.store_option = KV_STORE_DEFAULT;

		kv[i]->param.async_cb = async_store_cb;
		kv[i]->param.private_data = &complete_cnt;
                assert(kv_store_async(handle, kv[i]) == KV_SUCCESS);
        }

	while(complete_cnt < test_cnt){
		usleep(1);
	}
        fprintf(stderr,"Done.\n");
}

void test_retrieve_async(uint64_t handle, uint32_t prefix, kv_pair** kv, int key_length, int value_length, int test_cnt){
        fprintf(stderr, "  Retrieve...");
	int complete_cnt = 0;

        for(int i = 0; i < test_cnt; i++){
                DISPLAY_CNT(i);
                generate_key(kv[i]->key.key, key_length, prefix, i);
                kv[i]->key.length = key_length;
                kv[i]->value.length = value_length;
                kv[i]->value.offset = 0;
                kv[i]->param.io_option.retrieve_option = KV_RETRIEVE_DEFAULT;

		kv[i]->param.async_cb = async_retrieve_cb;
		kv[i]->param.private_data = &complete_cnt;
                assert(kv_retrieve_async(handle, kv[i]) == KV_SUCCESS);
        }
	
	while(complete_cnt < test_cnt){
		usleep(1);
	}
        fprintf(stderr,"Done.\n");
}

void test_delete_async(uint64_t handle, uint32_t prefix, kv_pair** kv, int key_length, int test_cnt){
        fprintf(stderr,"  Delete...");
	int complete_cnt = 0;

        for(int i = 0; i < test_cnt; i++){
                DISPLAY_CNT(i);
                generate_key(kv[i]->key.key, key_length, prefix, i);
                kv[i]->key.length = key_length;
                kv[i]->param.io_option.delete_option = KV_DELETE_DEFAULT;

		kv[i]->param.async_cb = async_delete_cb;
		kv[i]->param.private_data = &complete_cnt;
                assert(kv_delete_async(handle, kv[i]) ==  KV_SUCCESS);
        }

	while(complete_cnt < test_cnt){
		usleep(1);
	}
        fprintf(stderr,"Done.\n");
}

void test_iterate_read_async(uint64_t handle, uint32_t prefix, int key_length, int value_length, int test_cnt, int type){
	fprintf(stderr, "  Iterator Read for prefix 0x%x(%s)...", prefix, (type==KV_KEY_ITERATE?"KEY ONLY IT":"KEY VALUE IT"));

	int ret, iterate_submit_cnt, iterate_read_key_cnt = 0;
        uint32_t bitmask = BIT_MASK;
	uint8_t keyspace_id = KV_KEYSPACE_IODATA;
        uint32_t iterator;
        int iterate_buffer_size;
        kv_iterate* it;
	uint8_t* key_exist;

	iterate_param p;
	p.prefix = prefix;
	p.iterate_type = type;
	p.read_key_cnt = 0;
	p.read_complete_cnt = 0;
	p.eof_flag = 0;
	p.key_length = key_length;
        p.iterate_type = type;
        p.key_exist = (uint8_t*)calloc(test_cnt, sizeof(uint8_t));
	assert(p.key_exist != NULL);

        iterator = kv_iterate_open(handle, keyspace_id, bitmask, prefix, type);
        assert(iterator != KV_INVALID_ITERATE_HANDLE && iterator <= KV_MAX_ITERATE_HANDLE);
	iterate_buffer_size = KV_ITERATE_READ_BUFFER_SIZE; //32KB

	iterate_submit_cnt = 0;
        do{
		it = (kv_iterate*)malloc(sizeof(kv_iterate));
	        assert(it != NULL);

	        it->kv.key.key = malloc(key_length+1);
		assert(it->kv.key.key != NULL);
		memset(it->kv.key.key, 0, key_length+1);
	        it->kv.value.value = malloc(iterate_buffer_size);
		assert(it->kv.value.value != NULL);

		it->iterator = iterator;
		it->kv.key.length = 0;
                it->kv.value.length = iterate_buffer_size;
                it->kv.value.offset = 0;
                it->kv.param.io_option.iterate_read_option = KV_ITERATE_READ_DEFAULT;
		it->kv.param.async_cb = async_iterate_read_cb;
		it->kv.param.private_data = &p;

                assert(kv_iterate_read_async(handle, it) == KV_SUCCESS);
	
		iterate_submit_cnt++;	
        }while(!p.eof_flag);

	while(p.read_complete_cnt < iterate_submit_cnt){
		usleep(1);
	}
	assert(p.read_key_cnt == test_cnt);

	fprintf(stderr, "submit %d times, read %u keys from %u keys. ", iterate_submit_cnt, p.read_key_cnt, test_cnt);

        assert(kv_iterate_close(handle, iterator) == KV_SUCCESS);

	free(p.key_exist);

	fprintf(stderr, "Done.\n");
}

kv_pair** test_alloc_kv_pair(int key_length, int value_length, int test_cnt){
        kv_pair** kv = (kv_pair**)malloc(sizeof(kv_pair*)*test_cnt);
        assert(kv != NULL);
        for(int i = 0; i < test_cnt; i++){
                DISPLAY_CNT(i);
                kv[i] = (kv_pair*)malloc(sizeof(kv_pair));
                assert(kv[i] != NULL);

		kv[i]->keyspace_id = KV_KEYSPACE_IODATA;
                kv[i]->key.key = calloc(key_length+1, 1);
                assert(kv[i]->key.key != NULL);

                kv[i]->value.value = malloc(value_length);
                assert(kv[i]->value.value != NULL);
        }
	return kv;
}

void test_free_kv_pair(kv_pair** kv, int test_cnt){
        for(int i = 0; i < test_cnt; i++){
		DISPLAY_CNT(i);
                if (kv[i]->key.key) free(kv[i]->key.key);
                if (kv[i]->value.value) free(kv[i]->value.value);
                if (kv[i]) free(kv[i]);
        }
        if (kv) free(kv);
}

void test_basic_io(uint64_t dev_handle, int test_cnt){
	printf("◆ Test basic IO with random prefix...");

	uint32_t prefix = rand() % BIT_MASK;
	int key_length = KEY_LENGTH;
	int value_length = VALUE_LENGTH;

        kv_pair** kv = test_alloc_kv_pair(key_length, value_length, test_cnt);

	test_store_async(dev_handle, prefix, kv, key_length, value_length, test_cnt);
	test_retrieve_async(dev_handle, prefix, kv, key_length, value_length, test_cnt);
	test_iterate_read_async(dev_handle, prefix, key_length, value_length, test_cnt, KV_KEY_ITERATE);
	test_delete_async(dev_handle, prefix, kv, key_length, test_cnt);

	test_free_kv_pair(kv, test_cnt);

	printf("Done.\n\n");
}

void* iterate_read_thread(void* data){
	test_thread_param* param = (test_thread_param*)data;

	uint64_t handle = param->dev_handle;
	uint32_t prefix = param->prefix;
	int key_length = param->key_length;
	int value_length = param->value_length;
	int test_cnt = param->test_cnt;
	int type = param->iterate_type;

	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(param->tid % NUM_CORES_USED, &cpuset);
	sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

	test_iterate_read_async(handle, prefix, key_length, value_length, test_cnt, type); 
}

void test_multi_iterate(uint64_t dev_handle, int test_cnt){
	printf("◆ Test multi iterate handles read(%d threads)...", KV_MAX_ITERATE_HANDLE);

        uint32_t prefix = rand() % (BIT_MASK - KV_MAX_ITERATE_HANDLE + 1);
        int key_length = KEY_LENGTH;
        int value_length = VALUE_LENGTH;

        pthread_t t[KV_MAX_ITERATE_HANDLE];
        test_thread_param p[KV_MAX_ITERATE_HANDLE];
	int status[KV_MAX_ITERATE_HANDLE];
	kv_pair** kv[KV_MAX_ITERATE_HANDLE];

	for(int i = 0; i < KV_MAX_ITERATE_HANDLE; i++){
		kv[i] = test_alloc_kv_pair(key_length, value_length, test_cnt);
		test_store_async(dev_handle, prefix+i, kv[i], key_length, value_length, test_cnt);
	}

	for(int i = 0; i < KV_MAX_ITERATE_HANDLE; i++){
		p[i].dev_handle = dev_handle;
		p[i].prefix = prefix+i;
		p[i].key_length = key_length;
		p[i].value_length = value_length;
		p[i].test_cnt = test_cnt;
		p[i].iterate_type = KV_KEY_ITERATE;
		p[i].tid = i;

		assert(pthread_create(&t[i], NULL, iterate_read_thread, &p[i]) >= 0);
	}

	for(int i = 0; i < KV_MAX_ITERATE_HANDLE; i++){
		pthread_join(t[i], (void**)&status[i]);
	}

	for(int i = 0; i < KV_MAX_ITERATE_HANDLE; i++){
		test_delete_async(dev_handle, prefix+i, kv[i], key_length, test_cnt);
		test_free_kv_pair(kv[i], test_cnt);
	}
	
	printf("Done.\n\n");
}

void test_finalize(){
	printf("◆ Test finalize...");
	kv_sdk_finalize();
	printf("Done.\n\n");
}

int test(){
	uint64_t dev_handle = test_init("0000:02:00.0");

	test_repeat_get_iterate_info(dev_handle, 10);

	test_repeat_itearte_open_close(dev_handle, 10);

	test_iterate_open_wrong_condition(dev_handle);

	test_iterate_read_wrong_condition(dev_handle);

	test_iterate_open_max_handles(dev_handle);

	test_basic_io(dev_handle, 1 * 10000); //store retrieve iterate_read delete

	//test_multi_iterate(dev_handle, 1 * 1000); //store multi-iterate_read delete

	test_finalize();

	return KV_SUCCESS;
}

int main(void)
{
	kv_sdk_info();
	return test();
}
