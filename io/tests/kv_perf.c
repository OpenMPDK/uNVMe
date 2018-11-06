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
#include <sys/queue.h>

#include <sched.h>
#include <getopt.h>
#include <errno.h>

#include <kv_trace.h>

#include "latency_stat.h"

#include "kv_apis.h"
#include "kv_types.h"
#include "kvnvme.h"
#include "kvutil.h"

bool g_low_cmd_mode = true; //use Driver-level API (Default: true)

enum workload_types{
	SEQ_INC = 0,
	SEQ_DEC = 1,
	RAND = 2, // uniform random keys(can be duplicated) in given ranges(from offset to num_keys+offset)
	RAND_RANGE = 3, // generate random keys and shuffle. range of keys is random (ignore offset and num_keys)
	RAND_NOC = 4, // unique random keys(no collision) in given ranges(from offset to num_keys+offset)
};
#define	MIN_KEY_SIZE	0x400
#define	SECTOR_SIZE	    (512)
#define	MAX_KEY_RANGE_KV    (0xC0000000) // This supports till 3TB Device Size
#define	MAX_KEY_RANGE_LBA   (0x80000000) // 1TB/512B(sector_size)

#define DEFAULT_NUM_KEYS   1000
#define DEFAULT_KEY_SIZE   16
#define DEFAULT_VALUE_SIZE 4096
#define DEFAULT_JSON_CONFIG_PATH "./kv_perf_scripts/kv_perf_default_config.json"
#define MASK_SIZE   64

static kv_sdk sdk_opt;
int kv_sdk_load_option(kv_sdk *sdk_opt, char* log_path); //function which updates kv_sdk structure

enum op_type {
	NOP,
	WRITE,
	READ,
	DEL,
};
#define NUM_BLEND_OP 3

int is_miscompare(unsigned long long *v1, unsigned long long *v2) {
	return !((v1[0] == v2[0]) && (v1[1] == v2[1]));
}

struct kvpair {
	int op;
	int value_idx;
	kv_pair pair;
	unsigned long long hash_value[2];
	struct timeval start;
	struct timeval end;
	struct thread_priv_data *tpd;

	TAILQ_ENTRY(kvpair) tailq;
};

struct test_opt {
	// A number of keys to create
	unsigned int num_keys[NUM_BLEND_OP];
	// A number of tests
	int num_tests;

	// Perform read test
	int read;
	// Perform write test
	int write;
	// Perform verify
	int verify;
	// Perform delete
	int delete;
	// Perform format
	int format;
	// Perform blend
	int blend;

	// Value size
	unsigned int value_size[8];
	// A number of value
	int num_values;
	// max value size
	unsigned int max_value_size;
	// Key size
	unsigned int key_size;

	// SSD
	char *ssd;
	char mode;

	// Core Mask
	char *core_mask;

	// Core Mask
	char *sync_mask;

	// CQ Thread Mask
	char *cq_thread_mask;

	// 8 byte value string
	char *key_value;

	// File name to read the Key-Value pairs
	char *file_to_read;

	// Key offset, from which key will be incremented
	unsigned int key_offset[NUM_BLEND_OP];

	// Key range, from which key will be chosen
	struct _key_range{
		unsigned int key_start;
		unsigned int key_end;
	}key_range[NUM_BLEND_OP];

	// Key distribution
	int key_dist[NUM_BLEND_OP];

	// I/O Queue Depth
	unsigned int queue_depth;

	// seed value
	unsigned int seed[NR_MAX_SSD*MAX_CPU_CORES];
	// A number of seed
	int num_seeds;

	// json config file path
	char *json_path;
	// enable (read)cache
	int cache;
	// slab size
	size_t slab_size;
	// send get log
	int send_get_log;
} g_opt;

typedef struct thread_priv_data {
	unsigned int device_index;
	unsigned int thread_index;
	unsigned int cpu_id;

	unsigned int start_key[NUM_BLEND_OP];
	unsigned int num_keys[NUM_BLEND_OP];
	unsigned int *keys[NUM_BLEND_OP];
	unsigned int key_count[NUM_BLEND_OP];

	TAILQ_HEAD(, kvpair) kv_data;

	float read_iops;
	float write_iops;
	float delete_iops;
	float blend_iops;

	unsigned int submit_cnt;
	unsigned int cb_cnt;
	struct {
		unsigned long long cnt;
		unsigned long long value_idx_cnt[8];
	} op_stat[4];
	unsigned int miscompare_cnt;

	pthread_mutex_t mutex;
} thread_priv_data_t;

typedef struct dev_opt {
        // SSD
        char ssd[DEV_ID_LEN];

        // Core Mask
        char core_mask[MASK_SIZE];

        // Core Mask
        char sync_mask[MASK_SIZE];

        // CQ Thread Mask
        char cq_thread_mask[MASK_SIZE];

	// Q depth
	char queue_depth[MASK_SIZE];
} dev_opt_t;


unsigned int g_num_cpu_cores[NR_MAX_SSD], g_cpu_core_id[NR_MAX_SSD][MAX_CPU_CORES];
pthread_t threadid[NR_MAX_SSD][MAX_CPU_CORES];
thread_priv_data_t g_private_data[NR_MAX_SSD][MAX_CPU_CORES];

uint64_t handle[NR_MAX_SSD];

dev_opt_t device_opts[NR_MAX_SSD];
unsigned int g_num_devices = 0; //num of devices set from command options (not from config.json)

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
unsigned int submit_cnt[NR_MAX_SSD], cb_cnt[NR_MAX_SSD];


unsigned int uniform(unsigned int rangeLow, unsigned int rangeHigh, unsigned int *seed) {
        double myRand;
        if (seed)
                myRand = rand_r(seed)/(1.0 + RAND_MAX);
        else
                myRand = rand()/(1.0 + RAND_MAX);
        int range = rangeHigh - rangeLow + 1;
        int myRand_scaled = (myRand * range) + rangeLow;
        return myRand_scaled;
}

void kv_callback_fn(kv_pair *kv, unsigned int result, unsigned int status) {
	if (status != KV_SUCCESS){
		if (status != KV_ERR_NOT_EXIST_KEY){ //deleting unwritten key is allowed
			printf("%s fails: status(0x%x) != KV_SUCCESS\n", __FUNCTION__, status);
			exit(1);
		}
	}
	if (!kv){
		printf("%s fails: kv_pair is NULL\n", __FUNCTION__);
		exit(1);
	}
	struct kvpair* pair = (struct kvpair*)kv->param.private_data;
	gettimeofday(&pair->end, NULL);
	thread_priv_data_t *tpd = pair->tpd;

	pthread_mutex_lock(&tpd->mutex);
	tpd->cb_cnt++;
	pthread_mutex_unlock(&tpd->mutex);
}

#define KV_PERF_SYNC_IO(op, ret, handle, qid, kv) \
{\
	if (g_low_cmd_mode) {\
		ret = kv_nvme_##op(handle, qid, kv);\
	} else {\
		ret = kv_##op(handle, kv);\
	}\
}

#define KV_PERF_ASYNC_IO(op, ret, handle, qid, kv) \
{\
	if (g_low_cmd_mode) {\
		ret = kv_nvme_##op##_async(handle, qid, kv);\
	} else {\
		ret = kv_##op##_async(handle, kv);\
	}\
}

int (*kv_nvme_store)(uint64_t handle, int, kv_pair* kv);
int (*kv_nvme_store_async)(uint64_t handle, int, kv_pair* kv);
int (*kv_nvme_retrieve)(uint64_t handle, int, kv_pair* kv);
int (*kv_nvme_retrieve_async)(uint64_t handle, int, kv_pair* kv);

void set_io_function(bool low_cmd_mode){
	if (low_cmd_mode){
		kv_nvme_store = kv_nvme_write;
		kv_nvme_store_async = kv_nvme_write_async;
		kv_nvme_retrieve = kv_nvme_read;
		kv_nvme_retrieve_async = kv_nvme_read_async;
	}
}

void shuffle(unsigned int *keys, unsigned int num_keys)
{
	for(unsigned int i = num_keys-1; i > 0; i--) {
		unsigned int j = rand() % (i+1);
		unsigned long long tmp = keys[i];
		keys[i] = keys[j];
		keys[j] = tmp;
	}
}

void delete_test_data(unsigned int device, unsigned int thread_index)
{
	while(!TAILQ_EMPTY(&g_private_data[device][thread_index].kv_data))
	{
		struct kvpair *kv = TAILQ_FIRST(&g_private_data[device][thread_index].kv_data);
		TAILQ_REMOVE(&g_private_data[device][thread_index].kv_data, kv, tailq);

		if (g_low_cmd_mode) {
			kv_free(kv->pair.value.value);
			kv_free(kv->pair.key.key);
		} else {
			free(kv->pair.value.value);
			free(kv->pair.key.key);
		}
		free(kv);
	}

	for (int i = 0; i < NUM_BLEND_OP; i++) {
		free(g_private_data[device][thread_index].keys[i]);
	}

	return;
}

uint64_t create_test_key(unsigned int thread_index, unsigned int id){
	return (thread_index * 100000000UL) + id;
}

int create_test_data(unsigned int device, unsigned int thread_index, unsigned int seed)
{
	int ret = 0;
	int fd = -1;

	printf("Creating test data...");

	TAILQ_INIT(&g_private_data[device][thread_index].kv_data);

	/*
	 * To verify integrity of retrieving values, kv_perf does Murmur hash for values filled from /dev/srandom, and save the results before doing I/O tests.
	 * After retrieving value by given key, the values is compared with the previous hash result.
         */
	if (g_opt.verify) {
		fd = open("/dev/srandom", O_RDONLY);
		if (fd < 3){
			fd = open("/dev/urandom", O_RDONLY);
		}
	}

	int blend_idx = 0;
	int blend_idx_limit = 1;
	if (g_opt.blend) {
		blend_idx_limit = NUM_BLEND_OP; //3 (Write, Read, and Delete)
	}

	for (blend_idx = 0; blend_idx < blend_idx_limit; blend_idx++) {
		if (g_opt.key_dist[blend_idx] == RAND_RANGE) { //keys for shuffle
			g_private_data[device][thread_index].keys[blend_idx] = malloc(g_private_data[device][thread_index].num_keys[blend_idx]*sizeof(unsigned int));
			if (g_private_data[device][thread_index].keys[blend_idx] == NULL) {
				printf("Error: malloc() failed for a key array\n");
				close(fd);
				ret = -ENOMEM;
				goto err;
			}
		}
	}

	TAILQ_HEAD(, kvpair) temp_tailq[NUM_BLEND_OP];
	for (blend_idx = 0; blend_idx < blend_idx_limit; blend_idx++) {
		TAILQ_INIT(&temp_tailq[blend_idx]);
		if (g_opt.key_dist[blend_idx] == RAND_RANGE) {
			if (g_opt.key_range[blend_idx].key_end == 0) {
				if (sdk_opt.ssd_type == LBA_TYPE_SSD) {
					g_opt.key_range[blend_idx].key_end = (unsigned int)MAX_KEY_RANGE_LBA;
				} else {
					g_opt.key_range[blend_idx].key_end = (unsigned int)MAX_KEY_RANGE_KV;
				}
			}
			seed = time(NULL);
		}

		unsigned int count = g_private_data[device][thread_index].start_key[blend_idx]; //start_key is 0 + x, where x is set by -o(--offset)
		for(; count < (g_private_data[device][thread_index].start_key[blend_idx] + g_private_data[device][thread_index].num_keys[blend_idx]); count++)
		{
			struct kvpair *kv = NULL;
			kv = malloc(sizeof(struct kvpair));
			if (kv == NULL) {
				printf("Error: malloc() failed for an entry %d\n", count);
				delete_test_data(device, thread_index);
				ret = -ENOMEM;
				goto err;
			}
			kv->pair.keyspace_id = KV_KEYSPACE_IODATA;
			kv->pair.key.length = g_opt.key_size;
			if (g_low_cmd_mode ) {
				int key_buffer_size = kv->pair.key.length;
				if(kv->pair.key.length%4){
					key_buffer_size += (4-kv->pair.key.length%4);
				}
				kv->pair.key.key = kv_zalloc(key_buffer_size);
			} else {
				kv->pair.key.key = malloc(kv->pair.key.length);
			}
			if (kv->pair.key.key == NULL) {
				printf("Error: malloc() failed for a key %d\n", count);
				delete_test_data(device, thread_index);
				ret = -ENOMEM;
				goto err;
			}

			kv->pair.param.async_cb = kv_callback_fn;
			kv->tpd = &g_private_data[device][thread_index];
			kv->op = blend_idx + 1; //non, put, get, del

			unsigned long long value_idx = 0;
			if (g_opt.key_dist[blend_idx] == RAND) {
				unsigned int id = uniform(g_private_data[device][thread_index].start_key[blend_idx], g_private_data[device][thread_index].num_keys[blend_idx]-1, &g_opt.seed[thread_index]);
				if(sdk_opt.ssd_type == LBA_TYPE_SSD) {
					*(uint64_t*)(kv->pair.key.key) = id;
				} else {
					uint64_t _key = create_test_key(thread_index, id);
					memcpy(kv->pair.key.key, &_key, MIN((size_t)g_opt.key_size, sizeof(uint64_t)));
				}
				value_idx = id;
			} else if (g_opt.key_dist[blend_idx] == RAND_RANGE) {
				unsigned int tmp_key = (unsigned int)((rand_r(&seed)/(double)(1.0f + RAND_MAX)) * (g_opt.key_range[blend_idx].key_end - g_opt.key_range[blend_idx].key_start + 1.0f)) + g_opt.key_range[blend_idx].key_start;
				g_private_data[device][thread_index].keys[blend_idx][g_private_data[device][thread_index].key_count[blend_idx]] = tmp_key;
				g_private_data[device][thread_index].key_count[blend_idx]++;
				value_idx = tmp_key;
			} else {
				if(sdk_opt.ssd_type == LBA_TYPE_SSD) {
					*(uint64_t*)(kv->pair.key.key) = count;
				} else {
					uint64_t _key = create_test_key(thread_index, count);
					memcpy(kv->pair.key.key, &_key, MIN((size_t)g_opt.key_size, sizeof(uint64_t)));
				}
				value_idx = count;
			}

			if (g_opt.num_values != 1) {
				kv->pair.value.length = g_opt.value_size[value_idx % g_opt.num_values];
				kv->value_idx = value_idx % g_opt.num_values;
			} else {
				kv->pair.value.length = g_opt.value_size[0];
				kv->value_idx = 0;
			}

			if (sdk_opt.ssd_type == LBA_TYPE_SSD) {
				//change key to proper lba addr
				if (g_opt.key_dist[blend_idx] != RAND_RANGE) {
					uint64_t orig_key = *(uint64_t*)(kv->pair.key.key);
					*(uint64_t*)(kv->pair.key.key) = orig_key * (kv->pair.value.length / SECTOR_SIZE);
				}
			}

			if (g_low_cmd_mode) {
				int value_buffer_size = kv->pair.value.length;
				if(value_buffer_size%4){
					value_buffer_size += (4-kv->pair.value.length%4);
				}
				kv->pair.value.value = kv_zalloc(value_buffer_size);
			} else {
				kv->pair.value.value = malloc(kv->pair.value.length);
			}
			if (kv->pair.value.value == NULL) {
				printf("Error: malloc() failed for a value of the key %d\n", count);
				delete_test_data(device, thread_index);
				ret = -ENOMEM;
				goto err;
			}

			kv->pair.value.offset = 0;
			kv->pair.value.actual_value_size = 0;

			if (g_opt.verify) {
				if (read(fd, kv->pair.value.value, kv->pair.value.length) !=  kv->pair.value.length){
					ret = -EIO;
					goto err;
				}
				Hash128_2_P128(kv->pair.value.value, kv->pair.value.length, 0x12345678L, kv->hash_value); 
			} else {
				if(!g_opt.key_value) {
					memset(kv->pair.value.value, 'a' + (value_idx % 26), kv->pair.value.length);
				} else {
					unsigned int i, key_value_string_len = 0;
					char *value_buffer = kv->pair.value.value;
					key_value_string_len = strlen(g_opt.key_value);
					for(i = 0; i < kv->pair.value.length; i += key_value_string_len) {
						memcpy(value_buffer, g_opt.key_value, key_value_string_len);
						value_buffer += key_value_string_len;
					}
				}
			}

			if (g_opt.key_dist[blend_idx] == SEQ_DEC) {
				TAILQ_INSERT_HEAD(&temp_tailq[blend_idx], kv, tailq);
			}
			else {
				TAILQ_INSERT_TAIL(&temp_tailq[blend_idx], kv, tailq);
			}
		}

		if (g_opt.key_dist[blend_idx] == RAND_NOC) {
			/* for shuffle */
			int size = g_private_data[device][thread_index].num_keys[blend_idx];
			struct kvpair **arr = (struct kvpair **)malloc(size * sizeof(struct kvpair *));
			struct kvpair *kv;
			int i = 0;
			if (arr == NULL){
				ret = -ENOMEM;
				goto err;
			}
			TAILQ_FOREACH(kv, &temp_tailq[blend_idx], tailq) {
				arr[i] = kv;
				i++;
			}
			TAILQ_INIT(&temp_tailq[blend_idx]);
			for (i = 0; i < size - 1; i++) {
				int j;
				if (g_opt.seed[thread_index]) {
					j = i + rand_r(&g_opt.seed[thread_index]) / (RAND_MAX / (size - i) + 1);
				}
				else {
					j = i + rand() / (RAND_MAX / (size - i) + 1);
				}
				struct kvpair *tmp = arr[i];
				arr[i] = arr[j];
				arr[j] = tmp;
			}

			for (i = 0; i < size; i++) {
				TAILQ_INSERT_TAIL(&temp_tailq[blend_idx], arr[i], tailq);
			}
			free(arr);
		} else if (g_opt.key_dist[blend_idx] == RAND_RANGE) { // Shuffle randomly generated keys
			struct kvpair *kv;
			unsigned int cnt = 0;
			shuffle(g_private_data[device][thread_index].keys[blend_idx], g_private_data[device][thread_index].num_keys[blend_idx]);
			TAILQ_FOREACH(kv, &temp_tailq[blend_idx], tailq) {
				if (sdk_opt.ssd_type == LBA_TYPE_SSD) {
					*(uint64_t *)kv->pair.key.key = MAX(g_private_data[device][thread_index].keys[blend_idx][cnt] * (kv->pair.value.length / SECTOR_SIZE), MAX_KEY_RANGE_LBA);
				} else {
					uint64_t _key = create_test_key(thread_index, (unsigned int)g_private_data[device][thread_index].keys[blend_idx][cnt]);
					memcpy(kv->pair.key.key, &_key, MIN((size_t)g_opt.key_size, sizeof(uint64_t)));
				}
				cnt++;
			}
		}
		/* debug */
		//struct kvpair *kv;
		//TAILQ_FOREACH(kv, &temp_tailq[blend_idx], tailq) {
		//	printf("key=%16s\n", (char*)kv->pair.key.key);
		//}
	}

	if (g_opt.blend) {
		/* shuffle */
		int blend_idx = 0;
		int size = 0;
		for (blend_idx = 0; blend_idx < NUM_BLEND_OP; blend_idx++) {
			size += g_private_data[device][thread_index].num_keys[blend_idx];
		}
		struct kvpair **arr = (struct kvpair **)malloc(size * sizeof(struct kvpair *));
		struct kvpair *kv;
		int i = 0;
		for (blend_idx = 0; blend_idx < NUM_BLEND_OP; blend_idx++) {
			TAILQ_FOREACH(kv, &temp_tailq[blend_idx], tailq) {
				arr[i] = kv;
				i++;
			}
			TAILQ_INIT(&temp_tailq[blend_idx]);
		}

		for (i = 0; i < size - 1; i++) {
			int j;
			if (g_opt.seed[thread_index]) {
				j = i + rand_r(&g_opt.seed[thread_index]) / (RAND_MAX / (size - i) + 1);
			}
			else {
				j = i + rand() / (RAND_MAX / (size - i) + 1);
			}
			struct kvpair *tmp = arr[i];
			arr[i] = arr[j];
			arr[j] = tmp;
		}

		for (i = 0; i < size; i++) {
			TAILQ_INSERT_TAIL(&g_private_data[device][thread_index].kv_data, arr[i], tailq);
		}

		free(arr);
	} else {
		TAILQ_CONCAT(&g_private_data[device][thread_index].kv_data, &temp_tailq[0], tailq);
	}

/* for debug */
//	{
//		struct kvpair *kv;
//		TAILQ_FOREACH(kv, &g_private_data[thread_index].kv_data, tailq) {
//			printf("%d, %s\n", kv->op, (char*)kv->key.key);
//		}
//	}

err:
	if (ret == 0){
		printf("Done\n");
	}
	else{
		printf("%s Error. ret=%d\n",__FUNCTION__,ret);
	}
	close(fd);
	return ret;
}


void show_test_info(){
	printf("\n");
	printf("[KV Perf Test Information: %s]\n", g_opt.json_path);
	if (g_low_cmd_mode){
		printf("  ***RUN AS LOW CMD MODE***\n");
	}
        printf("  - test list (0: false, 1: true)\n");
        printf("      format: %d\n", g_opt.format);
        printf("      write_test: %d\n", g_opt.write);
        printf("      read_test: %d\n", g_opt.read);
        printf("      verify_read: %d\n", g_opt.verify);
        printf("      delete_test: %d\n", g_opt.delete);
        printf("      blend : %d\n", g_opt.blend);

	printf("  - key_size: %u (default: %u)\n", g_opt.key_size, DEFAULT_KEY_SIZE);
	printf("  - value_size (default: %u)\n", DEFAULT_VALUE_SIZE);
	for(int i=0; i<g_opt.num_values; i++){
		printf("      value_size[%d]: %u\n", i, g_opt.value_size[i]);
	}
	printf("      max_value_size: %u\n", g_opt.max_value_size);
	printf("  - num_keys");
	if (g_opt.blend){
		printf(" (default: %u)\n", DEFAULT_NUM_KEYS);
		printf("      num_keys[WRITE]: %u\n", g_opt.num_keys[0]);
		printf("      num_keys[READ]: %u\n", g_opt.num_keys[1]);
		printf("      num_keys[DEL]: %u\n", g_opt.num_keys[2]);
	} else {
		printf(": %u (default: %u)\n", g_opt.num_keys[0], DEFAULT_NUM_KEYS);
	}
	printf("  - num_tests: %d (default: %d)\n", g_opt.num_tests, 1);
	printf("  - key_offset");
	if (g_opt.blend){
		printf(" (default: %u)\n", 0);
		printf("      key_offset[WRITE]: %u\n", g_opt.key_offset[0]);
		printf("      key_offset[READ]: %u\n", g_opt.key_offset[1]);
		printf("      key_offset[DEL]: %u\n", g_opt.key_offset[2]);
	} else {
		printf(": %u (default: %u)\n", g_opt.key_offset[0], 0);
	}
	printf("  - key_dist");
	if (g_opt.blend){
		printf("\n");
		printf("      key_dist[WRITE]: %u\n", g_opt.key_dist[0]);
		printf("      key_dist[READ]: %u\n", g_opt.key_dist[1]);
		printf("      key_dist[DEL]: %u\n", g_opt.key_dist[2]);
	} else {
		printf(": %u (default: %u)\n", g_opt.key_dist[0], 0);
	}
	printf("  - seed (default: %u)\n", 0);
	if (g_opt.num_seeds){
		for(int i=0; i<g_opt.num_seeds; i++){
			printf("      seed[%d]: %u\n", i, g_opt.seed[i]);
		}
	} else {
		printf("      default\n");
	}
	printf(" use_cache: %u\n", g_opt.cache);
	printf(" send_get_log: %u\n", g_opt.send_get_log);

	printf("\n\n");
}

void set_cmd_opt_to_sdk(){
	if (g_opt.cache)
		sdk_opt.use_cache = true;
	if (g_opt.slab_size)
		sdk_opt.slab_size = g_opt.slab_size;


	for (unsigned int i = 0; i < g_num_devices; i++){
		if (g_opt.ssd)
			strcpy(sdk_opt.dev_id[i], device_opts[i].ssd);
		if (g_opt.core_mask){
			sdk_opt.dd_options[i].core_mask = (uint64_t)strtoull(device_opts[i].core_mask, NULL, 16);
			for(unsigned int j = 0; j < MAX_CPU_CORES; j++) {
				if(sdk_opt.dd_options[i].core_mask & (1ULL << j)) {
					g_cpu_core_id[i][g_num_cpu_cores[i]++] = j;
				}
			}
		}
		if (g_opt.sync_mask)
			sdk_opt.dd_options[i].sync_mask = (uint64_t)strtoull(device_opts[i].sync_mask, NULL, 16);
		if (g_opt.cq_thread_mask) {
			sdk_opt.dd_options[i].num_cq_threads = 0;
			sdk_opt.dd_options[i].cq_thread_mask = (uint64_t)strtoull(device_opts[i].cq_thread_mask, NULL, 16);
                        for(int j = 0; j < MAX_CPU_CORES; j++) {
                        	if(sdk_opt.dd_options[i].cq_thread_mask & (1ULL << j)) {
					sdk_opt.dd_options[i].num_cq_threads++;
                                }
			}
		}
		if (g_opt.queue_depth) 
			sdk_opt.dd_options[i].queue_depth = (uint32_t)atoi(device_opts[i].queue_depth);
	}

	/* 
	  overwrite cmd to json.
	  e.g. json: 2 devices. user: 1 device ==> test: 1 device only
	*/
	if (g_num_devices!=0) 
		sdk_opt.nr_ssd = g_num_devices;
}


void show_kv_perf_menu(){
	printf("\t--write | -w, Write test\n"
               "\t--read | -r, Read test\n"
               "\t--verify | -x, Verify test\n"
               "\t--delete | -e, Delete test\n"
               "\t--blend | -b, Blend Workload test\n"
               "\t--format | -z, Format device\n"
               "\t--num_keys | -n <Number of keys>, -n <w> <r> <d> for blend test, default: 1000\n"
               "\t--num_tests | -t <Number of tests>, default: 1\n"
               "\t--value_size | -v <value size in bytes>, You can specify up to 8 numbers from which test value size will be chosen randomly, default: 4096\n"
               "\t--workload | -p <key distribution>, -p <w> <r> <d> for blend test, default: 0\n"
	       "\t              0: Sequential increment\n"
	       "\t              1: Sequential decrement\n"
	       "\t              2: Uniform random keys in given range(from offset to offset+num_keys, can be duplicated)\n"
	       "\t              3: Random keys of which range is specified by --key_range(-y), default key range: 0x0 to 0xC0000000(offset will be ignored)\n"
	       "\t              4: Unique random keys in given range(from offset to offset+num_keys)\n"
               "\t--json_conf | -j <json config file path>, default: \"./kv_perf_scripts/kv_perf.default_config.json\"\n"
               "\t--send_get_log | -i, send get_log_page cmd during IO operation, default: off \n"
               "\t--slab_size | -l <slab size in MB>, default: 512MB\n"
               "\t--use_cache | -u, Read cache enable, default: off\n"
               "\t--device | -d <PCI addr of the device>, default: \"0000:02:00.0\"\n"
               "\t--ssd_type | -m <SSD Type>, 0: LBA SSD, 1: KV SSD, default: 1\n"
               "\t--core_mask | -c <CPU core mask>, CPU cores to execute the I/O threads(e.g.FF), default: 1\n"
               "\t--sync_mask | -s <Sync mask>, I/O threads to perform Sync operations(e.g.FF), default: 1\n"
               "\t--cq_mask | -q <CQ Thread Mask>, CQ Processing threads CPU Core Mask(e.g.FF), default: 2\n"
               "\t--qd | -a <IO Queue Depth>, default: 64\n"
               "\t--offset | -o <offset>, -o <w> <r> <d> for blend test, Start number from which key ID will be generated, default: 0\n"
               "\t--seed | -g <seed_value>, Seeds for generating random key (this option can be set with multiple values), default: 0\n"
               "\t--def_value | -k <key_value>, 8 bytes value string. This is filled upto the value size(e.g.DEADBEEF), default: NULL\n"
               "\t--read_file | -f <File Name>, Name of the file to read the Key Value(e.g.\"/home/guest/key_value.txt\"), default: NULL\n"
               "\t--key_range | -y <key_start-key_end>, -y <s-e> <s-e> <s-e> for blend test, workload type should be 3(-p 3), default: NULL\n"
               "\t--use_sdk_cmd | -L, Use SDK level IO cmds. If this options is not set, use low level IO cmds\n"
	       "\t--help | -h, Showing command menu\n");
}

int parse_opt(int argc, char **argv)
{
	int c;
	unsigned int num_devices = 0;

	// Default:
	g_opt.num_keys[0] = DEFAULT_NUM_KEYS;
	g_opt.num_keys[1] = 0;
	g_opt.num_keys[2] = 0;
	g_opt.num_tests = 1;
	g_opt.write = 0;
	g_opt.read = 0;
	g_opt.verify = 0;
	g_opt.delete = 0;
	g_opt.format = 0;
	g_opt.blend = 0;
	g_opt.value_size[0] = DEFAULT_VALUE_SIZE;
	g_opt.max_value_size = DEFAULT_VALUE_SIZE;
	g_opt.num_values = 1;
	g_opt.key_size = DEFAULT_KEY_SIZE;
	g_opt.key_value = NULL;
	g_opt.file_to_read = NULL;
	g_opt.key_offset[0] = 0;
	g_opt.key_offset[1] = 0;
	g_opt.key_offset[2] = 0;
	g_opt.key_dist[0] = SEQ_INC;
	g_opt.key_dist[1] = SEQ_INC;
	g_opt.key_dist[2] = SEQ_INC;
	g_opt.json_path = DEFAULT_JSON_CONFIG_PATH;

	for (int i = 0; i < MAX_CPU_CORES; i++)
		g_opt.seed[i] = 0;

	g_opt.ssd = NULL;
	g_opt.send_get_log = 0;
	g_opt.cache = 0;
	g_opt.slab_size = 0;
	g_opt.queue_depth = 0;
	g_opt.core_mask = NULL;
	g_opt.sync_mask = NULL;
	g_opt.cq_thread_mask = NULL;

	static struct option long_options[] = {
		{"num_keys",  required_argument, 0,  'n' },
		{"num_tests", required_argument, 0,  't' },
		{"device",    required_argument, 0,  'd' },
		{"ssd_type",  required_argument, 0,  'm' },
		{"value_size",required_argument, 0,  'v' },
		{"core_mask", required_argument, 0,  'c' },
		{"sync_mask", required_argument, 0,  's' },
		{"cq_mask",   required_argument, 0,  'q' },
		{"qd",        required_argument, 0,  'a' },
		{"def_value", required_argument, 0,  'k' },
		{"read_file", required_argument, 0,  'f' },
		{"key_range", required_argument, 0,  'y' },
		{"offset",    required_argument, 0,  'o' },
		{"workload",  required_argument, 0,  'p' },
		{"seed",      required_argument, 0,  'g' },
		{"json_conf", required_argument, 0,  'j' },
		{"slab_size", required_argument, 0,  'l' },
		{"blend",     no_argument,       0,  'b' },
		{"read",      no_argument,       0,  'r' },
		{"write",     no_argument,       0,  'w' },
		{"verify",    no_argument,       0,  'x' },
		{"delete",    no_argument,       0,  'e' },
		{"format",    no_argument,       0,  'z' },
		{"use_cache", no_argument,	 0,  'u' },
		{"send_get_log", no_argument,	 0,  'i' },
		{"use_sdk_cmd", no_argument,	 0,  'L' },
		{"help",      no_argument,       0,  'h' },
		{0,           0,                 0,  0   }
	};

	int long_index = 0;
	while ((c = getopt_long(argc, argv, "n:t:d:m:v:c:s:q:a:k:f:y:o:p:g:j:l:brwxezuhiL", long_options, &long_index)) != -1) {
		switch(c) {
		case 'n': //num_keys
			{
				int num = 0;
				int index = optind - 1;
				char *next = NULL;
				while (index < argc) {
					next = strdup(argv[index]);
					index++;
					if (next[0] != '-') {
						unsigned int val = atoi(next);
						g_opt.num_keys[num++] = val;
					}
					else break;
				}
				optind = index - 1;
				break;
			}
			break;
		case 't': //num_tests
			g_opt.num_tests = atoi(optarg);
			break;
		case 'r': //read test
			g_opt.read = 1;
			break;
		case 'w': //write test
			g_opt.write = 1;
			break;
		case 'd': //device(id)
			{
				g_opt.ssd = optarg;
				num_devices = 0;
				
				int index = optind - 1;
				char *next = NULL;
				while (index < argc) {
					next = strdup(argv[index]);
					index++;
					if (next[0] != '-'){
						sprintf(device_opts[num_devices++].ssd, "%s", next);
					}
					else break;
				}
				optind = index - 1;
				if (!g_num_devices){
					g_num_devices = num_devices;
				} else {
					if (num_devices != g_num_devices){
						printf("Please Check the configuration: num. of device_id: %d num. of other configuration: %d\n", num_devices, g_num_devices);
						return -EINVAL;
					}
				}
				/* for debug
				for(int i = 0; i< num_devices; i++){
					printf("Device %d Name: %s\n", i, device_opts[i].ssd);
				}
				*/
			}
			break;
		case 'm': //ssd_type
			g_opt.mode = atoi(optarg);
			break;
		case 'v': //value_size
			{
				g_opt.max_value_size = 0;
				g_opt.num_values = 0;
				int index = optind - 1;
				char *next = NULL;
				while (index < argc) {
					next = strdup(argv[index]);
					index++;
					if (next[0] != '-') {
						unsigned int val = atoi(next);
						g_opt.value_size[g_opt.num_values++] = val;
						if (g_opt.max_value_size < val)
							g_opt.max_value_size = val;
					}
					else break;
				}
				optind = index - 1;
				break;
			}
			break;
		case 'b': //blend test
			g_opt.blend = 1;
			break;
		case 'c': //core_mask
			{	
				g_opt.core_mask = optarg;
				num_devices = 0;

				int index = optind - 1;
				char *next = NULL;
				while (index < argc) {
					next = strdup(argv[index]);
					index++;
					if (next[0] != '-'){
						sprintf(device_opts[num_devices++].core_mask, "%s", next);
					}
					else break;
				}
				optind = index - 1;

                                if (!g_num_devices){
                                        g_num_devices = num_devices;
                                } else {
                                        if (num_devices != g_num_devices){
                                                printf("Please Check the configuration: num. of core_mask: %d num. of other configuration: %d\n", num_devices, g_num_devices);
						return -EINVAL;
                                        }
                                }
			}
			break;
		case 's': //sync_mask
			{
				g_opt.sync_mask = optarg;
                                num_devices = 0;

                                int index = optind - 1;
                                char *next = NULL;
                                while (index < argc) {
                                        next = strdup(argv[index]);
                                        index++;
                                        if (next[0] != '-'){
                                                sprintf(device_opts[num_devices++].sync_mask, "%s", next);
                                        }
                                        else break;
                                }
                                optind = index - 1;

                                if (!g_num_devices){
                                        g_num_devices = num_devices;
                                } else {
                                        if (num_devices != g_num_devices){
                                                printf("Please Check the configuration: num. of sync_mask: %d num. of other configuration: %d\n", num_devices, g_num_devices);
                                                return -EINVAL;
                                        }
                                }
			}
			break;
		case 'q': //cq_mask
			{
				g_opt.cq_thread_mask = optarg;
                                num_devices = 0;

                                int index = optind - 1;
                                char *next = NULL;
                                while (index < argc) {
                                        next = strdup(argv[index]);
                                        index++;
                                        if (next[0] != '-'){
                                                sprintf(device_opts[num_devices++].cq_thread_mask, "%s", next);
                                        }
                                        else break;
                                }
                                optind = index - 1;

                                if (!g_num_devices){
                                        g_num_devices = num_devices;
                                } else {
                                        if (num_devices != g_num_devices){
                                                printf("Please Check the configuration: num. of cq_mask: %d num. of other configuration: %d\n", num_devices, g_num_devices);
                                                return -EINVAL;
                                        }
				}
			}
			break;
		case 'a': //queue depth
			{
				g_opt.queue_depth = atoi(optarg);
                                num_devices = 0;

                                int index = optind - 1;
                                char *next = NULL;
                                while (index < argc) {
                                        next = strdup(argv[index]);
                                        index++;
                                        if (next[0] != '-'){
                                                sprintf(device_opts[num_devices++].queue_depth, "%s", next);
                                        }
                                        else break;
                                }
                                optind = index - 1;
				if (!g_num_devices) {
					g_num_devices = num_devices;
                                } else if (num_devices != g_num_devices) {
                                        printf("No. of Queue depth: %d for the Devices: %d Not Matching\n", num_devices, g_num_devices);
                                        return -EINVAL;
                                }
			}
			break;
		case 'k': //def_value: 8B value
			g_opt.key_value = optarg;
			break;
		case 'f': //read_file
			g_opt.file_to_read = optarg;
			break;
		case 'x': //verify read test
			g_opt.verify = 1;
			break;
		case 'e': //delete test
			g_opt.delete = 1;
			break;
		case 'z': //format device
			g_opt.format = 1;
			break;
		case 'o': //key offset (start from)
			{
				int num = 0;
				int index = optind - 1;
				char *next = NULL;
				while (index < argc) {
					next = strdup(argv[index]);
					index++;
					if (next[0] != '-') {
						unsigned int val = atoi(next);
						g_opt.key_offset[num++] = val;
					}
					else break;
				}
				optind = index - 1;
				break;
			}
			break;
		case 'y': //key range (start end)
			{
				int num = 0;
				int index = optind - 1;
				char *next = NULL;
				while (index < argc) {
					next = strdup(argv[index]);
					index++;
					if (next[0] != '-') {
						char *result = strtok(next, "-");
						if (strcmp(strdup(argv[index-1]), next) == 0) {
							printf("Invalid key range (%s)\n", argv[index-1]);
							return -EINVAL;
						}

						unsigned int key_start = atoi(result);
						result = strtok(NULL, "-");
						unsigned int key_end = atoi(result);

						g_opt.key_range[num].key_start = key_start;
						g_opt.key_range[num].key_end = key_end;
						if (key_start > key_end){
		                                        printf("Invalid key range (start key=%u, end key=%u)\n", key_start, key_end);
				                        return -EINVAL;
						}
						num++;
					}
					else break;
				}
				optind = index - 1;
				break;
			}
			break;
		case 'p': //work load type
			{
				int num = 0;
				int index = optind - 1;
				char *next = NULL;
				while (index < argc) {
					next = strdup(argv[index]);
					index++;
					if (next[0] != '-') {
						unsigned int val = atoi(next);
						g_opt.key_dist[num++] = val;
					}
					else break;
				}
				optind = index - 1;
				break;
			}
			break;
		case 'g': //random number seed
			{
				g_opt.num_seeds = 0;
				int index = optind - 1;
				char *next = NULL;
				while (index < argc) {
					next = strdup(argv[index]);
					index++;
					if (next[0] != '-') {
						unsigned int val = atoi(next);
						g_opt.seed[g_opt.num_seeds++] = val;
					}
					else break;
				}
				optind = index - 1;
				break;
			}
			break;
		case 'j': //json file path
			{
				g_opt.json_path = optarg;
			}
			break;
		case 'u': //use read cache
			{
				g_opt.cache = 1;
			}
			break;
		case 'l': //slab_size
			{
				g_opt.slab_size = atoi(optarg) * 1024 * 1024;
			}
			break;
		case 'i': //get_log
			{
				g_opt.send_get_log = 1;
			}
			break;
		case 'L':
			{
				g_low_cmd_mode = !g_low_cmd_mode;
			}
			break;
		case 'h': //help
			{
				printf("[USAGE] ./kv_perf [options] ...\n");
				show_kv_perf_menu();
				return -EINVAL;
			}
			break;
		default:
			{
				printf("Error: Unknown key. Use:\n");
				show_kv_perf_menu();
				return -EINVAL;
			}

			return -EINVAL;
		}
	}

	if (g_opt.read == 0 && g_opt.write == 0 && g_opt.delete == 0 && g_opt.format == 0 && g_opt.blend == 0 && g_opt.verify == 0 && g_opt.send_get_log == 0) {
		printf("At least one of tests should be specified: -w(write), -r(read), -e(delete), -b(blend), -z(format), -i(info)\n");
		return -EINVAL;
	}

	if ((g_opt.blend != 0) && (g_opt.read != 0 || g_opt.write != 0 || g_opt.delete != 0 || g_opt.format != 0 || g_opt.verify != 0)) {
		printf("This combination of tests is not supported.\n");
		printf("You can run the test(s) of 1) blend(-b) only, or, 2) other options without blend\n");
		return -EINVAL;
	}

	if (g_num_devices!=0) {	//if users set device description by cmd
		for(unsigned int i = 0; i< g_num_devices; i++){
			fprintf(stderr,"Device %d Name: %s, core_mask: %s, sync_mask: %s, cq_thread_msk: %s, queue_depth: %s\n", i, device_opts[i].ssd, device_opts[i].core_mask, device_opts[i].sync_mask, device_opts[i].cq_thread_mask, device_opts[i].queue_depth);
		}
	}

	return 0;
}

void show_results(const char *test_name, struct timeval *test_time, struct test_opt *param, unsigned int thread_index, unsigned int device)
{
	float iops = 0;
	float total_size_kb = 0;
	unsigned int time_ms = test_time->tv_sec*1000+test_time->tv_usec/1000;
	unsigned long long time_us = test_time->tv_sec*1000000+test_time->tv_usec;

	printf("\nDevice [%u] Thread Index: %u results for %s\n", device, thread_index, test_name);

	unsigned long long total_operations = 0;

	printf("[WRITE]:[READ]:[DEL] = %llu:%llu:%llu\n",
			g_private_data[device][thread_index].op_stat[WRITE].cnt,
			g_private_data[device][thread_index].op_stat[READ].cnt,
			g_private_data[device][thread_index].op_stat[DEL].cnt);
	const char *op_str[4] = {"[NOP]", "[WRITE]", "[READ]", "[DEL]"};
	for (int op_idx = WRITE; op_idx <= DEL; op_idx++) {
		if (g_private_data[device][thread_index].op_stat[op_idx].cnt) {
			unsigned long long total_key_size = 0;
			unsigned long long total_value_size = 0;
			for (int i = 0; i < 8; i++) { //show results for each value_size
				if (!g_opt.value_size[i]) break;
				printf("%s operations: %llu, Key size: %d, Value size: %d\n",
						op_str[op_idx],
						g_private_data[device][thread_index].op_stat[op_idx].value_idx_cnt[i],
						g_opt.key_size,
						g_opt.value_size[i]);
				total_key_size += g_opt.key_size * g_private_data[device][thread_index].op_stat[op_idx].value_idx_cnt[i];
				total_value_size += g_opt.value_size[i] * g_private_data[device][thread_index].op_stat[op_idx].value_idx_cnt[i];
			}
			total_operations += g_private_data[device][thread_index].op_stat[op_idx].cnt;
			printf("%s Total Key Size: %llu\n", op_str[op_idx], total_key_size);
			printf("%s Total Value Size: %llu\n", op_str[op_idx], total_value_size);
			total_size_kb = (float)(total_value_size)/(float)KB;
		}
	}

	printf("Latency: %.4f usec\n", (float)time_us/total_operations);
	if (test_time->tv_sec == 0) {
		iops = ((float)((total_operations)/(float)time_us))*1000000;
		printf("OPS/sec: %.2f, ", iops);
		if (g_opt.blend != 1)
			printf("Throughput: %.2f KB\n", (total_size_kb/(float)time_us)*1000000);
		printf("Test time: %llu usec\n", time_us);
	} else {
		iops = ((float)(total_operations)/(float)time_ms)*1000;
		printf("OPS/sec: %.2f, ", iops);
		if (g_opt.blend != 1)
			printf("Throughput: %.2f KB\n", (total_size_kb/(float)time_ms)*1000);
		printf("Test time: %d msec\n", time_ms);
	}

	if(strcmp(test_name, "Write") == 0) {
		g_private_data[device][thread_index].write_iops = iops;
	} else if(strcmp(test_name, "Read") == 0) {
		g_private_data[device][thread_index].read_iops = iops;
	} else if (strcmp(test_name, "Delete") == 0) {
		g_private_data[device][thread_index].delete_iops = iops;
	} else if (strcmp(test_name, "Blend") == 0) {
		g_private_data[device][thread_index].blend_iops = iops;
	} else {
		printf("Invalid test name: %s\n", test_name);
	}

	return;
}

void show_smart_info(char *log_buffer) {
	// this function depends on send_log_thread: this function must be called after success of kv_get_log_page()
	printf("============ Extended S.M.A.R.T. Information ============\n");
	printf("    nRetrieve_TotalSubmissionCommandCount: %llu\n", *(unsigned long long int*) (log_buffer + 512));
	printf("    nRetrieve_TotalSucceededCompletionCommandCount: %llu\n", *(unsigned long long int*) (log_buffer + 520));
	printf("    nRetrieve_TotalFailedCompletionCommandCount: %llu\n", *(unsigned long long int*) (log_buffer + 528));
	printf("    nStore_TotalSubmissionCommandCount: %llu\n", *(unsigned long long int*) (log_buffer + 536));
	printf("    nStore_TotalSucceededCompletionCommandCount: %llu\n", *(unsigned long long int*) (log_buffer + 544));
	printf("    nStore_TotalFailedCompletionCommandCount: %llu\n", *(unsigned long long int*) (log_buffer + 552));
	printf("    nAppend_TotalSubmissionCommandCount: %llu\n", *(unsigned long long int*) (log_buffer + 560));
	printf("    nAppend_TotalSucceededCompletionCommandCount: %llu\n", *(unsigned long long int*) (log_buffer + 568));
	printf("    nAppend_TotalFailedCompletionCommandCount: %llu\n", *(unsigned long long int*) (log_buffer + 576));
	printf("    nDelete_TotalSubmissionCommandCount: %llu\n", *(unsigned long long int*) (log_buffer + 584));
	printf("    nDelete_TotalSucceededCompletionCommandCount: %llu\n", *(unsigned long long int*) (log_buffer + 592));
	printf("    nDelete_TotalFailedCompletionCommandCount: %llu\n", *(unsigned long long int*) (log_buffer + 600));

	printf("    nRetrieveMaxValueSize: %u\n", *(unsigned int*) (log_buffer + 728));
	printf("    nRetrieveMinValueSize: %u\n", *(unsigned int*) (log_buffer + 732));
	printf("    nRetrieveAvgValueSize: %u\n", *(unsigned int*) (log_buffer + 736));
	printf("    nStoreMaxValueSize_CommandSpec: %u\n", *(unsigned int*) (log_buffer + 740));
	printf("    nStoreMinValueSize_CommandSpec: %u\n", *(unsigned int*) (log_buffer + 744));
	printf("    nStoreAvgValueSize: %u\n", *(unsigned int*) (log_buffer + 748));
	printf("    nAppendMaxValueSize_CommandSpec: %u\n", *(unsigned int*) (log_buffer + 752));
	printf("    nAppendMinValueSize_CommandSpec: %u\n", *(unsigned int*) (log_buffer + 756));
	printf("    nAppendAvgValueSize: %u\n", *(unsigned int*) (log_buffer + 760));

	printf("    nMaxKeySize_Spec: %u\n", *(unsigned int*) (log_buffer + 824));
	printf("    nMinKeySize_Spec: %u\n", *(unsigned int*) (log_buffer + 828));
	printf("    nKey_Alignment_Spec: %u\n", *(unsigned int*) (log_buffer + 832));
	printf("    nMaxValueSize_Spec: %u\n", *(unsigned int*) (log_buffer + 836));
	printf("    nMinValueSize_Spec: %u\n", *(unsigned int*) (log_buffer + 840));
	printf("    nValue_Alignment_Spec: %u\n", *(unsigned int*) (log_buffer + 844));
	printf("    nRetrieveMaxValueSize_CommandSpec: %u\n", *(unsigned int*) (log_buffer + 848));
	printf("    nRetrieveMinValueSize_CommandSpec: %u\n", *(unsigned int*) (log_buffer + 852));
	printf("    nRetrieveValue_Alignment_CommandSpec: %u\n", *(unsigned int*) (log_buffer + 856));
	printf("    nStoreMaxValueSize_CommandSpec: %u\n", *(unsigned int*) (log_buffer + 860));
	printf("    nStoreMinValueSize_CommandSpec: %u\n", *(unsigned int*) (log_buffer + 864));
	printf("    nStoreValue_Alignment_CommandSpec: %u\n", *(unsigned int*) (log_buffer + 868));
	printf("    nAppendMaxValueSize_CommandSpec: %u\n", *(unsigned int*) (log_buffer + 872));
	printf("    nAppendMinValueSize_CommandSpec: %u\n", *(unsigned int*) (log_buffer + 876));
	printf("    nAppendValue_Alignment_CommandSpec: %u\n", *(unsigned int*) (log_buffer + 880));
	printf("    WAF: %u\n", *(unsigned int*) (log_buffer + 944));
	printf("=========================================================\n");
}

void send_log_thread(void *data){
	int i;
	int ret = 0;
	int nr_send_log_page = 0;
	int* run_send_log_thread = (int*)data;
	int log_id = 0xC0;
	char buffer[1024];

	int current = kv_get_core_id();
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(current,&cpuset);
	sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

	//Show Handles on Given CPU
	int nr_handle = 0;
	uint64_t arr_handle[NR_MAX_SSD];
	memset((char*)arr_handle, 0, sizeof(arr_handle));
	uint64_t handle = 0;

	for(i=0;i<MAX_CPU_CORES;i++){
		ret = kv_get_devices_on_cpu(i, &nr_handle, arr_handle);
		if(ret == KV_SUCCESS){
			handle = arr_handle[0];	
			printf("[%s] issues smart_log_page on handle=%ld\n",__FUNCTION__, handle);
			break;
		}
	}

	while(*run_send_log_thread == 1){
		int line = 16;
		memset(buffer,0,sizeof(buffer));
		ret = kv_get_log_page(handle, log_id, buffer, sizeof(buffer));
		if(ret == KV_SUCCESS){
			nr_send_log_page++;
			if (g_opt.read == 0 && g_opt.write == 0 && g_opt.delete == 0 && g_opt.format == 0 && g_opt.blend == 0 && g_opt.verify == 0) {
				show_smart_info(buffer);
			}
			for(i=0;i<(signed)sizeof(buffer);i++){
				if(!(i%line)){
					printf("%04x: ", i);
				}
				printf("%02x ", *(unsigned char*)(buffer+i));

				if(i>0 && !((i+1)%line)){
					printf("\n");
				}
			}
		}
		printf("nr_send_log_page = %d\n", nr_send_log_page);
		sleep(1);
	}
}

void io_thread(void *data) {
	thread_priv_data_t *tpd = NULL;
	unsigned int thread_index = 0;
	int ret, test_count = 0, queue_io_type = 0;
	unsigned int device;
	struct timeval time;
	int qid = DEFAULT_IO_QUEUE_ID;

	tpd = (thread_priv_data_t *)data;
	thread_index = tpd->thread_index;
	device = tpd->device_index;

	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(tpd->cpu_id, &cpuset);
	sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

	uint64_t handle = sdk_opt.dev_handle[device];

	queue_io_type = kv_io_queue_type(handle, tpd->cpu_id);

	if(queue_io_type < 0) {
		printf("Invalid I/O Queue type for the CPU Core ID\n");
		return;
	}

	pthread_mutex_init(&tpd->mutex, NULL);

	while(test_count < g_opt.num_tests) {
		printf("Device [%u] Thread Index: %u Test %d is running...\n", device, thread_index, test_count);
		struct kvpair *kv;

		if (g_opt.write || g_opt.verify) {
			KVNVME_TIMESTART(time);

			tpd->submit_cnt = 0;
			tpd->cb_cnt = 0;

			for (int i = 0; i < 4; i++) {
				tpd->op_stat[i].cnt = 0;
				for (int j = 0; j < 8; j++) {
					tpd->op_stat[i].value_idx_cnt[j] = 0;
				}
			}

			TAILQ_FOREACH(kv, &g_private_data[device][thread_index].kv_data, tailq) {
				kv->pair.param.io_option.store_option = KV_STORE_DEFAULT;
				if(queue_io_type == ASYNC_IO_QUEUE) {
					ret = -EINVAL;
					kv->pair.param.private_data = (void *)kv;
					while(ret) {
						gettimeofday(&kv->start, NULL);
						KV_PERF_ASYNC_IO(store, ret, handle, qid, &kv->pair);
						if(ret) {
							//usleep(1);
						} else {
							//gettimeofday(&kv->start, NULL);
							tpd->submit_cnt++;
							tpd->op_stat[WRITE].cnt++;
							tpd->op_stat[WRITE].value_idx_cnt[kv->value_idx]++;
							break;
						}
					}
				} else {
					gettimeofday(&kv->start, NULL);
					KV_PERF_SYNC_IO(store, ret, handle, qid, &kv->pair);
					gettimeofday(&kv->end, NULL);
					if(ret) {
						printf("kv_store() failed for the key %s, test: %d\n", (char *)kv->pair.key.key, test_count);
						break;
					} else {
						tpd->op_stat[WRITE].cnt++;
						tpd->op_stat[WRITE].value_idx_cnt[kv->value_idx]++;
					}
				}
			}

			if(queue_io_type == ASYNC_IO_QUEUE) {
				while(tpd->cb_cnt < tpd->submit_cnt) {
					usleep(1);
				}
			}

			KVNVME_TIMESTOP(time);

			struct latency_stat stat;
			reset_latency_stat(&stat);
			TAILQ_FOREACH(kv, &g_private_data[device][thread_index].kv_data, tailq) {
				add_latency_stat(&stat, &kv->start, &kv->end);
			}


			// Print benchmark results
			if (!ret) {
				show_results("Write", &time, &g_opt, thread_index, device);
				print_latency_stat(&stat);
			}
			else
				break;
		}

		if (g_opt.verify) {
			TAILQ_FOREACH(kv, &g_private_data[device][thread_index].kv_data, tailq) {
				memset(kv->pair.value.value, 0, kv->pair.value.length);
			}
		}

		if (g_opt.read || g_opt.verify) {
			KVNVME_TIMESTART(time);

			tpd->submit_cnt = 0;
			tpd->cb_cnt = 0;

			for (int i = 0; i < 4; i++) {
				tpd->op_stat[i].cnt = 0;
				for (int j = 0; j < 8; j++) {
					tpd->op_stat[i].value_idx_cnt[j] = 0;
				}
			}

			TAILQ_FOREACH(kv, &g_private_data[device][thread_index].kv_data, tailq) {
				kv->pair.param.io_option.retrieve_option = KV_RETRIEVE_DEFAULT;
				if(queue_io_type == ASYNC_IO_QUEUE) {
					ret = -EINVAL;
					kv->pair.param.private_data = (void *)kv;
					while(ret) {
						gettimeofday(&kv->start, NULL);
						KV_PERF_ASYNC_IO(retrieve, ret, handle, qid, &kv->pair);

						if(ret) {
							//usleep(1);
						} else {
							//gettimeofday(&kv->start, NULL);
							tpd->submit_cnt++;
							tpd->op_stat[READ].cnt++;
							tpd->op_stat[READ].value_idx_cnt[kv->value_idx]++;
							break;
						}
					}
				} else {
					gettimeofday(&kv->start, NULL);
					KV_PERF_SYNC_IO(retrieve, ret, handle, qid, &kv->pair);
					gettimeofday(&kv->end, NULL);
					if(ret) {
						printf("kv_retrieve() failed for the key %s, test: %d\n", (char *)kv->pair.key.key, test_count);
						break;
					} else {
						tpd->op_stat[READ].cnt++;
						tpd->op_stat[READ].value_idx_cnt[kv->value_idx]++;
					}
				}
			}

			if(queue_io_type == ASYNC_IO_QUEUE) {
				while(tpd->cb_cnt < tpd->submit_cnt) {
					usleep(1);
				}
			}

			KVNVME_TIMESTOP(time);

			// Copy key value pairs to file provided
			if(g_opt.file_to_read) {
				FILE *f = fopen(g_opt.file_to_read, "w");
				char *STRING_KEY = "KEY:\n";
				char *STRING_DASH = "=========\n";
				char *STRING_NEWLINE = "\n";
				char *STRING_VALUE = "VALUE:\n";

				if(f == NULL) {
					printf("Failed to open the File provided\n");
					break;
				}

				TAILQ_FOREACH(kv, &g_private_data[device][thread_index].kv_data, tailq) {
					kv->pair.param.io_option.retrieve_option = KV_RETRIEVE_DEFAULT;
					if(queue_io_type == ASYNC_IO_QUEUE) {
						ret = -EINVAL;
						kv->pair.param.private_data = (void *)kv;
						while(ret) {
							KV_PERF_ASYNC_IO(retrieve, ret, handle, qid, &kv->pair);

							if(ret) {
								//usleep(1);
							} else {
								tpd->submit_cnt++;
								break;
							}
						}
					} else {
						KV_PERF_SYNC_IO(retrieve, ret, handle, qid, &kv->pair);
						if(ret) {
							printf("kv_retreive() failed for the key %s, test: %d\n", (char *)kv->pair.key.key, test_count);
							break;
						}
					}

					fwrite(STRING_KEY, strlen(STRING_KEY), 1, f);
					fwrite(kv->pair.key.key, kv->pair.key.length, 1, f);
					fwrite(STRING_NEWLINE, strlen(STRING_NEWLINE), 1, f);
					fwrite(STRING_DASH, strlen(STRING_DASH), 1, f);
					fwrite(STRING_VALUE, strlen(STRING_VALUE), 1, f);
					fwrite(kv->pair.value.value, kv->pair.value.length, 1, f);
					fwrite(STRING_NEWLINE, strlen(STRING_NEWLINE), 1, f);
					fwrite(STRING_DASH, strlen(STRING_DASH), 1, f);
				}
			}


			struct latency_stat stat;
			reset_latency_stat(&stat);
			TAILQ_FOREACH(kv, &g_private_data[device][thread_index].kv_data, tailq) {
				add_latency_stat(&stat, &kv->start, &kv->end);
			}
			// Print benchmark results
			if(!ret) {
				show_results("Read", &time, &g_opt, thread_index, device);
				print_latency_stat(&stat);
			}
			else
				break;

		}

		if (g_opt.verify) {
			g_private_data[device][thread_index].miscompare_cnt = 0;
			TAILQ_FOREACH(kv, &g_private_data[device][thread_index].kv_data, tailq) {
				unsigned long long tmp_hash[2];
				Hash128_2_P128(kv->pair.value.value, kv->pair.value.length, 0x12345678L, tmp_hash); 
				if (is_miscompare(tmp_hash, kv->hash_value)) {
					g_private_data[device][thread_index].miscompare_cnt++;
				/* for debug */
				//printf("expected hash_value %llu, %llu\n", kv->hash_value[0], kv->hash_value[1]);
				//printf("real hash_value %llu, %llu\n", tmp_hash[0], tmp_hash[1]);
				}
			}
			printf("Miscompare count: %u\n", g_private_data[device][thread_index].miscompare_cnt);
		}

		if (g_opt.delete) {
			KVNVME_TIMESTART(time);

			tpd->submit_cnt = 0;
			tpd->cb_cnt = 0;

			for (int i = 0; i < 4; i++) {
				tpd->op_stat[i].cnt = 0;
				for (int j = 0; j < 8; j++) {
					tpd->op_stat[i].value_idx_cnt[j] = 0;
				}
			}

			TAILQ_FOREACH(kv, &g_private_data[device][thread_index].kv_data, tailq) {
				kv->pair.param.io_option.delete_option = KV_DELETE_DEFAULT;
				if(queue_io_type == ASYNC_IO_QUEUE) {
					ret = -EINVAL;
					kv->pair.param.private_data = (void *)kv;
					while(ret) {
						gettimeofday(&kv->start, NULL);
						KV_PERF_ASYNC_IO(delete, ret, handle, qid, &kv->pair);

						if(ret) {
							//usleep(1);
						} else {
							//gettimeofday(&kv->start, NULL);
							tpd->submit_cnt++;
							tpd->op_stat[DEL].cnt++;
							tpd->op_stat[DEL].value_idx_cnt[kv->value_idx]++;
							break;
						}
					}
				} else {
					gettimeofday(&kv->start, NULL);
					KV_PERF_SYNC_IO(delete, ret, handle, qid, &kv->pair);
					gettimeofday(&kv->end, NULL);
					if(ret) {
						printf("kv_nvme_delete() failed for the key %s, test: %d\n", (char *)kv->pair.key.key, test_count);
						break;
					} else {
						tpd->op_stat[DEL].cnt++;
						tpd->op_stat[DEL].value_idx_cnt[kv->value_idx]++;
					}
				}
			}

			if(queue_io_type == ASYNC_IO_QUEUE) {
				while(tpd->cb_cnt < tpd->submit_cnt) {
					usleep(1);
				}
			}

			KVNVME_TIMESTOP(time);

			struct latency_stat stat;
			reset_latency_stat(&stat);
			TAILQ_FOREACH(kv, &g_private_data[device][thread_index].kv_data, tailq) {
				add_latency_stat(&stat, &kv->start, &kv->end);
			}

			// Print benchmark results
			if (!ret) {
				show_results("Delete", &time, &g_opt, thread_index, device);
				print_latency_stat(&stat);
			}
			else
				break;
		}

		if (g_opt.blend) {
			KVNVME_TIMESTART(time);

			tpd->submit_cnt = 0;
			tpd->cb_cnt = 0;
			for (int i = 0; i < 4; i++) {
				tpd->op_stat[i].cnt = 0;
				for (int j = 0; j < 8; j++) {
					tpd->op_stat[i].value_idx_cnt[j] = 0;
				}
			}

			TAILQ_FOREACH(kv, &g_private_data[device][thread_index].kv_data, tailq) {
				if (queue_io_type == ASYNC_IO_QUEUE) {
					ret = -EINVAL;
					kv->pair.param.private_data = (void *)kv;
					while(ret) {
						gettimeofday(&kv->start, NULL);

						switch(kv->op) {
							case WRITE:
								kv->pair.param.io_option.store_option = KV_STORE_DEFAULT;
								KV_PERF_ASYNC_IO(store, ret, handle, qid, &kv->pair);
								break;
							case READ:
								kv->pair.param.io_option.retrieve_option = KV_RETRIEVE_DEFAULT;
								KV_PERF_ASYNC_IO(retrieve, ret, handle, qid, &kv->pair);
								break;
							case DEL:
								kv->pair.param.io_option.delete_option = KV_DELETE_DEFAULT;
								KV_PERF_ASYNC_IO(delete, ret, handle, qid, &kv->pair);
								break;
							default:
								break;
						}

						if(ret) {
							//usleep(1);
						} else {
							//gettimeofday(&kv->start, NULL);
							tpd->submit_cnt++;
							tpd->op_stat[kv->op].cnt++;
							tpd->op_stat[kv->op].value_idx_cnt[kv->value_idx]++;
							break;
						}
					}
				} else {
					gettimeofday(&kv->start, NULL);
					switch(kv->op) {
						case WRITE:
							kv->pair.param.io_option.store_option = KV_STORE_DEFAULT;
							KV_PERF_SYNC_IO(store, ret, handle, qid, &kv->pair);
							break;
						case READ:
							kv->pair.param.io_option.retrieve_option = KV_RETRIEVE_DEFAULT;
							KV_PERF_SYNC_IO(retrieve, ret, handle, qid, &kv->pair);
							break;
						case DEL:
							kv->pair.param.io_option.delete_option = KV_DELETE_DEFAULT;
							KV_PERF_SYNC_IO(delete, ret, handle, qid, &kv->pair);
							break;
						default:
							break;
					}
					gettimeofday(&kv->end, NULL);
					if(ret) {
						printf("kv_op() failed for the key %s, test: %d\n", (char *)kv->pair.key.key, test_count);
						break;
					} else {
						tpd->op_stat[kv->op].cnt++;
						tpd->op_stat[kv->op].value_idx_cnt[kv->value_idx]++;
					}
				}
			}

			if (queue_io_type == ASYNC_IO_QUEUE) {
				while(tpd->cb_cnt < tpd->submit_cnt) {
					usleep(1);
				}
			}

			KVNVME_TIMESTOP(time);


			struct latency_stat blend_stat[NUM_BLEND_OP+1];
			for(int i=0; i<NUM_BLEND_OP+1; i++) {
				reset_latency_stat(&blend_stat[i]);
			}

			TAILQ_FOREACH(kv, &g_private_data[device][thread_index].kv_data, tailq) {
				add_latency_stat(&blend_stat[0], &kv->start, &kv->end);
				add_latency_stat(&blend_stat[kv->op], &kv->start, &kv->end);
			}
			// Print benchmark results
			if (!ret) {
				show_results("Blend", &time, &g_opt, thread_index, device);
				char *result_title[] = {"   [Total Blended IO Latency]",
							"   [Store Latency]",
							"   [Retrieve Latency]",
							"   [Delete Latency]"};
				for(int i=0; i<NUM_BLEND_OP+1; i++){
					if (!blend_stat[i].samples) continue;
					printf("%s\n", result_title[i]);
					print_latency_stat(&blend_stat[i]);
					printf("\n");
				}
			}
			else
				break;

		}

		test_count++;
	}
}

void show_device_info(char *text){ // should be called after kv_sdk_init()
	int device;
	uint64_t total_size, used_size;
	double waf = .0;

	printf("--------------------------------------------------------------------------\n");
	printf("Device Information");
	if (text != NULL) 
		printf(": %s", text);
	printf("\n");
        for(device = 0; device < sdk_opt.nr_ssd; device++){
		uint64_t handle = sdk_opt.dev_handle[device];
		total_size = kv_get_total_size(handle);
		used_size = kv_get_used_size(handle);
		waf = kv_get_waf(handle);

		printf("Device ID: %s\n", sdk_opt.dev_id[device]);
		if ((total_size != KV_ERR_INVALID_VALUE) && (used_size != KV_ERR_INVALID_VALUE) && (waf != KV_ERR_INVALID_VALUE)){
			waf /= 10;
			printf("\tTotal Size of the NVMe Device: %lld MB\n", (unsigned long long)total_size / MB);
			printf("\tUsed Size of the NVMe Device: %.2f %s\n", (float)used_size / 100, "%");
			printf("\tWAF: %f\n", waf / 10);
		} else {
			printf("\tCan't load device info\n");	
		}
		printf("\n");
	}
	printf("--------------------------------------------------------------------------\n");
}


int main(int argc, char **argv) {
	unsigned int i, tmp_i;
	int device=0;
	int ret = -EINVAL;
	float read_iops[NR_MAX_SSD] = {0}, write_iops[NR_MAX_SSD] = {0}, delete_iops[NR_MAX_SSD] = {0}, blend_iops[NR_MAX_SSD] = {0};
	thread_priv_data_t *tpd = NULL;
	unsigned int start_key[NUM_BLEND_OP] = {0,};
	unsigned int num_io_thread= 0;

	kv_sdk_info(); 

	srand(time(NULL));

	ret = parse_opt(argc, argv);
	if (ret) {
		return ret;
	}
	printf("Starting tests...\n");

	ret = kv_sdk_load_option(&sdk_opt, g_opt.json_path);
	if (ret != KV_SUCCESS) {
		printf("Fail to read json configuration file\n");
		return ret;
	}
	show_test_info();

	set_cmd_opt_to_sdk(); //overwrite cmd config to sdk_opt

	for(int i = 0; i < sdk_opt.nr_ssd; i++) {
		if (!g_opt.core_mask) {
			for(int j = 0; j < MAX_CPU_CORES; j++){
				if(sdk_opt.dd_options[i].core_mask & (1ULL << j)) {
					g_cpu_core_id[i][g_num_cpu_cores[i]++] = j;
				}
			}
		}
		num_io_thread += g_num_cpu_cores[i];
	}

	set_io_function(g_low_cmd_mode); //added to support low cmd API
	if (g_low_cmd_mode) {
		uint64_t io_buffer = (uint64_t)(((g_opt.max_value_size * g_opt.num_keys[0] * 1.5)) * num_io_thread);
		sdk_opt.app_hugemem_size = io_buffer;
	}


	ret = kv_sdk_init(KV_SDK_INIT_FROM_STR, &sdk_opt);

	if (ret != KV_SUCCESS) {
		printf("Can't initialize\n");
		return ret;
	}

	show_device_info("Before Test\n");

	if(g_opt.format) {
		for(i=0;i<(unsigned int)sdk_opt.nr_ssd;i++){
			int ret = kv_format_device(sdk_opt.dev_handle[i], KV_FORMAT_USERDATA);
			if(ret) {
				printf("Error in formatting the device, ret : %d\n", ret);
				goto finalize_sdk;
			}
			printf("Successfully formatted the device: %s\n", sdk_opt.dev_id[i]);
		}

        }

        for(i = 0; i < NUM_BLEND_OP; i++) {
                start_key[i] += g_opt.key_offset[i];
        }

	for(device = 0; device < sdk_opt.nr_ssd; device++){
		for(i = 0; i < g_num_cpu_cores[device]; i++) {
			g_private_data[device][i].device_index = device;
			g_private_data[device][i].thread_index = i;
			g_private_data[device][i].cpu_id = g_cpu_core_id[device][i];
			int j = 0;
			for (j = 0; j < NUM_BLEND_OP; j++) {
				g_private_data[device][i].start_key[j] = start_key[j];
				g_private_data[device][i].num_keys[j] = g_opt.num_keys[j];
				start_key[j] += g_private_data[device][i].num_keys[j] + g_opt.key_offset[j];
			}

			ret = create_test_data(device, i, i);
			if(ret) {
				printf("Create test data error\n");
				for(tmp_i = 0; tmp_i < i; tmp_i++) {
					delete_test_data(device, tmp_i);
				}
				goto finalize_sdk;
			}
		}
	}

	for(device = 0; device < sdk_opt.nr_ssd; device++){
		for(i = 0; i < g_num_cpu_cores[device]; i++) {
			tpd = &g_private_data[device][i];

			ret = pthread_create(&threadid[device][i], NULL, (void *)&io_thread, tpd);

			if(ret) {
				printf("Pthread create error\n");
				for(tmp_i = 0; tmp_i < i; tmp_i++) {
					pthread_join(threadid[device][tmp_i], NULL);
				}

				goto delete_test_data;
			}
		}
	}

	//for send_get_log
	pthread_t t_send_log;
	int run_send_log_thread = 1;
	if(g_opt.send_get_log) {
		ret = pthread_create(&t_send_log, NULL, (void *)&send_log_thread, &run_send_log_thread);
	}
	
	for(device = 0; device < sdk_opt.nr_ssd; device++){
		for(i = 0; i < g_num_cpu_cores[device]; i++) {
			tpd = &g_private_data[device][i];
			pthread_join(threadid[device][i], NULL);
			submit_cnt[device] += tpd->submit_cnt;
			cb_cnt[device] += tpd->cb_cnt;
		}
		printf("[Device no: %u, Received Callbacks Count: %d, Submitted I/Os Count: %d]\n", device, cb_cnt[device], submit_cnt[device]);
	}

	run_send_log_thread = 0;
	if(g_opt.send_get_log) {
		pthread_join(t_send_log, NULL);
	}


	show_device_info("After Test\n");

delete_test_data:
	for(device = 0; device < sdk_opt.nr_ssd; device++){
		for(i = 0; i < g_num_cpu_cores[device]; i++) {
			delete_test_data(device, i);
		}
	}

finalize_sdk:
	kv_sdk_finalize();

	for(device = 0; device <sdk_opt.nr_ssd; device++){
		for(i = 0; i < g_num_cpu_cores[device]; i++) {
			read_iops[device] += g_private_data[device][i].read_iops;
			write_iops[device] += g_private_data[device][i].write_iops;
			delete_iops[device] += g_private_data[device][i].delete_iops;
			blend_iops[device] += g_private_data[device][i].blend_iops;
		}
		printf("%s Device's Write OPS/sec: %.2f\n", sdk_opt.dev_id[device], write_iops[device]);
		printf("%s Device's Read OPS/sec: %.2f\n", sdk_opt.dev_id[device], read_iops[device]);
		printf("%s Device's Delete OPS/sec: %.2f\n", sdk_opt.dev_id[device], delete_iops[device]);
		printf("%s Device's Blend OPS/sec: %.2f\n", sdk_opt.dev_id[device], blend_iops[device]);
	}

        if(ret)
                printf("FAIL\n");
        else
                printf("SUCCESS\n");

	return ret;
}
