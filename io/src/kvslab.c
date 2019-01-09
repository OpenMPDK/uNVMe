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
#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<string.h>

#include "kvutil.h"
#include "kvslab.h"
#include "kvcache.h"

#define MEMORY_ALIGNMENT (256)

static pthread_mutex_t *kvsl_slab_mutex;

int kvslab_init(size_t total_slab_size, int slab_alloc_policy, int nr_ssd){
	kvsl_rstatus_t status;

	double factor = 2.0;
	size_t chunk_size = KV_MEM_ALIGN(KVSLAB_ITEM_HDR_SIZE + 4*KB + sizeof(kv_pair) + KV_MAX_KEY_LEN + MEMORY_ALIGNMENT, KVSLAB_ALIGNMENT); //4KB value with 255B key

	/**
	 * slab can support upto 64KB value length for now.
	 */
	size_t slab_size = KV_MEM_ALIGN(HUGEPAGE_SIZE, KVSLAB_ALIGNMENT);
	size_t max_slab_memory = KV_MEM_ALIGN(total_slab_size, KVSLAB_ALIGNMENT);

	kvsl_set_options(false, factor, max_slab_memory, chunk_size, slab_size, slab_alloc_policy, nr_ssd);

	status = kvsl_slab_init();
	if (status != KVSLAB_OK) {
		return KVSLAB_ERROR;
	}

        kvsl_slab_mutex = malloc(nr_ssd*sizeof(pthread_mutex_t));
        if(!kvsl_slab_mutex){
                return KVSLAB_ERROR;
        }
        for(int i=0; i<nr_ssd; i++){
                if(pthread_mutex_init(&kvsl_slab_mutex[i], NULL) != 0){
                        return KVSLAB_ERROR;
                }
        }

	print_slab_class_info(true);

	return status;
}

int kvslab_destroy(){
	print_slab_class_info(true);

	if(kvsl_slab_mutex){
		free(kvsl_slab_mutex);
	}
	kvsl_rstatus_t result = kvsl_slab_deinit();
	if (result != KVSLAB_OK) {
		exit(1);
	}
	return 0;
}
	
kv_pair* posix_alloc_pair(int key_len, int value_len, int flag){
	if(key_len <= 0 || value_len <= 0){
		goto err;
	}
	kv_pair* kv = malloc(sizeof(kv_pair));
	if(!kv){
		goto err;
	}
	kv->key.key = malloc(key_len);
	if(!kv->key.key){
		free(kv);
		goto err;
	}
	memset(kv->key.key,0,key_len);

	kv->value.value = malloc(value_len);
	if(!kv->value.value){
		free(kv->key.key);
		free(kv);
		goto err;
	}
	memset(kv->value.value,0,value_len);
	return kv;

err:
	return NULL;
}

void posix_free_pair(kv_pair* kv){
	if(!kv){
		return;
	}
	if(kv->key.key){
		free(kv->key.key);
		kv->key.key = NULL;
	}
	if(kv->value.value){
		free(kv->value.value);
		kv->value.value = NULL;
	}
	free(kv);
	kv = NULL;
}


kv_pair* slab_alloc_pair(int key_len, int value_len, int did){
	if(key_len <= 0 || value_len < 0 || did < 0){
                goto err;
        }
	check_lock(pthread_mutex_lock(&kvsl_slab_mutex[did]));
	struct kvsl_item* item = kvsl_get_free_item(key_len+value_len+sizeof(kv_pair)+MEMORY_ALIGNMENT, (uint8_t)did);
	check_lock(pthread_mutex_unlock(&kvsl_slab_mutex[did]));

	if(!item){
		goto err;
	}

	void* data = (void*)kvsl_item_data(item);
	kv_pair* kv = data;
	kv->key.key = (void*)(((char*)data)+sizeof(kv_pair));
	kv->value.value = (void*)KV_PTR_ALIGN(((char*)kv->key.key)+key_len, MEMORY_ALIGNMENT);
	return kv;

err:
	return NULL;
}

kv_iterate* slab_alloc_iterate(int key_len, int value_len, int did){
	if(key_len <=0 || value_len <= 0 || did < 0){
                goto err;
        }
	check_lock(pthread_mutex_lock(&kvsl_slab_mutex[did]));
	struct kvsl_item* item = kvsl_get_free_item(key_len+value_len+sizeof(kv_iterate)+MEMORY_ALIGNMENT, (uint8_t)did);
	check_lock(pthread_mutex_unlock(&kvsl_slab_mutex[did]));

	if(!item){
		goto err;
	}

	void* data = (void*)kvsl_item_data(item);
	kv_iterate* it = (kv_iterate*)data;
	it->kv.key.key = (void*)(((char*)data)+sizeof(kv_iterate));
	it->kv.value.value = (void*)KV_PTR_ALIGN(((char*)it->kv.key.key)+key_len, MEMORY_ALIGNMENT);
	return it;

err:
	return NULL;
}

void slab_free_iterate(kv_iterate* it){
	slab_free_pair((kv_pair*)it);
}

void slab_free_pair(kv_pair* kv){
	if(!kv){
                return;
        }
	struct kvsl_item* item = kvsl_data_item((void*)kv);

	kvsl_slab_decrease_use_cnt(item->did, item->sid);
}
