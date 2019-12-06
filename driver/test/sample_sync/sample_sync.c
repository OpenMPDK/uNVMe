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

#define SUCCESS 0
#define FAILED 1

#define DEBUG_ON 1

#define INSERT_OP (1)
#define RETRIEVE_OP (2)
#define DELETE_OP (3)
#define ITERATOR_OP (4)
#define EXIST_OP (5)

uint32_t prefix = 0;
uint32_t bitmask = 0xFFFF0000;

int _print_iter_keys(void *iter_list_buff, uint32_t size,
                     uint32_t num_entries) {
  const int KEY_LEN_BYTES = 4;
  int ret = 0;
  unsigned int key_size = 0;
  int keydata_len_with_padding = 0;
  char *data_buff = (char *)iter_list_buff;
  unsigned int buffer_size = size;
  unsigned int key_count = num_entries; 

  // char *current_ptr = data_buff;
  unsigned int buffdata_len = buffer_size;
  if (data_buff == 0) return FAILED;

  buffdata_len -= KEY_LEN_BYTES;
  data_buff += KEY_LEN_BYTES;
  for (uint32_t i = 0; i < key_count && buffdata_len > 0; i++) {
    if (buffdata_len < KEY_LEN_BYTES) {
      ret = FAILED;
      break;
    }

    // get key size
    key_size = *((uint32_t *)data_buff);
    buffdata_len -= KEY_LEN_BYTES;
    data_buff += KEY_LEN_BYTES;
    fprintf(stderr, "Iterator key_len:%d, key:%s.\n", key_size,
                     data_buff);
    if (key_size > buffdata_len || key_size >= KV_MAX_KEY_LEN)
    {
      ret = FAILED;
      break;
    }

    // calculate 4 byte aligned current key len including padding bytes
    keydata_len_with_padding = (((key_size + 3) >> 2) << 2);
    // skip to start position of next key
    buffdata_len -= keydata_len_with_padding;
    data_buff += keydata_len_with_padding;
  }

  return ret;
}

void usage(char *program)
{
  printf("==============\n");
  printf("usage: %s -d device_path [-n num_ios] [-q queue_depth] [-o op_type] [-k klen] [-v vlen]\n", program);
  printf("-d      device_path  :  kvssd device path. e.g. udd: 0000:06:00.0\n");
  printf("-n      num_ios      :  total number of ios (ignore this for iterator)\n");
  printf("-o      op_type      :  1: write; 2: read; 3: delete; 4: iterator; 5: check key exist\n");
  printf("-k      klen         :  key length (ignore this for iterator)\n");
  printf("-v      vlen         :  value length (ignore this for iterator)\n");
  printf("==============\n");
}

 int _init_test_env(char* dev_path, uint64_t *dev_handle) {
  int ret = FAILED;
  //NOTE : check pci information first using lspci -v 
  unsigned int ssd_type = KV_TYPE_SSD;
  kv_nvme_io_options options = {0};
  options.core_mask = 1; // Use CPU 0
  options.sync_mask = 1; // Use Sync I/O mode
  options.num_cq_threads = 1; // Use only one CQ Processing Thread
  options.cq_thread_mask = 2; // Use CPU 7
  options.mem_size_mb = 256; // MAX -1

  ret = kv_env_init(options.mem_size_mb);
  if(ret){
    fprintf(stderr, "KV env initiation failed.\n");
    return ret;
  }
  ret = kv_nvme_init(dev_path, &options, ssd_type);
  if(ret){
    fprintf(stderr, "NVMe device initiation failed.\n");
    return ret;
  }
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(0, &cpuset); // CPU 0
  sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

  uint64_t handle = kv_nvme_open(dev_path);
  if(!handle){
    ret = kv_nvme_finalize(dev_path);
    return ret;
  }
  *dev_handle = handle;

  uint64_t total_size = kv_nvme_get_total_size(handle);
  if(total_size != KV_ERR_INVALID_VALUE){
    fprintf(stderr, "Total Size of the NVMe Device: %lld MB\n", (unsigned long long)total_size / MB);
  }

  uint64_t used_size = kv_nvme_get_used_size(handle);
  if(used_size != KV_ERR_INVALID_VALUE){
    if(ssd_type == KV_TYPE_SSD){
      fprintf(stderr, "Used utilization(0~10000) of the NVMe Device: %ld.\n",
        used_size);
    }
    else{
      fprintf(stderr, "Used Size of the NVMe Device: %lld MB\n", (unsigned long long)used_size / MB);
    }
  }

  double waf = (double)kv_nvme_get_waf(handle) / 10;
  if(waf != KV_ERR_INVALID_VALUE){
    fprintf(stderr, "WAF Before doing I/O: %f\n", waf);
  }
  return SUCCESS;
}

void _deinit_test_env(uint64_t handle, char *dev_path) {
  double waf = (double)kv_nvme_get_waf(handle) / 10;
  if(waf != KV_ERR_INVALID_VALUE){
    fprintf(stderr,"WAF After doing I/O: %f\n", waf);
  }

  int ret = kv_nvme_close(handle);
  if(ret){
    fprintf(stderr,"ERROR, close nvme failed. error 0x0%x\n", ret);
  }

  ret = kv_nvme_finalize(dev_path);
  if(ret){
    fprintf(stderr,"ERROR, finalize kv nvme failed. error 0x0%x\n", ret);
  }
}

void _check_iter_info(uint64_t handle) {
  //check if iterator is already opened, and close it if so.
  int nr_iterate_handle = KV_MAX_ITERATE_HANDLE;
  kv_iterate_handle_info info[KV_MAX_ITERATE_HANDLE];
  int ret = kv_nvme_iterate_info(handle, info, nr_iterate_handle);
  if(ret == KV_SUCCESS){
      fprintf(stderr,"iterate_handle count=%d\n",nr_iterate_handle);
      for(int i = 0; i < nr_iterate_handle; i++){
        #if DEBUG_ON
        fprintf(stderr, "iterate_handle_info[%d] : info.handle_id=%d "
          "info.status=%d info.type=%d info.prefix=%08x info.bitmask=%08x "
          "info.is_eof=%d\n", i+1, info[i].handle_id, info[i].status,
          info[i].type, info[i].prefix, info[i].bitmask, info[i].is_eof);
        #endif
        if(info[i].status == ITERATE_HANDLE_OPENED){
            fprintf(stderr, "close iterate_handle : %d\n", info[i].handle_id);
            kv_nvme_iterate_close(handle, info[i].handle_id);
        }
      }
  }
}

kv_pair* _malloc_kv_pair(uint8_t klen, uint32_t vlen) {
  // alloc pair
  kv_pair* kv = (kv_pair*)malloc(sizeof(kv_pair));
  if(!kv)
    return NULL;
  kv->keyspace_id = KV_KEYSPACE_IODATA;

  // alloc value
  kv->value.value = NULL;
  if(vlen > 0) {
    kv->value.value = kv_zalloc(vlen);
    if(!kv->value.value) {
      free(kv);
      return NULL;
    }
    kv->value.length = vlen;
    kv->value.offset = 0;
  }

  //alloc key
  kv->key.key = kv_zalloc(klen);
  if(!kv->key.key) {
    kv_free(kv->value.value);
    free(kv);
    return NULL;
  }
  kv->key.length = klen;
  kv->param.private_data = NULL;

  return kv;
}

void _free_kv_pair(kv_pair* kv) {
  if(kv->value.value)
    kv_free(kv->value.value);
  if(kv->key.key)
    kv_free(kv->key.key);
  if(kv)
    free(kv);
}

void _free_kv_iter(kv_iterate* it) {
  if(it){
    if(it->kv.value.value){
       kv_free(it->kv.value.value);
       it->kv.value.value = NULL;
    }
    free(it);
    it = NULL;
  }
}

kv_iterate* _malloc_kv_iter(int iterate_buff_size) {
  kv_iterate* it = (kv_iterate*)malloc(sizeof(kv_iterate));
    if(!it)
      return NULL;
  it->iterator = KV_INVALID_ITERATE_HANDLE;
  it->kv.key.length = 0;
  it->kv.key.key = NULL;

  it->kv.value.value = kv_zalloc(iterate_buff_size);
  if(!it->kv.value.value) {
    free(it);
    return NULL;
  }
  it->kv.value.length = iterate_buff_size;
  it->kv.value.offset = 0;
  memset(it->kv.value.value, 0, it->kv.value.length);
  return it;
}

int perform_insert(uint64_t handle, int qid, unsigned long key_count,
  uint8_t klen, uint32_t vlen) { 
  int ret = FAILED;
  kv_pair *kv = _malloc_kv_pair(klen ,vlen);
  if(!kv) {
    fprintf(stderr, "Malloc resource failed.\n");
    return ret;
  }
  // prepare writing
  kv->param.async_cb = NULL;
  kv->param.io_option.store_option = KV_STORE_DEFAULT;
  
  for(unsigned long i = 0; i < key_count; i++){
    if(!(i % 10000) && i > 0){
      fprintf(stderr, "perform_insert: %ld\n", i);
    }

    snprintf(kv->key.key, klen, "%0*ld", klen - 1, i);
    snprintf(kv->value.value, vlen, "value%ld", i);
    ret = kv_nvme_write(handle, qid, kv);
    if(ret) {
      fprintf(stderr, "Write failed. error code: 0x%x.\n", ret);
      break;
    }
  }
  _free_kv_pair(kv);
  return ret;
}

int perform_iterator(uint64_t handle, int qid, int iterate_count) {
  bitmask = 0xFFFF0000;
  char prefix_str[5] = "0000";
  prefix = 0;
  for (int i = 0; i < 4; i++){
    prefix |= (prefix_str[i] << (3-i)*8);
  }
  _check_iter_info(handle);

  int iterate_buff_size = 32 * 1024;
  //NVMe Iterate_Open
  uint32_t iterator = KV_INVALID_ITERATE_HANDLE;
  uint8_t keyspace_id = KV_KEYSPACE_IODATA;
  fprintf(stderr,"Iterate bitmast: 0x%8x, prefix: 0x%8x.\n", bitmask, prefix);
  iterator = kv_nvme_iterate_open(handle, keyspace_id, bitmask, prefix,
    KV_KEY_ITERATE);
  if (iterator > KV_MAX_ITERATE_HANDLE ||
      iterator == KV_INVALID_ITERATE_HANDLE) {
    fprintf(stderr, "Open iterator failed, error code:0x%x.\n", iterator);
    return FAILED;
  }
  fprintf(stderr, "Iterate_Open Success: iterator id=%d\n", iterator);

  int ret = SUCCESS;
  uint32_t total_cnt = 0;
  kv_iterate* it = _malloc_kv_iter(iterate_buff_size);
  if(!it) {
    kv_nvme_iterate_close(handle, iterator);
    fprintf(stderr, "Malloc resource failed.\n");
    return FAILED; 
  }

  unsigned int num_entries = 0;
  for(unsigned long i = 0; i < iterate_count; i++) {
    memset(it->kv.value.value, 0, it->kv.value.length);
    it->iterator = iterator;
    it->kv.param.async_cb = NULL;
    it->kv.param.private_data = NULL;
    it->kv.param.io_option.iterate_read_option = KV_ITERATE_READ_DEFAULT;
    it->kv.value.length = iterate_buff_size;

    ret = kv_nvme_iterate_read(handle, qid, it);
    if(ret != KV_SUCCESS && ret != KV_ERR_ITERATE_READ_EOF) {
      fprintf(stderr, "Iterator read failed, error code: 0x%x\n", ret);
      break;
    }
    num_entries = *((unsigned int *)it->kv.value.value);
    total_cnt += num_entries;

    #if DEBUG_ON
    fprintf(stderr, "Iterator buff_len=%d, status=%x, key_cnt:%d.\n",
      it->kv.value.length, ret, num_entries);
    if(_print_iter_keys(it->kv.value.value, it->kv.value.length, num_entries)) {
      fprintf(stderr, "Parse keys in iter buffuer failed.\n");
    }
    #endif

    if(ret == KV_ERR_ITERATE_READ_EOF) {
      fprintf(stderr, "Iterator finished, total key cnt retrieved: %d.\n", total_cnt);
      ret = SUCCESS;
      break;
    }
  }

  //NVMe Iterate_Close
  if(ret != SUCCESS) {
    kv_nvme_iterate_close(handle, iterator);
  }else {
    ret = kv_nvme_iterate_close(handle, iterator);
  }

  _free_kv_iter(it);
  return ret;
}

int perform_read(uint64_t handle, int qid, unsigned long key_count,
  uint8_t klen, uint32_t vlen) {
  int ret = FAILED;
  kv_pair *kv = _malloc_kv_pair(klen ,vlen);
  if(!kv) {
    fprintf(stderr, "Malloc resource failed.\n");
    return ret;
  }
  kv->param.async_cb = NULL;
  kv->param.io_option.retrieve_option = KV_RETRIEVE_DEFAULT;

  for(unsigned long i = 0; i < key_count; i++){
    if(!(i % 10000) && i > 0){
      fprintf(stderr, "perform_read: %ld\n", i);
    }
    snprintf(kv->key.key, klen, "%0*ld", klen - 1, i);
    memset(kv->value.value, 0, vlen);
    ret = kv_nvme_read(handle, qid, kv);
    if(ret != KV_SUCCESS && ret != KV_ERR_NOT_EXIST_KEY) {
      fprintf(stderr, "Read key:%s. error code:0x%x.\n", (char*)kv->key.key, ret);
      break;
    }
    else if(ret == KV_ERR_NOT_EXIST_KEY) {
      fprintf(stderr, "Warning! Read key:%s, but not exist in SSD.\n",
        (char*)kv->key.key);
    }
    else {
      #if DEBUG_ON
      fprintf(stderr, "Read key len:%d, key= %s, "
        "val len: %d, actual len: %d, val = %s.\n", kv->key.length,
        (char*)kv->key.key, kv->value.length, kv->value.actual_value_size,
        (char*)kv->value.value);
      #endif
    }
  }
  _free_kv_pair(kv);
  return ret;
}

int perform_delete(uint64_t handle, int qid, unsigned long key_count,
  uint8_t klen) {
  int ret = FAILED;
  kv_pair *kv = _malloc_kv_pair(klen, 0);
  if(!kv) {
    fprintf(stderr, "Malloc resource failed.\n");
    return ret;
  }
  kv->param.async_cb = NULL;
  kv->param.io_option.delete_option = KV_DELETE_CHECK_IDEMPOTENT;

  for(unsigned long i = 0; i < key_count; i++){
    if(!(i % 10000) && i > 0){
      fprintf(stderr, "perform_delete: %ld\n", i);
    }
    snprintf(kv->key.key, klen, "%0*ld", klen - 1 , i);
    ret = kv_nvme_delete(handle, qid, kv);
    if(ret != KV_SUCCESS && ret != KV_ERR_NOT_EXIST_KEY) {
      fprintf(stderr, "Delete key: %s. error code: 0x%x.\n",
        (char*)kv->key.key, ret);
      break;
    }else if(ret == KV_ERR_NOT_EXIST_KEY) {
      fprintf(stderr, "Warning! Delete key:%s, but not exist in SSD.\n",
        (char*)kv->key.key);
    }else {
      #if DEBUG_ON
      fprintf(stderr, "Delete key= %s successfully.\n", (char*)kv->key.key);
      #endif
    }

  }
  _free_kv_pair(kv);
  return ret;
}

int perform_exist(uint64_t handle, int qid, unsigned long key_count,
  uint8_t klen) {
  int ret = FAILED;
  kv_pair *kv = _malloc_kv_pair(klen, 0);
  if(!kv) {
    fprintf(stderr, "Malloc resource failed.\n");
    return ret;
  }
  kv->param.async_cb = NULL;
  kv->param.io_option.exist_option = KV_EXIST_DEFAULT;

  for(unsigned long i = 0; i < key_count; i++){
    if(!(i % 10000) && i > 0){
      fprintf(stderr, "perform_exist: %ld\n", i);
    }
    snprintf(kv->key.key, klen, "%0*ld", klen - 1, i);
    ret = kv_nvme_exist(handle, qid, kv);
    if(ret != KV_SUCCESS && ret != KV_ERR_NOT_EXIST_KEY) {
      fprintf(stderr, "Exist key: %s. error code: 0x%x.\n",
        (char*)kv->key.key, ret);
      break;
    }else if(ret == KV_ERR_NOT_EXIST_KEY) {
      fprintf(stderr, "Exist Key = %s is False.\n",
        (char*)kv->key.key);
    }else {
      #if DEBUG_ON
      fprintf(stderr, "Exist key= %s is True.\n", (char*)kv->key.key);
      #endif
    }
  }
  _free_kv_pair(kv);
  return ret;
}

int perform_sync_io(uint64_t handle, uint8_t op_type,
   unsigned long key_count, uint8_t klen, uint32_t vlen) {
  struct timeval start = {0};
  struct timeval end = {0};
  fprintf(stderr,"Setup App: \n");
  gettimeofday(&start, NULL);
  int ret = SUCCESS;
  int qid = DEFAULT_IO_QUEUE_ID; 
  switch(op_type) {
  case INSERT_OP:
    ret = perform_insert(handle, qid, key_count, klen, vlen);
    break;
  case RETRIEVE_OP:
    ret = perform_read(handle, qid, key_count, klen, vlen);
    break;
  case DELETE_OP:
    ret = perform_delete(handle, qid, key_count, klen);
    break;
  case ITERATOR_OP:
    ret = perform_iterator(handle, qid, key_count);
    break;
  case EXIST_OP:
    ret = perform_exist(handle, qid, key_count, klen);
    break;
  default:
    fprintf(stderr, "Please specify a correct op_type for testing\n");
    ret = FAILED;
  }

  gettimeofday(&end, NULL);
  fprintf(stderr, "App finished, execute result(0:SUCCESS, 1:FAILED): %d.\n", ret);
  show_elapsed_time(&start, &end, "Run result: ", 1, 0, NULL);
  return ret;
}

int main(int argc, char *argv[]) {
  int ret = -EINVAL;
  char* dev_path = NULL;
  unsigned long num_ios = 10;
  int op_type = 1;
  uint8_t klen = 16;
  uint32_t vlen = 4096;
  int c;

  while ((c = getopt(argc, argv, "d:n:o:k:v:h")) != -1) {
    switch(c) {
    case 'd':
      dev_path = optarg;
      break;
    case 'n':
      num_ios = atoi(optarg);
      break;
    case 'o':
      op_type = atoi(optarg);
      break;
    case 'k':
      klen = atoi(optarg);
      break;
    case 'v':
      vlen = atoi(optarg);
      break;
    case 'h':
      usage(argv[0]);
      return ret;
    default:
      usage(argv[0]);
      return ret;
    }
  }
  if(!dev_path) {
    usage(argv[0]);
    return ret;
  }
  uint64_t dev_handle = 0;
  if(_init_test_env(dev_path, &dev_handle) == FAILED)
    return ret;
  kv_nvme_sdk_info();
  ret = perform_sync_io(dev_handle, op_type, num_ios, klen, vlen);

  _deinit_test_env(dev_handle, dev_path);
  return ret;
}

