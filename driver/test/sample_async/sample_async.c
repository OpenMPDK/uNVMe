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
#include <pthread.h>

#include "kv_types.h"
#include "kvnvme.h"

#include "latency_stat.h"
#include "kvutil.h"

#define SUCCESS 0
#define FAILED 1

#define DEBUG_ON 1

#define INSERT_OP (1)
#define RETRIEVE_OP (2)
#define DELETE_OP (3)
#define ITERATOR_OP (4)
#define EXIST_OP (5)
#define BATHCH_STORE_OP (6)


unsigned int miscompare_cnt = 0;
int check_miscompare = 0;
uint32_t prefix = 0;
uint32_t bitmask = 0xFFFF0000;

static pthread_mutex_t mutex;

uint32_t cur_qdepth = 0;
uint32_t completed = 0;
uint32_t read_total_kv_cnt = 0;

void _safe_add(uint32_t *addend, uint32_t increase) {
  pthread_mutex_lock(&mutex);
  *addend = *addend + increase;
  pthread_mutex_unlock(&mutex);
}

void _safe_dec(uint32_t *addend, uint32_t dec) {
  pthread_mutex_lock(&mutex);
  *addend = *addend - dec;
  pthread_mutex_unlock(&mutex);
}

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
  printf("-d      device_path  :  kvssd device path. e.g. 0000:06:00.0\n");
  printf("-n      num_ios      :  total number of ios (ignore this for iterator)\n");
  printf("-q      queue_depth  :  queue depth (ignore this for iterator)\n");
  printf("-o      op_type      :  1: write; 2: read; 3: delete; 4: iterator; 5: check key exist\n");
  printf("-k      klen         :  key length (ignore this for iterator)\n");
  printf("-v      vlen         :  value length (ignore this for iterator)\n");
  printf("==============\n");
}

void udd_write_cb(kv_pair *kv, unsigned int result, unsigned int status) {
  if(status != KV_SUCCESS){
    fprintf(stderr, "ERROR! key=%s option=%d value.length=%d value.offset=%d status code=%d\n",
      (char*)kv->key.key, kv->param.io_option.store_option, kv->value.length,
      kv->value.offset, status);
  }
  else {
    #if DEBUG_ON
    fprintf(stderr, "complete status code = %d, write key= %s, val = %s.\n", status, 
      (char*)kv->key.key, (char*)kv->value.value);
    #endif
  }
  _safe_add(&completed, 1);
  _safe_dec(&cur_qdepth, 1);
}

void udd_read_cb(kv_pair *kv, unsigned int result, unsigned int status) {
  if(status != KV_SUCCESS){
    fprintf(stderr, "ERROR! key=%s option=%d value.length=%d value.offset=%d"
      " status code=0x%2x\n", (char*)kv->key.key,
      kv->param.io_option.retrieve_option, kv->value.length, kv->value.offset,
      status);
  }
  else {
    #if DEBUG_ON
    fprintf(stderr, "complete status code = %d, read key len:%d, key= %s, "
      "val len: %d, actual len: %d, val = %s.\n", status, kv->key.length,
      (char*)kv->key.key, kv->value.length, kv->value.actual_value_size,
      (char*)kv->value.value);
    #endif
  }
  _safe_add(&completed, 1);
  _safe_dec(&cur_qdepth, 1);
}

void udd_delete_cb(kv_pair *kv, unsigned int result, unsigned int status) {
  if(status != KV_SUCCESS){
    fprintf(stderr, "ERROR! key=%s option=%d status code=%d\n",
      (char*)kv->key.key, kv->param.io_option.delete_option, status);
  }
  else {
    #if DEBUG_ON
    fprintf(stderr, "complete status code = %d, delete key= %s.\n", status, 
      (char*)kv->key.key);
    #endif
  }
  _safe_add(&completed, 1);
  _safe_dec(&cur_qdepth, 1);
}

void udd_exist_cb(kv_pair *kv, unsigned int result, unsigned int status) {
  if(status != KV_SUCCESS){
    fprintf(stderr, "ERROR! key=%s option=%d status code=0x%2x\n",
      (char*)kv->key.key, kv->param.io_option.exist_option, status);
  }
  else {
    #if DEBUG_ON
    fprintf(stderr, "complete status code = %d, exist key= %s.\n", status, 
      (char*)kv->key.key);
    #endif
  }
  _safe_add(&completed, 1);
  _safe_dec(&cur_qdepth, 1);
}

void udd_iterate_cb(kv_iterate *it, unsigned int result,
                    unsigned int status) {
  int *is_eof = NULL;
  uint32_t num_entries = 0;
  switch(status) {
  case KV_ERR_ITERATE_READ_EOF:
    is_eof = it->kv.param.private_data;
    *is_eof = 1;
    fprintf(stderr, "complete: Iterator reach end.\n");
  case KV_SUCCESS:
    num_entries = *((unsigned int *)it->kv.value.value);
    _safe_add(&read_total_kv_cnt, num_entries);
    #if DEBUG_ON
    fprintf(stderr, "complete: Iter buff_len=%d, status=%x, key_cnt:%d.\n",
      result, status, num_entries);
    if(_print_iter_keys(it->kv.value.value, result, num_entries)) {
      fprintf(stderr, "Parse keys in iter buffuer failed.\n");
    }
    #endif
    break;
  default:
    fprintf(stderr, "ERROR! iterator result=%d status=%d\n", result, status);
    break;
  }
  _safe_add(&completed, 1);
  _safe_dec(&cur_qdepth, 1);
}

 int _init_test_env(char* dev_path, uint64_t *dev_handle, int maxdepth) {
  int ret = FAILED;
  //NOTE : check pci information first using lspci -v 
  unsigned int ssd_type = KV_TYPE_SSD;
  check_miscompare = 0;

  kv_nvme_io_options options = {0};
  options.core_mask = 1; // Use CPU 0
  options.sync_mask = 0; // Use Async I/O mode
  options.num_cq_threads = 1; // Use only one CQ Processing Thread
  options.cq_thread_mask = 2; // Use CPU 7
  options.queue_depth = maxdepth; // MAX QD
  options.mem_size_mb = 512; // MAX -1

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
  pthread_mutex_init(&mutex, NULL);
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
  pthread_mutex_destroy(&mutex);
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

unsigned long long (*hash_value)[2] = NULL;
unsigned long long seed = 0x0102030405060708LL;
kv_pair** _construct_kv_pairs(int key_count, uint8_t klen, uint32_t vlen) {
  /* 0:NONE, 1: Hash128_1_P128, 2: Hash128_2_P128, 3: MurmurHash3_x64_128 */
  int host_hash = 0;
  if(host_hash == 1){
    check_miscompare = 1;
  }
  int fd = 0;
  long int i = 0;

  kv_pair** kv = (kv_pair**)malloc(sizeof(kv_pair*) * key_count);
  hash_value = (unsigned long long(*)[2])malloc(
    sizeof(unsigned long long [2]) * key_count);
  if(!kv || !hash_value){
    goto free_memory;
  }

  // fprintf(stderr, "klen= %d vlen= %d key_count= %d\n", klen,  vlen, key_count);
  fd = open("/dev/srandom", O_RDONLY);
  if (fd < 3){
    fd = open("/dev/urandom", O_RDONLY);
  }

  // alloc  Memory 
  for(i = 0;i < key_count; i++){
    if(!(i % 10000) && i > 0){
      fprintf(stderr, "Allocated kv pairs num: %ld .\n", i);
    }
    // alloc pair
    kv[i] = (kv_pair*)malloc(sizeof(kv_pair));
    if(!kv[i])
      goto free_memory;
    kv[i]->keyspace_id = KV_KEYSPACE_IODATA;
    kv[i]->value.value = kv_zalloc(vlen);
    if(!kv[i]->value.value)
      goto free_memory;

    snprintf(kv[i]->value.value, vlen, "value%ld", i);
    kv[i]->value.length = vlen;
    kv[i]->value.offset = 0;

    if(check_miscompare){
      read(fd, kv[i]->value.value, vlen);
      Hash128_2_P128(kv[i]->value.value, vlen, seed, hash_value[i]);
    }
    //alloc key
    int key_buffer_size = klen;
    if(klen%4){
      key_buffer_size += (4 - klen%4);
    }
    kv[i]->key.key = kv_zalloc(key_buffer_size);
    if(!kv[i]->key.key)
      goto free_memory;

    kv[i]->key.length = klen;
    if (host_hash == 0) {
      snprintf(kv[i]->key.key, klen, "%0*ld", klen - 1, i);
    } else {
      memcpy(kv[i]->key.key, hash_value[i], klen);
    }
    kv[i]->param.private_data = NULL;
  }

  bitmask = 0xFFFF0000;
  char prefix_str[5] = "0000";
  prefix = 0;
  for (int i = 0; i < 4; i++){
    prefix |= (prefix_str[i] << (3-i)*8);
  }

  close(fd);
  return kv;

free_memory:
  for(int idx = 0; idx < i; idx++){
    kv_free(kv[i]->key.key);
    kv[i]->key.key = NULL;
    kv_free(kv[i]->value.value);
    kv[i]->value.value = NULL;
    free(kv[i]);
    kv[i] = NULL;
  }
  if(!kv || !hash_value){
    if(kv){
      free(kv);
      kv = NULL;
    }
    if(hash_value){
      free(hash_value);
      hash_value = NULL;
    }
  }
  if(fd != 0)
    close(fd);
  return NULL;
}

void _free_kv_pairs(int key_count, kv_pair** kv) {
  //Teardown Memory
  for(int i = 0;i < key_count; i++){
    if(!(i % 10000) && i > 0){
      fprintf(stderr, "free kv pairs, pair index: %d. \n", i);
    }
    if(kv[i]->value.value) kv_free(kv[i]->value.value);
    if(kv[i]->key.key) kv_free(kv[i]->key.key);
    if(kv[i]) free(kv[i]);
  }
  if(kv) free(kv);
  if(hash_value) free(hash_value);
}

void _free_kv_iter(kv_iterate** it, int iterate_count) {
  if(it){
    for(int i = 0;i < iterate_count; i++){
      if(it[i] != NULL && it[i]->kv.value.value != NULL){
        kv_free(it[i]->kv.value.value);
        it[i]->kv.value.value = NULL;
      }
      if(it[i]){
        free(it[i]);
        it[i] = NULL;
      }
    }
    free(it);
    it = NULL;
  }
}

kv_iterate** _malloc_kv_iter(int iterate_count, int iterate_read_size) {
  kv_iterate** it = (kv_iterate**)malloc(sizeof(kv_iterate*) * iterate_count);
  if(!it){
    return NULL;
  }
  int i = 0;
  for(i = 0; i < iterate_count; i++){
    it[i] = (kv_iterate*)malloc(sizeof(kv_iterate));
    if(!it[i])
      goto exit;
    it[i]->iterator = KV_INVALID_ITERATE_HANDLE;

    it[i]->kv.key.length = 0;
    it[i]->kv.key.key = NULL;

    it[i]->kv.value.value = kv_zalloc(iterate_read_size);
    if(!it[i]->kv.value.value)
      goto exit;
    it[i]->kv.value.length = iterate_read_size;
    it[i]->kv.value.offset = 0;
    memset(it[i]->kv.value.value, 0, it[i]->kv.value.length);
  }
  return it;

exit:
  _free_kv_iter(it, i);
  return NULL;
}

typedef int (*driver_io_func_t)(uint64_t handle, int qid, kv_pair* kv);
int _perform_io_process(const char *io_name, driver_io_func_t io_func,
  uint64_t handle, int qid, int qdepth, int count, kv_pair **kv){
  cur_qdepth = 0;
  int ret = SUCCESS;
  unsigned int submit_cnt = 0;
  while (submit_cnt < count) {
    while (cur_qdepth < qdepth) {
      if (submit_cnt >= count)
        break;
      if(!(submit_cnt % 1000) && submit_cnt > 0){
        fprintf(stderr, "Send %s count:%d. \n", io_name, submit_cnt);
      }
      // fprintf(stderr, "Send %s count:%d. \n", io_name, submit_cnt);
      ret = io_func(handle, qid, kv[submit_cnt]);
      if(ret != KV_SUCCESS) {
        fprintf(stderr, "%s failed, error code: 0x%x\n", io_name, ret);
        break;
      }
      cur_qdepth++;
      submit_cnt++;
    }
    if(ret != KV_SUCCESS) {
      fprintf(stderr, "%s failed, error code: 0x%x\n", io_name, ret);
      break;
    }
    if (cur_qdepth == qdepth) {
      usleep(1);
    }
  }

  printf("Total submited %s num:%d.\n", io_name, submit_cnt);
  while(completed < submit_cnt) {
    usleep(1);
  }
  return ret;
}

int perform_insert(uint64_t handle, int qid, int qdepth, int key_count,
                   kv_pair** kv) {
  // prepare writing
  for (int i = 0; i < key_count; i++) {
    kv[i]->param.async_cb = udd_write_cb;
    kv[i]->param.io_option.store_option = KV_STORE_DEFAULT;
  }
  // NVME Write
  int ret = _perform_io_process("Insert", kv_nvme_write_async, handle, qid,
                                 qdepth, key_count, kv);
  return ret;
}

int perform_iterator(uint64_t handle, int qid, int qdepth,
                     int iterate_count) {
  int iterate_read_size = 32 * 1024;
  kv_iterate** it = _malloc_kv_iter(iterate_count, iterate_read_size);
  if(!it) {
    fprintf(stderr, "Malloc Failed.\n");
    return FAILED; 
  }
  _check_iter_info(handle);

  //NVMe Iterate_Open
  uint32_t iterator = KV_INVALID_ITERATE_HANDLE;
  uint8_t keyspace_id = KV_KEYSPACE_IODATA;
  fprintf(stderr,"Iterate bitmast: 0x%8x, prefix: 0x%8x.\n", bitmask, prefix);
  iterator = kv_nvme_iterate_open(handle, keyspace_id, bitmask, prefix,
                                  KV_KEY_ITERATE);
  if (iterator > KV_MAX_ITERATE_HANDLE ||
      iterator == KV_INVALID_ITERATE_HANDLE) {
    fprintf(stderr, "Open iterator failed, error code:0x%x.\n", iterator);
    _free_kv_iter(it, iterator);
    return FAILED;
  }
  fprintf(stderr,"Iterate_Open Success: iterator id=%d\n", iterator);

  int ret = SUCCESS;
  unsigned int submit_cnt = 0;
  completed = 0;
  read_total_kv_cnt = 0;

  //Prepare Iterate_Read
  int is_eof = 0;
  for(int i = 0; i < iterate_count; i++){
    memset(it[i]->kv.value.value, 0, it[i]->kv.value.length);
    it[i]->iterator = iterator;
    it[i]->kv.param.async_cb = udd_iterate_cb;
    it[i]->kv.param.private_data = &is_eof;
    it[i]->kv.param.io_option.iterate_read_option = KV_ITERATE_READ_DEFAULT;
  }

  while (submit_cnt < iterate_count) {
    while (cur_qdepth < qdepth) {
      if (submit_cnt >= iterate_count || is_eof)
        break;
      if(!(submit_cnt % 1000) && submit_cnt > 0){
        fprintf(stderr, "Send iterator read seq:%d. \n", submit_cnt);
      }
      ret = kv_nvme_iterate_read_async(handle, qid, it[submit_cnt]);
      if(ret != KV_SUCCESS) {
        fprintf(stderr, "Iterator read failed, error code: 0x%x\n", ret);
        break;
      }

      cur_qdepth++;
      submit_cnt++;
      while(completed < submit_cnt) {
        usleep(1);
      }
    }
    if(is_eof || ret != KV_SUCCESS)
      break;
  }

  // printf("Total submited iterator read num: %d.\n", submit_cnt);
  printf("Total submited iterator cnt: %d, total read keys cnt:%d.\n",
         submit_cnt, read_total_kv_cnt);

  //NVMe Iterate_Close
  if(ret != SUCCESS) {
    kv_nvme_iterate_close(handle, iterator);
  }else {
    ret = kv_nvme_iterate_close(handle, iterator);
  }

  _free_kv_iter(it, iterate_count);
  return ret;
}

int perform_read(uint64_t handle, int qid, int qdepth, int key_count,
                 kv_pair** kv){
  //Prepare reading
  for(int i = 0; i < key_count; i++){
    memset(kv[i]->value.value, 0, kv[i]->value.length);
    kv[i]->param.async_cb = udd_read_cb;
    kv[i]->param.io_option.retrieve_option = KV_RETRIEVE_DEFAULT;
  }
  int ret = _perform_io_process("Read", kv_nvme_read_async, handle, qid,
                                 qdepth, key_count, kv);

  if(check_miscompare){
    for (int i = 0; i < key_count; i++) {
      unsigned long long tmp_hash[2];
      Hash128_2_P128(kv[i]->value.value, kv[i]->value.length, seed, tmp_hash);
      if (memcmp((void*)tmp_hash, (void*)hash_value[i],sizeof(tmp_hash)))
        miscompare_cnt++;
    }
    fprintf(stderr,"Miscompare count: %u\n", miscompare_cnt);
  }
  return ret;
}

int perform_delete(uint64_t handle, int qid, int qdepth, int key_count,
                   kv_pair** kv) {
  //Prepare deleting
  for(int i = 0; i < key_count; i++){
    memset(kv[i]->value.value, 0, kv[i]->value.length);
    kv[i]->param.async_cb = udd_delete_cb;
    kv[i]->param.io_option.delete_option = KV_DELETE_CHECK_IDEMPOTENT;
  }
  int ret = _perform_io_process("Delete", kv_nvme_delete_async, handle, qid,
                                 qdepth, key_count, kv);
  return ret;
}

int perform_exist(uint64_t handle, int qid, int qdepth, int key_count,
                   kv_pair** kv) {
  //Prepare deleting
  for(int i = 0; i < key_count; i++){
    memset(kv[i]->value.value, 0, kv[i]->value.length);
    kv[i]->param.async_cb = udd_exist_cb;
    kv[i]->param.io_option.exist_option = KV_EXIST_DEFAULT;
  }
  int ret = _perform_io_process("Exist", kv_nvme_exist_async, handle, qid,
                                 qdepth, key_count, kv);
  return ret;
}

int perform_io(uint64_t handle, uint8_t op_type, int key_count, int qdepth,
               uint8_t klen, uint32_t vlen) {
  struct timeval start = {0};
  struct timeval end = {0};
  fprintf(stderr, "Setup App: \n");
  gettimeofday(&start, NULL);
  int ret = SUCCESS;
  int qid = DEFAULT_IO_QUEUE_ID; 
  kv_pair** kv= _construct_kv_pairs(key_count, klen, vlen);
  switch(op_type) {
  case INSERT_OP:
    ret = perform_insert(handle, qid, qdepth, key_count, kv);
    break;
  case RETRIEVE_OP:
    ret = perform_read(handle, qid, qdepth, key_count, kv);
    break;
  case DELETE_OP:
    ret = perform_delete(handle, qid, qdepth, key_count, kv);
    break;
  case ITERATOR_OP:
    ret = perform_iterator(handle, qid, qdepth, key_count);
    break;
  case EXIST_OP:
    ret = perform_exist(handle, qid, qdepth, key_count, kv);
    break;
  default:
    fprintf(stderr, "Please specify a correct op_type for testing\n");
    ret = FAILED;
  }
  _free_kv_pairs(key_count, kv);

  gettimeofday(&end, NULL);
  fprintf(stderr, "App finished, execute result(0:SUCCESS, 1:FAILED): %d.\n", ret);
  show_elapsed_time(&start, &end, "Run result: ", 1, 0, NULL);
  return ret;
}

int main(int argc, char *argv[]) {
  int ret = -EINVAL;
  char* dev_path = NULL;
  int num_ios = 10;
  int qdepth = 256;
  int op_type = 1;
  uint8_t klen = 16;
  uint32_t vlen = 4096;
  int c;

  while ((c = getopt(argc, argv, "d:n:q:o:k:v:h")) != -1) {
    switch(c) {
    case 'd':
      dev_path = optarg;
      break;
    case 'n':
      num_ios = atoi(optarg);
      break;
    case 'q':
      qdepth = atoi(optarg);
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
  if(_init_test_env(dev_path, &dev_handle, qdepth) == FAILED)
    return ret;
  kv_nvme_sdk_info();
  ret = perform_io(dev_handle, op_type, num_ios, qdepth, klen, vlen);

  _deinit_test_env(dev_handle, dev_path);
  return ret;
}

