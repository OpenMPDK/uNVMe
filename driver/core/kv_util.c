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

#include "kv_driver.h"
#include "kv_version.h"

void *kv_alloc(unsigned long long size) {
        void *ptr = NULL;

        ptr = spdk_dma_malloc(size, 0, NULL);

        if(!ptr) {
                KVNVME_ERR("Could not allocate the requested memory of size: %lld bytes", size);

                LEAVE();
                return NULL;
        }

        return ptr;
}

void *kv_zalloc(unsigned long long size) {
        void *ptr = NULL;

        ptr = spdk_dma_zmalloc(size, 0, NULL);

        if(!ptr) {
                KVNVME_ERR("Could not allocate the requested memory of size: %lld bytes", size);

                LEAVE();
                return NULL;
        }

        return ptr;
}

void *kv_alloc_socket(unsigned long long size, int socket_id) {
  void* ptr = NULL;
  if(socket_id < 0 && socket_id != SOCKET_ID_ANY)
    return NULL;
  ptr = spdk_dma_malloc_socket(size, 0, NULL, socket_id);
  if(!ptr) {
    KVNVME_ERR("Could not allocate the requested memory of size: %lld bytes", size);
    return NULL;
  }
  return ptr;
}

void *kv_zalloc_socket(unsigned long long size, int socket_id) {
  if(socket_id < 0 && socket_id != SOCKET_ID_ANY)
    return NULL;
  void* ptr = NULL;
  ptr = spdk_dma_zmalloc_socket(size, 0, NULL, socket_id);
  if(!ptr) {
      KVNVME_ERR("Could not allocate the requested memory of size: %lld bytes", size);

      LEAVE();
      return NULL;
  }

  return ptr;
}

void kv_free(void *ptr) {
	spdk_dma_free(ptr);
}


void kv_nvme_sdk_info(void){
        fprintf(stderr, "KV SDK info: buildtime=%s, hash=%s, os=%s, kernel=%s, processor=%s dpdk_version=%s spdk_version=%s\n",
		buildtime,hash,os,kernel,processor,dpdk_version,spdk_version);
}

