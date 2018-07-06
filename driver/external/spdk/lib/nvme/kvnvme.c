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

#include "nvme_internal.h"
#include "spdk/kvnvme_spdk.h"
#include "kv_types.h"

static struct nvme_request *
_nvme_kv_cmd_allocate_request(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
		const struct nvme_payload *payload,
		uint32_t buffer_size, 
		uint32_t payload_offset, uint32_t md_offset,
		spdk_nvme_cmd_cb cb_fn, void *cb_arg, uint32_t opc, uint32_t io_flags, uint32_t keyspace_id);

/*
 * Setup store request
 */
static void
_nvme_kv_cmd_setup_store_request(struct spdk_nvme_ns *ns, struct nvme_request *req,
			   uint32_t key_size,
			   uint32_t buffer_size, 
			   uint32_t offset,
			   uint32_t io_flags, uint32_t option)
{
	struct spdk_nvme_cmd	*cmd;

	cmd = &req->cmd;
	cmd->cdw10 = buffer_size / 4; // In DWORDs

	// cdw11:
	// 2017.10.25 : for large value append / retrieve
	// [0:7] key_size -1
	// [8:15] option
        //cmd->cdw11 = key_size-1;
	cmd->cdw11 = ((uint32_t)((option&0xFF)<<8)|((key_size-1)&0xFF));

	//
	// Filling key value into (cdw10-13) in case key size is small than 16.
        // If md is set, lower layer (nvme_pcie_qpair_build_contig_request())
	// will prepare PRP and fill into (cdw10-11).
	// 
	if (key_size <= KV_MAX_EMBED_KEY_SIZE) {
		memcpy((uint8_t*)&cmd->cdw12, req->payload.md + req->md_offset, key_size);
		req->payload.md = NULL;  
	}
	else{
		//configure key prp1
		void* key_prp1_vaddr = req->payload.md + req->md_offset;
		uint64_t key_prp1_paddr = spdk_vtophys(key_prp1_vaddr);
		if(key_prp1_paddr == SPDK_VTOPHYS_ERROR) {
			SPDK_ERRLOG("invalid key prp1_vaddr=%016llx\n", (long long unsigned int)key_prp1_vaddr);
			return;
		}
		memcpy((uint8_t*)&cmd->cdw12, (void*)key_prp1_paddr, 8);
		cmd->cdw12 = key_prp1_paddr;
		cmd->cdw13 = key_prp1_paddr>>32;
		//SPDK_NOTICELOG("key_prp1_vaddr=%016llx paddr=%016llx\n", key_prp1_vaddr, key_prp1_paddr);

		//configure key prp2
		uint32_t unaligned = (uint64_t)key_prp1_vaddr & (PAGE_SIZE -1);
		//SPDK_NOTICELOG("key_size=%d unaligned = %d remained= %d\n",key_size, unaligned, PAGE_SIZE - unaligned);
		if(key_size > PAGE_SIZE - unaligned){
			void* key_prp2_vaddr = key_prp1_vaddr + PAGE_SIZE - unaligned;
			uint64_t key_prp2_paddr = spdk_vtophys(key_prp2_vaddr);
			if(key_prp2_paddr == SPDK_VTOPHYS_ERROR) {
				SPDK_ERRLOG("invalid key prp2_vaddr=%016llx\n", (long long unsigned int)key_prp2_vaddr);
				return;
			}
			cmd->cdw14 = key_prp2_paddr;
			cmd->cdw15 = key_prp2_paddr>>32;
			//SPDK_NOTICELOG("key_prp2_vaddr=%016llx paddr=%016llx\n", key_prp2_vaddr, key_prp2_paddr);
		}
		req->payload.md = NULL;
	}
	// cdw5:
	// 2017.10.25 : for large value append / retrieve
	//    [0:31] The offset of value in bytes
	// MPTR: CDW4-5
        //    To minimize the modification to original SPDK code, still use mptr here
	cmd->mptr = (uint64_t)offset;
	//cmd->mptr= ((uint64_t)((offset<<8)|(option&0xFF)));
}

/*
 * Setup retrieve request
 */
static void
_nvme_kv_cmd_setup_retrieve_request(struct spdk_nvme_ns *ns, struct nvme_request *req,
                           uint32_t key_size, uint32_t buffer_size,
			   uint32_t offset,
                           uint32_t io_flags, uint32_t option)
{
        struct spdk_nvme_cmd    *cmd;

        cmd = &req->cmd;
        cmd->cdw10 = buffer_size / 4; // In DWORDs

	// cdw11:
	// 2017.10.25 : for large value append / retrieve
	// [0:7] key_size -1
	// [8:15] option
        //cmd->cdw11 = key_size-1;
	cmd->cdw11 = ((uint32_t)((option&0xFF)<<8)|((key_size-1)&0xFF));

        //
	// Filling key value into (cdw10-13) in case key size is small than 16.
        // If md is set, lower layer (nvme_pcie_qpair_build_contig_request())
        // will prepare PRP and fill into (cdw10-11).
        //
        if (key_size <= KV_MAX_EMBED_KEY_SIZE) {
                memcpy((uint8_t*)&cmd->cdw12, req->payload.md + req->md_offset, key_size);
                req->payload.md = NULL;
        }
	else{
		//configure key prp1
		void* key_prp1_vaddr = req->payload.md + req->md_offset;
		uint64_t key_prp1_paddr = spdk_vtophys(key_prp1_vaddr);
		if(key_prp1_paddr == SPDK_VTOPHYS_ERROR) {
			SPDK_ERRLOG("invalid key prp1_vaddr=%016llx\n", (long long unsigned int)key_prp1_vaddr);
			return;
		}
		memcpy((uint8_t*)&cmd->cdw12, (void*)key_prp1_paddr, 8);
		cmd->cdw12 = key_prp1_paddr;
		cmd->cdw13 = key_prp1_paddr>>32;
		//SPDK_NOTICELOG("key_prp1_vaddr=%016llx paddr=%016llx\n", key_prp1_vaddr, key_prp1_paddr);

		//configure key prp2
		uint32_t unaligned = (uint64_t)key_prp1_vaddr & (PAGE_SIZE -1);
		//SPDK_NOTICELOG("key_size=%d unaligned = %d remained= %d\n",key_size, unaligned, PAGE_SIZE - unaligned);
		if(key_size > PAGE_SIZE - unaligned){
			void* key_prp2_vaddr = key_prp1_vaddr + PAGE_SIZE - unaligned;
			uint64_t key_prp2_paddr = spdk_vtophys(key_prp2_vaddr);
			if(key_prp2_paddr == SPDK_VTOPHYS_ERROR) {
				SPDK_ERRLOG("invalid key prp2_vaddr=%016llx\n", (long long unsigned int)key_prp2_vaddr);
				return;
			}
			cmd->cdw14 = key_prp2_paddr;
			cmd->cdw15 = key_prp2_paddr>>32;
			//SPDK_NOTICELOG("key_prp2_vaddr=%016llx paddr=%016llx\n", key_prp2_vaddr, key_prp2_paddr);
		}
		req->payload.md = NULL;
	}
	// cdw5:
	// 2017.10.25 : for large value append / retrieve / delete
	//    [8:31] The offset of value in bytes
	// MPTR: CDW4-5
        //    To minimize the modification to original SPDK code, still use mptr here
	cmd->mptr = (uint64_t)offset;
	//cmd->mptr= ((uint64_t)((offset<<8)|(option&0xFF)));

}

/*
 */             
static void     
_nvme_kv_cmd_setup_delete_request(struct spdk_nvme_ns *ns, struct nvme_request *req,
                           uint32_t key_size, uint32_t buffer_size,
			   uint32_t offset,
                           uint32_t io_flags, uint32_t option)
{
        struct spdk_nvme_cmd    *cmd;

        cmd = &req->cmd;
	cmd->cdw10 = 0;

	// cdw11:
	// [0:7] key_size -1
	// [8:15] option
	cmd->cdw11 = ((uint32_t)((option&0xFF)<<8)|((key_size-1)&0xFF));

        //
        // Filling key value into cdw10-13 in case key size is small than 16.
        // If md is set, lower layer (nvme_pcie_qpair_build_contig_request())
        // will prepare PRP and fill into (cdw10-11).
        //
        if (key_size <= KV_MAX_EMBED_KEY_SIZE) {
                memcpy((uint8_t*)&cmd->cdw12, req->payload.md + req->md_offset, key_size);
                req->payload.md = NULL;
        }
	else{
		//configure key prp1
		void* key_prp1_vaddr = req->payload.md + req->md_offset;
		uint64_t key_prp1_paddr = spdk_vtophys(key_prp1_vaddr);
		if(key_prp1_paddr == SPDK_VTOPHYS_ERROR) {
			SPDK_ERRLOG("invalid key prp1_vaddr=%016llx\n", (long long unsigned int)key_prp1_vaddr);
			return;
		}
		memcpy((uint8_t*)&cmd->cdw12, (void*)key_prp1_paddr, 8);
		cmd->cdw12 = key_prp1_paddr;
		cmd->cdw13 = key_prp1_paddr>>32;
		//SPDK_NOTICELOG("key_prp1_vaddr=%016llx paddr=%016llx\n", key_prp1_vaddr, key_prp1_paddr);

		//configure key prp2
		uint32_t unaligned = (uint64_t)key_prp1_vaddr & (PAGE_SIZE -1);
		//SPDK_NOTICELOG("key_size=%d unaligned = %d remained= %d\n",key_size, unaligned, PAGE_SIZE - unaligned);
		if(key_size > PAGE_SIZE - unaligned){
			void* key_prp2_vaddr = key_prp1_vaddr + PAGE_SIZE - unaligned;
			uint64_t key_prp2_paddr = spdk_vtophys(key_prp2_vaddr);
			if(key_prp2_paddr == SPDK_VTOPHYS_ERROR) {
				SPDK_ERRLOG("invalid key prp2_vaddr=%016llx\n", (long long unsigned int)key_prp2_vaddr);
				return;
			}
			cmd->cdw14 = key_prp2_paddr;
			cmd->cdw15 = key_prp2_paddr>>32;
			//SPDK_NOTICELOG("key_prp2_vaddr=%016llx paddr=%016llx\n", key_prp2_vaddr, key_prp2_paddr);
		}
		req->payload.md = NULL;
	}

	// cdw5:
	//    [8:31] The offset of value in bytes
	// MPTR: CDW4-5
        //    To minimize the modification to original SPDK code, still use mptr here
	cmd->mptr = (uint64_t)0;
	//cmd->mptr = (uint64_t)offset;
}


/*
 */
static void    
_nvme_kv_cmd_setup_exist_request(struct spdk_nvme_ns *ns, struct nvme_request *req,
                           uint32_t key_size, 
                           uint32_t io_flags, uint32_t option)
{
        struct spdk_nvme_cmd    *cmd;

        cmd = &req->cmd;
	cmd->cdw10 = 0;

	// cdw11:
	// [0:7] key_size -1
	// [8:15] option
	cmd->cdw11 = ((uint32_t)((option&0xFF)<<8)|((key_size-1)&0xFF));

        //
        // Filling key value into cdw10-13 in case key size is small than 16.
        // If md is set, lower layer (nvme_pcie_qpair_build_contig_request())
        // will prepare PRP and fill into (cdw10-11).
        //
        if (key_size <= KV_MAX_EMBED_KEY_SIZE) {
                memcpy((uint8_t*)&cmd->cdw12, req->payload.md + req->md_offset, key_size);
                req->payload.md = NULL;
        }
	else{
		//configure key prp1
		void* key_prp1_vaddr = req->payload.md + req->md_offset;
		uint64_t key_prp1_paddr = spdk_vtophys(key_prp1_vaddr);
		if(key_prp1_paddr == SPDK_VTOPHYS_ERROR) {
			SPDK_ERRLOG("invalid key prp1_vaddr=%016llx\n", (long long unsigned int)key_prp1_vaddr);
			return;
		}
		memcpy((uint8_t*)&cmd->cdw12, (void*)key_prp1_paddr, 8);
		cmd->cdw12 = key_prp1_paddr;
		cmd->cdw13 = key_prp1_paddr>>32;
		//SPDK_NOTICELOG("key_prp1_vaddr=%016llx paddr=%016llx\n", key_prp1_vaddr, key_prp1_paddr);

		//configure key prp2
		uint32_t unaligned = (uint64_t)key_prp1_vaddr & (PAGE_SIZE -1);
		//SPDK_NOTICELOG("key_size=%d unaligned = %d remained= %d\n",key_size, unaligned, PAGE_SIZE - unaligned);
		if(key_size > PAGE_SIZE - unaligned){
			void* key_prp2_vaddr = key_prp1_vaddr + PAGE_SIZE - unaligned;
			uint64_t key_prp2_paddr = spdk_vtophys(key_prp2_vaddr);
			if(key_prp2_paddr == SPDK_VTOPHYS_ERROR) {
				SPDK_ERRLOG("invalid key prp2_vaddr=%016llx\n", (long long unsigned int)key_prp2_vaddr);
				return;
			}
			cmd->cdw14 = key_prp2_paddr;
			cmd->cdw15 = key_prp2_paddr>>32;
			//SPDK_NOTICELOG("key_prp2_vaddr=%016llx paddr=%016llx\n", key_prp2_vaddr, key_prp2_paddr);
		}
		req->payload.md = NULL;
	}

	// cdw5:
	//    [8:31] The offset of value in bytes
	// MPTR: CDW4-5
        //    To minimize the modification to original SPDK code, still use mptr here
	cmd->mptr = (uint64_t)0;
}

/*
 */
static void
_nvme_kv_cmd_setup_iterate_read_request(struct spdk_nvme_ns *ns, struct nvme_request *req,
			   uint32_t iterator,
			   void*  buffer,
			   uint32_t buffer_size,
                           uint32_t buffer_offset,
                           uint32_t io_flags, uint32_t option)
{
        struct spdk_nvme_cmd    *cmd;

        cmd = &req->cmd;
	cmd->cdw11 = ((uint64_t)((option&0xFF)<<8)|((iterator)&0xFF));
        cmd->cdw10 = buffer_size / 4;

	//TODO COMMAND Composion
	// cdw5:
	//    [8:31] Reserved
	//    [0:7]  Exist Options
	// MPTR: CDW4-5
        //    To minimize the modification to original SPDK code, still use mptr here
	cmd->mptr= 0;
}

/*
 */
static void
_nvme_kv_cmd_setup_iterate_open_request(struct spdk_nvme_ns *ns, struct nvme_request *req,
			   uint32_t bitmask, 
                           uint32_t prefix,
                           uint32_t io_flags, uint32_t option)
{
        struct spdk_nvme_cmd    *cmd;

        cmd = &req->cmd;
        cmd->cdw13 = bitmask;
        cmd->cdw12 = prefix;
	cmd->cdw11 = ((uint64_t)((option&0xFF)<<8));
        cmd->cdw10 = 0;

	//TODO COMMAND Composion
	// cdw5:
	//    [8:31] Reserved
	//    [0:7]  Exist Options
	// MPTR: CDW4-5
        //    To minimize the modification to original SPDK code, still use mptr here
	cmd->mptr= 0;
}

/*
 */
static void
_nvme_kv_cmd_setup_iterate_close_request(struct spdk_nvme_ns *ns, struct nvme_request *req,
			   uint8_t iterator,
                           uint32_t io_flags, uint32_t option)
{
        struct spdk_nvme_cmd    *cmd;

        cmd = &req->cmd;
        cmd->cdw13 = 0;
        cmd->cdw12 = 0;
	cmd->cdw11 = ((uint64_t)((option&0xFF)<<8)|((iterator)&0xFF));
        cmd->cdw10 = 0;

	//TODO COMMAND Composion
	// cdw5:
	//    [8:31] Reserved
	//    [0:7]  Exist Options
	// MPTR: CDW4-5
        //    To minimize the modification to original SPDK code, still use mptr here
	cmd->mptr= 0;
}

/*
 * _nvme_kv_cmd_allocate_request
 *   Allocate request and fill payload/metadata.
 *   We use metadata/payload in different ways in different commands.
 *                         metadata         payload
 *   Store/Retrieve        key              value
 *   Delete                key              N/A
 *   Exist                 result list      key array 
 *   Iterate               N/A              key list
 */
static struct nvme_request *
_nvme_kv_cmd_allocate_request(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
		const struct nvme_payload *payload,
		uint32_t buffer_size, uint32_t payload_offset, uint32_t md_offset,
		spdk_nvme_cmd_cb cb_fn, void *cb_arg, uint32_t opc, uint32_t io_flags, uint32_t keyspace_id)
{
	struct nvme_request	*req;
        struct spdk_nvme_cmd    *cmd;

	if (io_flags & 0xFFFF) {
		/* We dont support any ioflag so far */
		return NULL;
	}

	req = nvme_allocate_request(qpair, payload, buffer_size, cb_fn, cb_arg);
	if (req == NULL) {
		return NULL;
	}

        cmd = &req->cmd;
        cmd->opc = opc;
        cmd->nsid = keyspace_id;

	req->payload_offset = payload_offset;
	req->md_offset = md_offset;

	return req;
}

/**
 * \brief Submits a KV Store I/O to the specified NVMe namespace.
 *
 * \param ns NVMe namespace to submit the KV Store I/O
 * \param qpair I/O queue pair to submit the request
 * \param keyspace_id namespace id of key
 * \param key virtual address pointer to the value
 * \param key_length length (in bytes) of the key
 * \param buffer virtual address pointer to the value
 * \param buffer_length length (in bytes) of the value
 * \param offset offset of value (in bytes) 
 * \param cb_fn callback function to invoke when the I/O is completed
 * \param cb_arg argument to pass to the callback function
 * \param io_flags set flags, defined by the SPDK_NVME_IO_FLAGS_* entries
 *                      in spdk/nvme_spec.h, for this I/O.
 * \param option option to pass to NVMe command 
 *          default = 0, compression = 1, idempotent = 2
 * \param is_store store=troe or append=false
	    SPDK_NVME_OPC_KV_STORE(0x81),  SPDK_NVME_OPC_KV_APPEND(0x83)
 * \return 0 if successfully submitted, KV_ERR_DD_NO_AVAILABLE_RESOURCE if an nvme_request
 *           structure cannot be allocated for the I/O request, KV_ERR_DD_INVALID_PARAM if
 *           key_length or buffer_length is too large.
 *
 * The command is submitted to a qpair allocated by spdk_nvme_ctrlr_alloc_io_qpair().
 * The user must ensure that only one thread submits I/O on a given qpair at any given time.
 */
int
spdk_nvme_kv_cmd_store(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
			      uint32_t keyspace_id, void *key, uint32_t key_length,
			      void *buffer, uint32_t buffer_length,
			      uint32_t offset,
			      spdk_nvme_cmd_cb cb_fn, void *cb_arg,
		              uint32_t io_flags,
			      uint8_t  option, uint8_t is_store)
{
	int ret = KV_SUCCESS;
	struct nvme_request *req;
	struct nvme_payload payload;
	
	if (key_length > KV_MAX_KEY_SIZE) return KV_ERR_DD_INVALID_PARAM;
	if (buffer_length > KV_MAX_VALUE_SIZE) return KV_ERR_DD_INVALID_PARAM;

	payload.type = NVME_PAYLOAD_TYPE_CONTIG;
	payload.u.contig = buffer;
	payload.md = key;

	req = _nvme_kv_cmd_allocate_request(ns, qpair, &payload, buffer_length,
                             0, 0, cb_fn, cb_arg, (is_store) ? SPDK_NVME_OPC_KV_STORE : SPDK_NVME_OPC_KV_APPEND,
                             io_flags, keyspace_id);

	if (NULL == req) {
		return KV_ERR_DD_NO_AVAILABLE_RESOURCE;
	}

	_nvme_kv_cmd_setup_store_request(ns, req,
			   key_length, buffer_length, 
			   offset, io_flags, option);

	ret = nvme_qpair_submit_request(qpair, req);
	if(ret != KV_SUCCESS){
		ret = KV_ERR_DD_NO_AVAILABLE_QUEUE;
	}
	return ret;
}

/**
 * \brief Submits a KV Retrieve I/O to the specified NVMe namespace.
 *
 * \param ns NVMe namespace to submit the KV Retrieve I/O
 * \param qpair I/O queue pair to submit the request
 * \param keyspace_id namespace id of key
 * \param key virtual address pointer to the value
 * \param key_length length (in bytes) of the key
 * \param buffer virtual address pointer to the value
 * \param buffer_length length (in bytes) of the value
 * \param offset offset of value (in bytes) 
 * \param cb_fn callback function to invoke when the I/O is completed
 * \param cb_arg argument to pass to the callback function
 * \param io_flags set flags, defined by the SPDK_NVME_IO_FLAGS_* entries
 *                      in spdk/nvme_spec.h, for this I/O.
 * \param option option to pass to NVMe command 
 *     default = 0, decompression = 1
 *
 * \return 0 if successfully submitted, KV_ERR_DD_NO_AVAILABLE_RESOURCE if an nvme_request
 *           structure cannot be allocated for the I/O request, KV_ERR_DD_INVALID_PARAM if
 *           key_length or buffer_length is too large.
 *
 * The command is submitted to a qpair allocated by spdk_nvme_ctrlr_alloc_io_qpair().
 * The user must ensure that only one thread submits I/O on a given qpair at any given time.
 */
int
spdk_nvme_kv_cmd_retrieve(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
			      uint32_t keyspace_id, void *key, uint32_t key_length,
			      void *buffer, uint32_t buffer_length,
			      uint32_t offset,
			      spdk_nvme_cmd_cb cb_fn, void *cb_arg,
			      uint32_t io_flags, uint32_t option)
{
	int ret = KV_SUCCESS;
	struct nvme_request *req;
	struct nvme_payload payload;
	
	if (key_length > KV_MAX_KEY_SIZE) return KV_ERR_DD_INVALID_PARAM;

	payload.type = NVME_PAYLOAD_TYPE_CONTIG;
	payload.u.contig = buffer;
	payload.md = key;

	req = _nvme_kv_cmd_allocate_request(ns, qpair, &payload, buffer_length,
			      0, 0, cb_fn, cb_arg, SPDK_NVME_OPC_KV_RETRIEVE,
			      io_flags, keyspace_id);
	if (NULL == req) {
		return KV_ERR_DD_NO_AVAILABLE_RESOURCE;
	}

	_nvme_kv_cmd_setup_retrieve_request(ns, req,
                           key_length, buffer_length, offset,
                           io_flags, option);

	ret = nvme_qpair_submit_request(qpair, req);
	if(ret != KV_SUCCESS){
		ret = KV_ERR_DD_NO_AVAILABLE_QUEUE;
	}
	return ret;
}

/**
 * \brief Submits a KV Delete I/O to the specified NVMe namespace.
 *
 * \param ns NVMe namespace to submit the KV DeleteI/O
 * \param qpair I/O queue pair to submit the request
 * \param keyspace_id namespace id of key
 * \param key virtual address pointer to the value
 * \param key_length length (in bytes) of the key
 * \param offset offset of value (in bytes)
 * \param cb_fn callback function to invoke when the I/O is completed
 * \param cb_arg argument to pass to the callback function
 * \param io_flags set flags, defined by the SPDK_NVME_IO_FLAGS_* entries
 *                      in spdk/nvme_spec.h, for this I/O.
 * \param option option to pass to NVMe command 
 *     No option supported for retrieve I/O yet. 
 *
 * \return 0 if successfully submitted, KV_ERR_DD_NO_AVAILABLE_RESOURCE if an nvme_request
 *           structure cannot be allocated for the I/O request, KV_ERR_DD_INVALID_PARAM if
 *           key_length or buffer_length is too large.
 *
 * The command is submitted to a qpair allocated by spdk_nvme_ctrlr_alloc_io_qpair().
 * The user must ensure that only one thread submits I/O on a given qpair at any given time.
 */
int
spdk_nvme_kv_cmd_delete(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
			      uint32_t keyspace_id, void *key, uint32_t key_length, uint32_t buffer_length, uint32_t offset,
			      spdk_nvme_cmd_cb cb_fn, void *cb_arg,
		              uint32_t io_flags, uint8_t  option)
{
	int ret = KV_SUCCESS;
	struct nvme_request *req;
	struct nvme_payload payload;
	
	if (key_length > KV_MAX_KEY_SIZE) return KV_ERR_DD_INVALID_PARAM;

	payload.type = NVME_PAYLOAD_TYPE_CONTIG;
	payload.u.contig = NULL;
	payload.md = key;

	req = _nvme_kv_cmd_allocate_request(ns, qpair, &payload,
			      0, //Payload length is 0 for delete command
			      0, 0, cb_fn, cb_arg, SPDK_NVME_OPC_KV_DELETE,
			      io_flags, keyspace_id);

	if (NULL == req) {
		return KV_ERR_DD_NO_AVAILABLE_RESOURCE;
	}

	_nvme_kv_cmd_setup_delete_request(ns, req,
                           key_length, buffer_length, offset, io_flags, option);

	ret = nvme_qpair_submit_request(qpair, req);
	if(ret != KV_SUCCESS){
		ret = KV_ERR_DD_NO_AVAILABLE_QUEUE;
	}
	return ret;

}


/**
 * \brief Submits a KV Exist I/O to the specified NVMe namespace.
 *
 * \param ns NVMe namespace to submit the KV Exist I/O
 * \param qpair I/O queue pair to submit the request
 * \param keys virtual address pointer to the key array 
 * \param key_length length (in bytes) of the key
 * \param buffer virtual address pointer to the return buffer 
 * \param buffer_length length (in bytes) of the return buffer 
 * \param cb_fn callback function to invoke when the I/O is completed
 * \param cb_arg argument to pass to the callback function
 * \param io_flags set flags, defined by the SPDK_NVME_IO_FLAGS_* entries
 *                      in spdk/nvme_spec.h, for this I/O.
 * \param option option to pass to NVMe command 
 *       0 - Fixed size; 1 - Variable size
 *
 * \return 0 if successfully submitted, KV_ERR_DD_NO_AVAILABLE_RESOURCE if an nvme_request
 *           structure cannot be allocated for the I/O request, KV_ERR_DD_INVALID_PARAM if
 *           key_length or buffer_length is too large.
 *
 * The command is submitted to a qpair allocated by spdk_nvme_ctrlr_alloc_io_qpair().
 * The user must ensure that only one thread submits I/O on a given qpair at any given time.
 */
int
spdk_nvme_kv_cmd_exist(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
			      uint32_t keyspace_id, void *key, uint32_t key_length,
			      spdk_nvme_cmd_cb cb_fn, void *cb_arg,
		              uint32_t io_flags, uint8_t  option)
{
	int ret = KV_SUCCESS;
	struct nvme_request *req;
	struct nvme_payload payload;
	
	if (key_length > KV_MAX_KEY_SIZE) return KV_ERR_DD_INVALID_PARAM;

	payload.type = NVME_PAYLOAD_TYPE_CONTIG;
	payload.u.contig = NULL;
	payload.md = key;

	req = _nvme_kv_cmd_allocate_request(ns, qpair, &payload, 
				0, //payload length is 0 for exist command
			      0, 0, cb_fn, cb_arg, SPDK_NVME_OPC_KV_EXIST,
			      io_flags, keyspace_id);

	if (NULL == req) {
		return KV_ERR_DD_NO_AVAILABLE_RESOURCE;
	}

	_nvme_kv_cmd_setup_exist_request(ns, req,
			   key_length, io_flags, option);

	ret = nvme_qpair_submit_request(qpair, req);
	if(ret != KV_SUCCESS){
		ret = KV_ERR_DD_NO_AVAILABLE_QUEUE;
	}
	return ret;

}

/**
 * \brief Submits a KV Iterate I/O to the specified NVMe namespace.
 *
 * \param ns NVMe namespace to submit the KV Iterate I/O
 * \param qpair I/O queue pair to submit the request
 * \param iterator iterator id (digit 1 byte)
 * \param buffer virtual address pointer to the return buffer 
 * \param buffer_length length (in bytes) of the return buffer 
 * \param buffer_offset offset (in bytes) of the return buffer 
 * \param cb_fn callback function to invoke when the I/O is completed
 * \param cb_arg argument to pass to the callback function
 * \param io_flags set flags, defined by the SPDK_NVME_IO_FLAGS_* entries
 *                      in spdk/nvme_spec.h, for this I/O.
 * \param option option to pass to NVMe command 
 *       0 - Fixed size; 1 - Variable size
 *
 * \return 0 if successfully submitted, KV_ERR_DD_NO_AVAILABLE_RESOURCE if an nvme_request
 *           structure cannot be allocated for the I/O request, KV_ERR_DD_INVALID_PARAM if
 *           key_length or buffer_length is too large.
 *
 * The command is submitted to a qpair allocated by spdk_nvme_ctrlr_alloc_io_qpair().
 * The user must ensure that only one thread submits I/O on a given qpair at any given time.
 */
int
spdk_nvme_kv_cmd_iterate_read(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
			      uint8_t iterator,
			      void *buffer, uint32_t buffer_length, uint32_t buffer_offset,
			      spdk_nvme_cmd_cb cb_fn, void *cb_arg,
		              uint32_t io_flags, uint8_t  option)
{
	int ret = KV_SUCCESS;
	struct nvme_request *req;
	struct nvme_payload payload;
	
	payload.type = NVME_PAYLOAD_TYPE_CONTIG;
	payload.u.contig = buffer;
	payload.md = NULL;

	req = _nvme_kv_cmd_allocate_request(ns, qpair, &payload, buffer_length,
			      0, 0, cb_fn, cb_arg, SPDK_NVME_OPC_KV_ITERATE_READ,
			      io_flags, 0);

	if (NULL == req) {
		return KV_ERR_DD_NO_AVAILABLE_RESOURCE;
	}

	_nvme_kv_cmd_setup_iterate_read_request(ns, req,
			   iterator, buffer, buffer_length, buffer_offset,
                           io_flags, option);

	ret = nvme_qpair_submit_request(qpair, req);
	if(ret != KV_SUCCESS){
		ret = KV_ERR_DD_NO_AVAILABLE_QUEUE;
	}
	return ret;

}

/**
 * \brief Submits a KV Iterate Request command with open option
 *
 * \param ns NVMe namespace to submit the KV Iterate I/O
 * \param qpair I/O queue pair to submit the request
 * \param keyspace_id keyspace_id (KV_KEYSPACE_IODATA=0, or KV_KEYSPACE_METADATA=1)
 * \param bitmask bitmask of matching key set
 * \param prefix prefix of matching key set
 * \param cb_fn callback function to invoke when the I/O is completed
 * \param cb_arg argument to pass to the callback function
 * \param io_flags set flags, defined by the SPDK_NVME_IO_FLAGS_* entries
 *                      in spdk/nvme_spec.h, for this I/O.
 * \param option option to pass to NVMe command 
 *       0 - Fixed size; 1 - Variable size
 *
 * \return 0 if successfully submitted, KV_ERR_DD_NO_AVAILABLE_RESOURCE if an nvme_request
 *           structure cannot be allocated for the I/O request, KV_ERR_DD_INVALID_PARAM if
 *           key_length or buffer_length is too large.
 *
 * The command is submitted to a qpair allocated by spdk_nvme_ctrlr_alloc_io_qpair().
 * The user must ensure that only one thread submits I/O on a given qpair at any given time.
 */
int
spdk_nvme_kv_cmd_iterate_open(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
			      uint8_t keyspace_id, uint32_t bitmask, uint32_t prefix,
			      spdk_nvme_cmd_cb cb_fn, void *cb_arg,
		              uint32_t io_flags, uint8_t  option)
{
	int ret = KV_SUCCESS;
	struct nvme_request *req;
	struct nvme_payload payload;
	
	payload.type = NVME_PAYLOAD_TYPE_CONTIG;
	payload.u.contig = NULL;
	payload.md = NULL;

	req = _nvme_kv_cmd_allocate_request(ns, qpair, &payload, 0,
			      0, 0, cb_fn, cb_arg, SPDK_NVME_OPC_KV_ITERATE_REQUEST,
			      io_flags, keyspace_id);

	if (NULL == req) {
		return KV_ERR_DD_NO_AVAILABLE_RESOURCE;
	}

	//NOTE : to make use of nvme command generation path as usual
	req->payload_size = 1;

	_nvme_kv_cmd_setup_iterate_open_request(ns, req,
			   bitmask, prefix,
                           io_flags, option);

	ret = nvme_qpair_submit_request(qpair, req);
	if(ret != KV_SUCCESS){
		ret = KV_ERR_DD_NO_AVAILABLE_QUEUE;
	}
	return ret;

}

/**
 * \brief Submits a KV Iterate Request command with open option
 *
 * \param ns NVMe namespace to submit the KV Iterate I/O
 * \param qpair I/O queue pair to submit the request
 * \param iterator iterator to be closed
 * \param cb_fn callback function to invoke when the I/O is completed
 * \param cb_arg argument to pass to the callback function
 * \param io_flags set flags, defined by the SPDK_NVME_IO_FLAGS_* entries
 *                      in spdk/nvme_spec.h, for this I/O.
 * \param option option to pass to NVMe command 
 *       0 - Fixed size; 1 - Variable size
 *
 * \return 0 if successfully submitted, KV_ERR_DD_NO_AVAILABLE_RESOURCE if an nvme_request
 *           structure cannot be allocated for the I/O request, KV_ERR_DD_INVALID_PARAM if
 *           key_length or buffer_length is too large.
 *
 * The command is submitted to a qpair allocated by spdk_nvme_ctrlr_alloc_io_qpair().
 * The user must ensure that only one thread submits I/O on a given qpair at any given time.
 */
int
spdk_nvme_kv_cmd_iterate_close(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
			      uint8_t iterator,
			      spdk_nvme_cmd_cb cb_fn, void *cb_arg,
		              uint32_t io_flags, uint8_t  option)
{
	int ret = KV_SUCCESS;
	struct nvme_request *req;
	struct nvme_payload payload;
	
	payload.type = NVME_PAYLOAD_TYPE_CONTIG;
	payload.u.contig = NULL;
	payload.md = NULL;

	req = _nvme_kv_cmd_allocate_request(ns, qpair, &payload, 0,
			      0, 0, cb_fn, cb_arg, SPDK_NVME_OPC_KV_ITERATE_REQUEST,
			      io_flags, 0);

	if (NULL == req) {
		return KV_ERR_DD_NO_AVAILABLE_RESOURCE;
	}

	//NOTE : to make use of nvme command generation path as usual
	req->payload_size = 1;

	_nvme_kv_cmd_setup_iterate_close_request(ns, req,
			   iterator,
                           io_flags, option);

	ret = nvme_qpair_submit_request(qpair, req);
	if(ret != KV_SUCCESS){
		ret = KV_ERR_DD_NO_AVAILABLE_QUEUE;
	}
	return ret;

}

/*
static void 
spdk_dump_nvme_cmd (const struct spdk_nvme_cmd *cmd) {
	if(!cmd){
		return;
	}

        printf("Dump nvme command: \n");
        printf("\tDWORD0\n");
        printf("\t\topc : 0x%04x\n", cmd->opc);
        printf("\t\tfuse : 0x%04x\n", cmd->fuse);
        printf("\t\trsvd1 : 0x%04x\n", cmd->rsvd1);
        printf("\t\tpsdt : 0x%04x\n", cmd->psdt);
        printf("\t\tcid : 0x%04x\n", cmd->cid);

        printf("\tDWORD1");
        printf("\tnsid : 0x%08x\n", cmd->nsid);

        printf("\tDWORD2");
        printf("\trsvd2 : 0x%08x\n", cmd->rsvd2);

        printf("\tDWORD3");
        printf("\trsvd3 : 0x%08x\n", cmd->rsvd3);

        printf("\tDWORD4-5");
        printf("\tmptr : 0x%016llx\n", (long long unsigned int)cmd->mptr);

        printf("\tDWORD6-9\n");
        printf("\t\tprp1 : 0x%016llx\n", (long long unsigned int)cmd->dptr.prp.prp1);
        printf("\t\tprp2 : 0x%016llx\n", (long long unsigned int)cmd->dptr.prp.prp2);

	printf("\tDWORD10");
	printf("\tcdw10 : 0x%08x\n", cmd->cdw10);
	printf("\tDWORD11");
	printf("\tcdw11 : 0x%08x\n", cmd->cdw11);
	printf("\tDWORD12");
	printf("\tcdw12 : 0x%08x\n", cmd->cdw12);
	printf("\tDWORD13");
	printf("\tcdw13 : 0x%08x\n", cmd->cdw13);
	printf("\tDWORD14");
	printf("\tcdw14 : 0x%08x\n", cmd->cdw14);
	printf("\tDWORD15");
	printf("\tcdw15 : 0x%08x\n", cmd->cdw15);

	char buf[32];
	memset(buf,0,sizeof(buf));
	memcpy(buf,(char*)&cmd->cdw12,4);
	memcpy(buf+4,(char*)&cmd->cdw13,4);
	memcpy(buf+8,(char*)&cmd->cdw14,4);
	memcpy(buf+12,(char*)&cmd->cdw15,4);
	printf("DWORD12-15: %s\n",buf);
}
*/
