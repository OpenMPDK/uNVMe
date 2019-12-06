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

#include <pthread.h>
#include "kv_driver.h"

#include "kv_cmd.h"
#include "lba_cmd.h"

/*!Check wether the iterator bitmask is valid 
 * \desc:  bitmask should be set from the first bit of a key and it is not 
 *            allowed setting bitmask from a middle position of a key. Hence, 
  *           Setting bitmask / prefix as 0xFF/0x0F is allowed while 0x0F/0x0F
 *             is not allowed.
 * \return bool : if input is valid bitmask return true, else return false
 */
inline bool _is_valid_bitmask(uint32_t bitmask){
  const uint32_t BITMASK_LEN = 32;
  //scan prefix bits whose value is 1; scan order: from high bits to low bits
  uint32_t cnt = 0;
  uint32_t bit_idx = 0;
  while(cnt < BITMASK_LEN){
    bit_idx = BITMASK_LEN - cnt - 1;
    if(!(bitmask & (1<<bit_idx)))
      break;
    cnt++;
  }

  //scan remain bits, if has bits whose value is 1, return false, else return true;
  if(cnt == BITMASK_LEN)
    return true;

  cnt++;
  while(cnt < BITMASK_LEN){
    bit_idx = BITMASK_LEN - cnt - 1;
    if(bitmask & (1<<bit_idx))
      return false;
    cnt++;
  }

  return true;
};

static void admin_complete(void *arg, const struct spdk_nvme_cpl *completion) {
	nvme_cmd_sequence_t *admin_sequence = NULL;

	ENTER();

	admin_sequence = (nvme_cmd_sequence_t *)arg;

	admin_sequence->status = completion->status.sc;
	admin_sequence->result = completion->cdw0;

	admin_sequence->is_completed = 1;

	KVNVME_DEBUG("Status of the Admin command: %d, Result of the Admin command: %d", admin_sequence->status, admin_sequence->result);

	LEAVE();
}

static void io_complete(void *arg, const struct spdk_nvme_cpl *completion) {
	nvme_cmd_sequence_t *io_sequence = NULL;

	ENTER();

	io_sequence = (nvme_cmd_sequence_t *)arg;

	io_sequence->status = completion->status.sc;
	io_sequence->result = completion->cdw0;

	io_sequence->is_completed = 1;

	KVNVME_DEBUG("Status of the I/O: %d, Result of the I/O: %d", io_sequence->status, io_sequence->result);

	LEAVE();
}

kv_nvme_cpl_t *kv_nvme_submit_raw_cmd(uint64_t handle, kv_nvme_cmd_t cmd, void *buf, uint32_t buf_len, raw_cmd_type_t type) {
	int ret = KV_ERR_DD_INVALID_PARAM;
	int qid = DEFAULT_IO_QUEUE_ID;
	kv_nvme_cpl_t *cpl = NULL;
	kv_nvme_t *nvme = NULL;
	struct spdk_nvme_qpair *qpair = NULL;
	nvme_cmd_sequence_t cmd_sequence = {0};
	struct spdk_nvme_cmd spdk_cmd = {0};

	ENTER();

	if(!handle) {
		KVNVME_ERR("Invalid handle passed");

		LEAVE();
		return cpl;
	}

	cpl = calloc(1, sizeof(kv_nvme_cpl_t));

	if(!cpl) {
		KVNVME_ERR("Could not allocate a completion structure");

		LEAVE();
		return cpl;
	}

	nvme = (kv_nvme_t *)handle;

	memcpy(&spdk_cmd, &cmd, sizeof(struct spdk_nvme_cmd));

	if(ADMIN_CMD_TYPE == type) {
		ret = spdk_nvme_ctrlr_cmd_admin_raw(nvme->ctrlr, &spdk_cmd, buf, buf_len, admin_complete, &cmd_sequence);
	} else if(IO_CMD_TYPE == type) {

		qid = sched_getcpu();
		if(qid < 0 ) {
			KVNVME_WARN("Could not get the CPU Core ID, Using Default 0");
			qid = 0;
		}
		qpair = nvme->qpairs[qid];

		if(!qpair) {
			KVNVME_ERR("No Matching I/O Queue found for the Passed CPU Core ID");

			free(cpl);

			LEAVE();
			return NULL;
		}

		ret = spdk_nvme_ctrlr_cmd_io_raw(nvme->ctrlr, qpair, &spdk_cmd, buf, buf_len, io_complete, &cmd_sequence);
	} else {
		KVNVME_ERR("Invalid Command Type: %d", type);

		free(cpl);

		LEAVE();
		return NULL;
	}

	if(ret) {
		KVNVME_ERR("Failed to Submit a Raw Command, ret = %d", ret);

		free(cpl);

		LEAVE();
		return NULL;
	}

	while(!cmd_sequence.is_completed) {
		if(ADMIN_CMD_TYPE == type) {
			spdk_nvme_ctrlr_process_admin_completions(nvme->ctrlr);
		} else if((IO_CMD_TYPE == type)) {
			if(qpair) {
				spdk_nvme_qpair_process_completions(qpair, 0);
			} else {
				KVNVME_ERR("Invalid I/O Queue to Process Completions");

				free(cpl);

				LEAVE();
				return NULL;
			}
		} else {
			KVNVME_ERR("Invalid Command Type: %d", type);

			free(cpl);

			LEAVE();
			return NULL;
		}
	}

	KVNVME_DEBUG("Result of the Raw command: %d, Status of the Raw command: %d", cmd_sequence.result, cmd_sequence.status);

	cpl->result = cmd_sequence.result;
	cpl->status = cmd_sequence.status;

	LEAVE();
	return cpl;
}

int kv_nvme_append(uint64_t handle, int qid, kv_pair *kv) {
	int ret = KV_ERR_DD_INVALID_PARAM;
	unsigned int queue_is_async = 0;
	kv_nvme_t *nvme = NULL;

	ENTER();

	if(!handle) {
		KVNVME_ERR("Invalid handle passed");

		LEAVE();
		return ret;
	}

	nvme = (kv_nvme_t *)handle;
	if(qid < 0){
		qid = sched_getcpu();
		if(qid < 0) {
			KVNVME_WARN("Could not get the CPU Core ID, Using Default 0");
			qid = 0;
		}
	}
  if(qid >= MAX_CPU_CORES || !nvme->io_queue_type[qid]) {
    KVNVME_ERR("Invalid qid: %d passed", qid);
    LEAVE();
    return ret;
  }

	queue_is_async = ((nvme->io_queue_type[qid] == ASYNC_IO_QUEUE) ? 1 : 0);
	if(queue_is_async) {
		KVNVME_ERR("I/O Queue supports only Async, and Append command doesn't support Async");

		LEAVE();
		return KV_ERR_DD_INVALID_QUEUE_TYPE;
	}

	uint8_t is_store = 0;
	ret = nvme->dev_ops.write(nvme, kv, qid, is_store);

	LEAVE();
	return ret;
}

int kv_nvme_write(uint64_t handle, int qid, kv_pair *kv) {
	int ret = KV_ERR_DD_INVALID_PARAM;
	unsigned int queue_is_async = 0;
	kv_nvme_t *nvme = NULL;

	ENTER();

	if(!handle) {
		KVNVME_ERR("Invalid handle passed");

		LEAVE();
		return ret;
	}

	nvme = (kv_nvme_t *)handle;
	if(qid < 0){
		qid = sched_getcpu();
		if(qid < 0) {
			KVNVME_WARN("Could not get the CPU Core ID, Using Default 0");
			qid = 0;
		}
	}
  if(qid >= MAX_CPU_CORES || !nvme->io_queue_type[qid]) {
    KVNVME_ERR("Invalid qid: %d passed", qid);
    LEAVE();
    return ret;
  }

	queue_is_async = ((nvme->io_queue_type[qid] == ASYNC_IO_QUEUE) ? 1 : 0);
	if(queue_is_async) {
		//KVNVME_ERR("I/O Queue supports only Async, Please do Async Write");

		LEAVE();
		return KV_ERR_DD_INVALID_QUEUE_TYPE;
	}

	uint8_t is_store = 1;
	ret = nvme->dev_ops.write(nvme, kv, qid, is_store);

	LEAVE();
	return ret;
}


int kv_nvme_write_async(uint64_t handle, int qid, kv_pair *kv) {
	int ret = KV_ERR_DD_INVALID_PARAM;
	unsigned int queue_is_sync = 0;
	kv_nvme_t *nvme = NULL;

	ENTER();

	if(!handle) {
		KVNVME_ERR("Invalid handle passed");

		LEAVE();
		return ret;
	}

	nvme = (kv_nvme_t *)handle;
	if(qid < 0){
		qid = sched_getcpu();
		if(qid < 0) {
			KVNVME_WARN("Could not get the CPU Core ID, Using Default 0");
			qid = 0;
		}
	}
  if(qid >= MAX_CPU_CORES || !nvme->io_queue_type[qid]) {
    KVNVME_ERR("Invalid qid: %d passed", qid);
    LEAVE();
    return ret;
  }

	queue_is_sync = ((nvme->io_queue_type[qid] == SYNC_IO_QUEUE) ? 1 : 0);
	if(queue_is_sync) {
		//KVNVME_ERR("I/O Queue supports only Sync, Please do Sync Write");

		LEAVE();
		return KV_ERR_DD_INVALID_QUEUE_TYPE;
	}

	ret = nvme->dev_ops.write_async(nvme, kv, qid);

	LEAVE();
	return ret;
}

int kv_nvme_read(uint64_t handle, int qid, kv_pair *kv) {
	int ret = KV_ERR_DD_INVALID_PARAM;
	unsigned int queue_is_async = 0;
	kv_nvme_t *nvme = NULL;

	ENTER();

	if(!handle) {
		KVNVME_ERR("Invalid handle passed");

		LEAVE();
		return ret;
	}

	nvme = (kv_nvme_t *)handle;
	if(qid < 0){
		qid = sched_getcpu();
		if(qid < 0) {
			KVNVME_WARN("Could not get the CPU Core ID, Using Default 0");
			qid = 0;
		}
	}
  if(qid >= MAX_CPU_CORES || !nvme->io_queue_type[qid]) {
    KVNVME_ERR("Invalid qid: %d passed", qid);
    LEAVE();
    return ret;
  }

	queue_is_async = ((nvme->io_queue_type[qid] == ASYNC_IO_QUEUE) ? 1 : 0);
	if(queue_is_async) {
		//KVNVME_ERR("I/O Queue supports only Async, Please do Async Read");

		LEAVE();
		return KV_ERR_DD_INVALID_QUEUE_TYPE;
	}

	ret = nvme->dev_ops.read(nvme, kv, qid);

	LEAVE();
	return ret;
}

int kv_nvme_read_async(uint64_t handle, int qid, kv_pair *kv) {
	int ret = KV_ERR_DD_INVALID_PARAM;
	unsigned int queue_is_sync = 0;
	kv_nvme_t *nvme = NULL;

	ENTER();

	if(!handle) {
		KVNVME_ERR("Invalid handle passed");

		LEAVE();
		return ret;
	}

	nvme = (kv_nvme_t *)handle;
	if(qid < 0){
		qid = sched_getcpu();
		if(qid < 0) {
			KVNVME_WARN("Could not get the CPU Core ID, Using Default 0");
			qid = 0;
		}
	}
  if(qid >= MAX_CPU_CORES || !nvme->io_queue_type[qid]) {
    KVNVME_ERR("Invalid qid: %d passed", qid);
    LEAVE();
    return ret;
  }

	queue_is_sync = ((nvme->io_queue_type[qid] == SYNC_IO_QUEUE) ? 1 : 0);
	if(queue_is_sync) {
		//KVNVME_ERR("I/O Queue supports only Sync, Please do Sync Read");

		LEAVE();
		return KV_ERR_DD_INVALID_QUEUE_TYPE;
	}

	ret = nvme->dev_ops.read_async(nvme, kv, qid);

	LEAVE();
	return ret;
}

int kv_nvme_delete(uint64_t handle, int qid, const kv_pair *kv) {
	int ret = KV_ERR_DD_INVALID_PARAM;
	unsigned int queue_is_async = 0;
	kv_nvme_t *nvme = NULL;

	ENTER();

	if(!handle) {
		KVNVME_ERR("Invalid handle passed");

		LEAVE();
		return ret;
	}

	nvme = (kv_nvme_t *)handle;
	if(qid < 0){
		qid = sched_getcpu();
		if(qid < 0) {
			KVNVME_WARN("Could not get the CPU Core ID, Using Default 0");
			qid = 0;
		}
	}
  if(qid >= MAX_CPU_CORES || !nvme->io_queue_type[qid]) {
    KVNVME_ERR("Invalid qid: %d passed", qid);
    LEAVE();
    return ret;
  }

	queue_is_async = ((nvme->io_queue_type[qid] == ASYNC_IO_QUEUE) ? 1 : 0);
	if(queue_is_async) {
		//KVNVME_ERR("I/O Queue supports only Async, Please do Async Delete");

		LEAVE();
		return KV_ERR_DD_INVALID_QUEUE_TYPE;
	}

	if(nvme->dev_ops.delete) {
		ret = nvme->dev_ops.delete(nvme, kv, qid);
	} else {
		KVNVME_ERR("This function is not supported by the Device");

		ret = KV_ERR_DD_UNSUPPORTED_CMD;
	}

	LEAVE();
	return ret;
}

int kv_nvme_delete_async(uint64_t handle, int qid, const kv_pair *kv) {
	int ret = KV_ERR_DD_INVALID_PARAM;
	unsigned int queue_is_sync = 0;
	kv_nvme_t *nvme = NULL;

	ENTER();

	if(!handle) {
		KVNVME_ERR("Invalid handle passed");

		LEAVE();
		return ret;
	}

	nvme = (kv_nvme_t *)handle;
	if(qid < 0){
		qid = sched_getcpu();
		if(qid < 0) {
			KVNVME_WARN("Could not get the CPU Core ID, Using Default 0");
			qid = 0;
		}
	}
  if(qid >= MAX_CPU_CORES || !nvme->io_queue_type[qid]) {
    KVNVME_ERR("Invalid qid: %d passed", qid);
    LEAVE();
    return ret;
  }

	queue_is_sync = ((nvme->io_queue_type[qid] == SYNC_IO_QUEUE) ? 1 : 0);
	if(queue_is_sync) {
		//KVNVME_ERR("I/O Queue supports only Sync, Please do Sync Delete");

		LEAVE();
		return KV_ERR_DD_INVALID_QUEUE_TYPE;
	}

	if(nvme->dev_ops.delete_async) {
		ret = nvme->dev_ops.delete_async(nvme, kv, qid);
	} else {
		KVNVME_ERR("This function is not supported by the Device");

		ret = KV_ERR_DD_UNSUPPORTED_CMD;
	}

	LEAVE();
	return ret;
}

int kv_nvme_exist(uint64_t handle, int qid, const kv_pair* kv) {
	int ret = KV_ERR_DD_INVALID_PARAM;
	unsigned int queue_is_async = 0;
	kv_nvme_t *nvme = NULL;

	ENTER();

	if(!handle) {
		KVNVME_ERR("Invalid handle passed");

		LEAVE();
		return ret;
	}

	nvme = (kv_nvme_t *)handle;
	if(qid < 0){
		qid = sched_getcpu();
		if(qid < 0) {
			KVNVME_WARN("Could not get the CPU Core ID, Using Default 0");
			qid = 0;
		}
	}
  if(qid >= MAX_CPU_CORES || !nvme->io_queue_type[qid]) {
    KVNVME_ERR("Invalid qid: %d passed", qid);
    LEAVE();
    return ret;
  }

	queue_is_async = ((nvme->io_queue_type[qid] == ASYNC_IO_QUEUE) ? 1 : 0);
	if(queue_is_async) {
		//KVNVME_ERR("I/O Queue supports only Async, Please do Async Delete");

		LEAVE();
		return KV_ERR_DD_INVALID_QUEUE_TYPE;
	}

	if(nvme->dev_ops.exist) {
		ret = nvme->dev_ops.exist(nvme, kv, qid);
	} else {
		KVNVME_ERR("This function is not supported by the Device");

		ret = KV_ERR_DD_UNSUPPORTED_CMD;
	}

	LEAVE();
	return ret;
}

int kv_nvme_exist_async(uint64_t handle, int qid, const kv_pair* kv) {
	int ret = KV_ERR_DD_INVALID_PARAM;
	unsigned int queue_is_sync = 0;
	kv_nvme_t *nvme = NULL;

	ENTER();

	if(!handle) {
		KVNVME_ERR("Invalid handle passed");

		LEAVE();
		return ret;
	}

	nvme = (kv_nvme_t *)handle;
	if(qid < 0){
		qid = sched_getcpu();
		if(qid < 0) {
			KVNVME_WARN("Could not get the CPU Core ID, Using Default 0");
			qid = 0;
		}
	}
  if(qid >= MAX_CPU_CORES || !nvme->io_queue_type[qid]) {
    KVNVME_ERR("Invalid qid: %d passed", qid);
    LEAVE();
    return ret;
  }

	queue_is_sync = ((nvme->io_queue_type[qid] == SYNC_IO_QUEUE) ? 1 : 0);
	if(queue_is_sync) {
		//KVNVME_ERR("I/O Queue supports only Async, Please do Async Delete");

		LEAVE();
		return KV_ERR_DD_INVALID_QUEUE_TYPE;
	}

	if(nvme->dev_ops.exist_async) {
		ret = nvme->dev_ops.exist_async(nvme, kv, qid);
	} else {
		KVNVME_ERR("This function is not supported by the Device");

		ret = KV_ERR_DD_UNSUPPORTED_CMD;
	}

	LEAVE();
	return ret;
}

uint32_t kv_nvme_iterate_open(uint64_t handle, uint8_t keyspace_id, uint32_t bitmask, uint32_t prefix, uint8_t iterate_type){
	uint32_t ret = KV_ERR_DD_INVALID_PARAM;
	int qid = 0;
	kv_nvme_t *nvme = NULL;
	uint32_t iterator = KV_INVALID_ITERATE_HANDLE;

	ENTER();

  if(!_is_valid_bitmask(bitmask)) {
    KVNVME_ERR("Invalid bitmask inputted.");
    LEAVE();
    return KV_ERR_DD_ITERATE_COND_INVALID;
  }
	if(!handle){
		KVNVME_ERR("Invalid handle passed");
		LEAVE();
		return ret;
	}
	
	if(keyspace_id != KV_KEYSPACE_IODATA && keyspace_id != KV_KEYSPACE_METADATA){
		KVNVME_ERR("Invalid keyspace id");
		LEAVE();
		return ret;
	}

	nvme = (kv_nvme_t *)handle;

	qid = sched_getcpu();
	if(qid < 0) {
		KVNVME_WARN("Could not get the CPU Core ID, Using Default 0");
		qid = 0;
	}
 
        /* Convert bitmask and bit_pattern endian to big endian when cpu is little endian  */
        bitmask = htobe32(bitmask);
	prefix = htobe32(prefix);
	if(nvme->dev_ops.iterate_open) {
		iterator = nvme->dev_ops.iterate_open(nvme, keyspace_id, bitmask, prefix, iterate_type, qid);
		LEAVE();
		return iterator;
	} else {
		KVNVME_ERR("This function is not supported by the Device");
		ret = KV_ERR_DD_UNSUPPORTED_CMD;
	}

	LEAVE();
	return ret;
}

int kv_nvme_iterate_close(uint64_t handle, const uint8_t iterator){
	int ret = KV_ERR_DD_INVALID_PARAM;
	int qid = 0;
	kv_nvme_t *nvme = NULL;

	ENTER();

	if(!handle) {
		KVNVME_ERR("Invalid handle passed");

		LEAVE();
		return ret;
	}

	nvme = (kv_nvme_t *)handle;

	qid = sched_getcpu();
	if(qid < 0) {
		KVNVME_WARN("Could not get the CPU Core ID, Using Default 0");
		qid = 0;
	}

	if(nvme->dev_ops.iterate_close) {
		ret = nvme->dev_ops.iterate_close(nvme, iterator, qid);
	} else {
		KVNVME_ERR("This function is not supported by the Device");

		ret = KV_ERR_DD_UNSUPPORTED_CMD;
	}

	LEAVE();
	return ret;
}

int kv_nvme_iterate_read(uint64_t handle, int qid, kv_iterate* it){
	int ret = KV_ERR_DD_INVALID_PARAM;
	unsigned int queue_is_async = 0;
	kv_nvme_t *nvme = NULL;

	ENTER();

	if(!handle) {
		KVNVME_ERR("Invalid handle passed");

		LEAVE();
		return ret;
	}

	nvme = (kv_nvme_t *)handle;
	if(qid < 0){
		qid = sched_getcpu();
		if(qid < 0) {
			KVNVME_WARN("Could not get the CPU Core ID, Using Default 0");
			qid = 0;
		}
	}
  if(qid >= MAX_CPU_CORES || !nvme->io_queue_type[qid]) {
    KVNVME_ERR("Invalid qid: %d passed", qid);
    LEAVE();
    return ret;
  }

	queue_is_async = ((nvme->io_queue_type[qid] == ASYNC_IO_QUEUE) ? 1 : 0);
	if(queue_is_async) {
		//KVNVME_ERR("I/O Queue supports only Async, Please do Async Read");

		LEAVE();
		return KV_ERR_DD_INVALID_QUEUE_TYPE;
	}

	ret = nvme->dev_ops.iterate_read(nvme, it, qid);

	LEAVE();
	return ret;
}

int kv_nvme_iterate_read_async(uint64_t handle, int qid, kv_iterate* it) {
	int ret = KV_ERR_DD_INVALID_PARAM;
	unsigned int queue_is_sync = 0;
	kv_nvme_t *nvme = NULL;

	ENTER();

	if(!handle) {
		KVNVME_ERR("Invalid handle passed");

		LEAVE();
		return ret;
	}

	nvme = (kv_nvme_t *)handle;
	if(qid < 0){
		qid = sched_getcpu();
		if(qid < 0) {
			KVNVME_WARN("Could not get the CPU Core ID, Using Default 0");
			qid = 0;
		}
	}
  if(qid >= MAX_CPU_CORES || !nvme->io_queue_type[qid]) {
    KVNVME_ERR("Invalid qid: %d passed", qid);
    LEAVE();
    return ret;
  }

	queue_is_sync = ((nvme->io_queue_type[qid] == SYNC_IO_QUEUE) ? 1 : 0);
	if(queue_is_sync) {
		//KVNVME_ERR("I/O Queue supports only Sync, Please do Sync Read");

		LEAVE();
		return KV_ERR_DD_INVALID_QUEUE_TYPE;
	}

	ret = nvme->dev_ops.iterate_read_async(nvme, it, qid);

	LEAVE();
	return ret;
}

int kv_nvme_format(uint64_t handle, int erase_user_data){
	int ret = KV_ERR_DD_INVALID_PARAM;
	kv_nvme_t *nvme = NULL;

	ENTER();

	if(!handle) {
		KVNVME_ERR("Invalid handle passed");

		LEAVE();
		return ret;
	}

	nvme = (kv_nvme_t *)handle;

	ret = nvme->dev_ops.format(nvme, erase_user_data);

	LEAVE();
	return ret;
}

uint64_t kv_nvme_get_total_size(uint64_t handle) {
	uint32_t sector_size = 0;
	uint64_t total_size = 0;
	kv_nvme_t *nvme = NULL;
	const struct spdk_nvme_ns_data *ns_data = NULL;

	ENTER();

	if(!handle) {
		KVNVME_ERR("Invalid handle passed");

		LEAVE();
		return total_size;
	}

	nvme = (kv_nvme_t *)handle;

	sector_size = spdk_nvme_ns_get_sector_size(nvme->ns);

	if(!sector_size) {
		KVNVME_ERR("Could not get the Namespace Sector size");

		LEAVE();
		return total_size;
	}

	ns_data = spdk_nvme_ns_get_data(nvme->ns);

	if(!ns_data) {
		KVNVME_ERR("Could not get the Namespace data");

		LEAVE();
		return total_size;
	}

	total_size = sector_size * ns_data->ncap;

	KVNVME_DEBUG("Total Size of the Device: %lld MB", (unsigned long long)total_size / MB);

	LEAVE();
	return total_size;
}

uint64_t kv_nvme_get_used_size(uint64_t handle) {
	uint64_t used_size = 0;
	kv_nvme_t *nvme = NULL;

	ENTER();

	if(!handle) {
		KVNVME_ERR("Invalid handle passed");

		LEAVE();
		return used_size;
	}

	nvme = (kv_nvme_t *)handle;

	used_size = nvme->dev_ops.get_used_size(nvme);

	LEAVE();
	return used_size;
}

int kv_nvme_get_log_page(uint64_t handle, uint8_t log_id, void* buffer, uint32_t buffer_size) {
	uint32_t ret = KV_ERR_DD_INVALID_PARAM;
	kv_nvme_t *nvme = NULL;
	uint8_t *hpage_buffer = NULL;
	uint32_t ns_id = 0;
	nvme_cmd_sequence_t cmd_sequence = {0};

	ENTER();

	if(!handle) {
		KVNVME_ERR("Invalid handle passed");
		LEAVE();
		return ret;
	}

	if(!buffer){
		KVNVME_ERR("Invalid buffer pointer");
		LEAVE();
		return ret;
	}


	nvme = (kv_nvme_t *)handle;

	ns_id = spdk_nvme_ns_get_id(nvme->ns);

	if(!ns_id) {
		KVNVME_ERR("Invalid Namespace ID: %d", ns_id);
		ret = KV_ERR_IO;
		return ret;
	}

	hpage_buffer = kv_zalloc(buffer_size);
	if(!hpage_buffer){
		KVNVME_ERR("fail to allocate huge page for log buffer");
		ret = KV_ERR_IO;
		return ret;
	}

	ret = spdk_nvme_ctrlr_cmd_get_log_page(nvme->ctrlr, log_id, ns_id, hpage_buffer, buffer_size, 0, admin_complete, &cmd_sequence);
	if(ret) {
		KVNVME_ERR("Failed to Get Log Page(0x%x) ret = 0x%x ", log_id, ret);
		kv_free(hpage_buffer);

		LEAVE();
		ret = KV_ERR_IO;
		return ret;
	}

	while(!cmd_sequence.is_completed) {
		spdk_nvme_ctrlr_process_admin_completions(nvme->ctrlr);
	}

	KVNVME_DEBUG("Result of Get Log Page(0x%x): result=0x%x status=0x%x", log_id, cmd_sequence.result, cmd_sequence.status);

	if(cmd_sequence.status == 0) {
		memcpy(buffer, hpage_buffer, buffer_size);
	}

	kv_free(hpage_buffer);

	LEAVE();
	return ret;
}


uint64_t kv_nvme_get_waf(uint64_t handle) {
	int ret = KV_ERR_DD_INVALID_PARAM;
	uint64_t waf = KV_ERR_INVALID_VALUE;
	char* log_buffer = NULL;

	ENTER();

	if(!handle) {
		KVNVME_ERR("Invalid handle passed");
		LEAVE();
		return ret;
	}

	log_buffer = kv_zalloc(VENDOR_LOG_SIZE);
	if(!log_buffer) {
		KVNVME_ERR("Could not allocate memory for the Vendor log page");

		LEAVE();
		return ret;
	}

	ret = kv_nvme_get_log_page(handle, VENDOR_LOG_ID, log_buffer,  VENDOR_LOG_SIZE);
	if(ret == KV_SUCCESS){
    // const uint32_t waf_val = *((uint32_t *)&data[256]);
		waf = *((uint32_t *)&log_buffer[256]);
	}

	kv_free(log_buffer);

	LEAVE();
	return waf;
}

uint32_t kv_nvme_get_sector_size(uint64_t handle) {
	uint32_t sector_size = 0;
	ENTER();

	if(!handle){
		KVNVME_ERR("Invalid handle passed");
		LEAVE();
		return sector_size;
	}

	kv_nvme_t *nvme = (kv_nvme_t *)handle;

	sector_size = spdk_nvme_ns_get_sector_size(nvme->ns);

	if(!sector_size) {
		KVNVME_ERR("Could not get the Namespace Sector size");
	}

	LEAVE();
	return sector_size;
}

uint64_t kv_nvme_get_num_sectors(uint64_t handle) {
	uint64_t num_sectors = 0;
	ENTER();

	if(!handle){
		KVNVME_ERR("Invalid handle passed");
		LEAVE();
		return num_sectors;
	}

	kv_nvme_t *nvme = (kv_nvme_t *)handle;

	num_sectors = spdk_nvme_ns_get_num_sectors(nvme->ns);

	if(!num_sectors) {
		KVNVME_ERR("Could not get the Number of Sectors in Namespace");
	}

	LEAVE();
	return num_sectors;
}

uint16_t kv_nvme_get_io_queue_size(uint64_t handle) {
	uint16_t queue_size = 0;

	ENTER();

	if(!handle){
		KVNVME_ERR("Invalid handle passed");
		LEAVE();
		return queue_size;
	}

	kv_nvme_t* nvme = (kv_nvme_t*)handle;
	queue_size = spdk_nvme_ns_get_max_io_queue_size(nvme->ns);

	LEAVE();
	return queue_size;
}

enum qd_op{
	READ,
	INCREASE,
	DECREASE,
};

static uint16_t _kv_qd_operation(uint64_t handle, int qid, int qd_op){
	int current_qd = 0;

	ENTER();

	if(!handle){
		KVNVME_ERR("Invalid handle passed");
		LEAVE();
		return current_qd;
	}

	kv_nvme_t* nvme = (kv_nvme_t *)handle;
	struct spdk_nvme_qpair* qpair = nvme->qpairs[qid];
	if(!qpair) {
		KVNVME_ERR("No Matching I/O Queue found for the Passed CPU Core ID");
		LEAVE();
		return current_qd;
	}


	pthread_spin_lock(&qpair->sq_lock);
	switch(qd_op){
		case INCREASE:
			current_qd = ++qpair->current_qd;
			break;
		case DECREASE:
			current_qd = --qpair->current_qd;
			break;
		case READ:
		default:
			current_qd = qpair->current_qd;
			break;
	}
	pthread_spin_unlock(&qpair->sq_lock);

	LEAVE();
	return current_qd;
}

uint16_t kv_get_current_qd(uint64_t handle, int qid){
	return _kv_qd_operation(handle, qid, READ);
}

uint16_t kv_increase_current_qd(uint64_t handle, int qid){
	return _kv_qd_operation(handle, qid, INCREASE);
}

uint16_t kv_decrease_current_qd(uint64_t handle, int qid){
	return _kv_qd_operation(handle, qid, DECREASE);
}

