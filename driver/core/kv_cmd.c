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

#include "spdk/likely.h"
#include "spdk/util.h"
#include <math.h>
#include "kv_driver.h"
#include "kv_cmd.h"

//#define ITDEBUG
#ifdef ITDEBUG
static uint8_t iterate_handle_type;
#endif

#define GENERAL_KV_SSD

static void _kv_io_complete(void *arg, const struct spdk_nvme_cpl *completion) {
        nvme_cmd_sequence_t *io_sequence = NULL;

        ENTER();

        io_sequence = (nvme_cmd_sequence_t *)arg;

        io_sequence->status = completion->status.sc;
        io_sequence->result = completion->cdw0;

        io_sequence->is_completed = 1;

        KVNVME_DEBUG("Status of the I/O: %d, Result of the I/O: %d", io_sequence->status, io_sequence->result);

        LEAVE();
}

static void _kv_async_io_complete(void *arg, const struct spdk_nvme_cpl *completion) {
        kv_pair *kv = NULL;
        unsigned int status = 0, result = 0;

        ENTER();

        kv = (kv_pair *)arg;

        status = completion->status.sc;
        result = completion->cdw0;

	if(kv->param.async_cb) {
                kv->param.async_cb(kv, result, status);
	}

	LEAVE();
}

static void _kv_retrieve_async_io_complete(void *arg, const struct spdk_nvme_cpl *completion) {
        kv_pair *kv = NULL;
        unsigned int status = 0, result = 0;

        ENTER();

        kv = (kv_pair *)arg;

        status = completion->status.sc;
        result = completion->cdw0;

        KVNVME_DEBUG("Status of the Async I/O: %d, Result of the Async I/O: %d, kv->key.key: %s", status, result, (char *)kv->key.key);

        if(status == KV_SUCCESS){
          //result is the total value length that returned from ssd, 
          //remain value length = result - offset, user inputted buffer length may
          //big or little than remain_val_len
          kv->value.actual_value_size = result - kv->value.offset;
          kv->value.length = spdk_min(kv->value.length, kv->value.actual_value_size);
        }
        else{
          kv->value.length = 0;
          kv->value.actual_value_size = 0;
        }

        if(kv->param.async_cb) {
                kv->param.async_cb(kv, result, status);
        }

        LEAVE();
}

static void _kv_store_async_io_complete(void *arg, const struct spdk_nvme_cpl *completion) {
        kv_pair *kv = NULL;
        unsigned int status = 0, result = 0;

        ENTER();

        kv = (kv_pair *)arg;

        status = completion->status.sc;
        result = completion->cdw0;

        KVNVME_DEBUG("Status of the Async I/O: %d, Result of the Async I/O: %d, kv->key.key: %s", status, result, (char *)kv->key.key);

	if(status == KV_SUCCESS){
		kv->value.actual_value_size = kv->value.length;
	}

        if(kv->param.async_cb) {
                kv->param.async_cb(kv, result, status);
        }

        LEAVE();
}

static void _kv_iterate_read_async_cb(void *arg, const struct spdk_nvme_cpl *completion) {
        kv_iterate *it = NULL;
        unsigned int status = 0, result = 0;

        ENTER();

        it = (kv_iterate*)arg;

        status = completion->status.sc;
        result = completion->cdw0;

        KVNVME_DEBUG("Status of the Async I/O: %d, Result of the Async I/O: %d", status, result);

        if(it->kv.param.async_cb) {
#ifdef GENERAL_KV_SSD
		it->kv.key.length = 0;	// general KV SSD supports key only iterate
		uint32_t transfer_byte_size = result;
		if(spdk_likely(it->kv.value.length >= transfer_byte_size)){
			it->kv.value.length = transfer_byte_size;
		}
		else{
			KVNVME_ERR("warning : transfer_byte_size from cdw0=%d, while value.length from caller=%d\n", transfer_byte_size, it->kv.value.length);
		}
#else
		it->kv.key.length=((result&0xFF000000)>>24);		//msb 1B: key length
		uint32_t value_size = (result&0xFFFFFF);		//lsb 3B: value length
		if(spdk_likely(it->kv.value.length >= value_size)){
			it->kv.value.length = value_size;
		}
		else{
			KVNVME_ERR("warning : value.length from cdw0=%d, while value.length from caller=%d\n", value_size, it->kv.value.length);
		}
#endif
		KVNVME_DEBUG("status=%d key.length=%d value.length=%d\n",status, it->kv.key.length, it->kv.value.length);
                it->kv.param.async_cb(it, it->kv.value.length, status);
        }

        LEAVE();
}

int _kv_nvme_store(kv_nvme_t *nvme, kv_pair *kv, int qid, uint8_t is_store) {
        int ret = KV_ERR_DD_INVALID_PARAM;
        struct spdk_nvme_qpair *qpair = NULL;
        nvme_cmd_sequence_t io_sequence = {0};

        ENTER();

        if(!kv || !kv->key.key) {
                KVNVME_ERR("Invalid Parameters passed");
                LEAVE();
                return ret;
        }

        qpair = nvme->qpairs[qid];

        if(!qpair) {
                KVNVME_ERR("No Matching I/O Queue found for the Passed CPU Core ID");
                LEAVE();
                return ret;
        }

        pthread_spin_lock(&qpair->sq_lock);
        ret = spdk_nvme_kv_cmd_store(nvme->ns, qpair, kv->keyspace_id, kv->key.key, kv->key.length, kv->value.value, kv->value.length, kv->value.offset, _kv_io_complete, &io_sequence, 0, kv->param.io_option.store_option, is_store);

        if(ret) {
                pthread_spin_unlock(&qpair->sq_lock);
                KVNVME_ERR("Error in Performing Store on the KV Type SSD");
                LEAVE();
                return ret;
        }

        while(!io_sequence.is_completed) {
                spdk_nvme_qpair_process_completions(qpair, 0);
        }
        pthread_spin_unlock(&qpair->sq_lock);

	if(io_sequence.status == KV_SUCCESS){
		kv->value.actual_value_size = kv->value.length;
	}

        KVNVME_DEBUG("Result of the I/O: %d, Status of the I/O: %d", io_sequence.result, io_sequence.status);
        LEAVE();
        return io_sequence.status;
}

int _kv_nvme_store_async(kv_nvme_t *nvme, kv_pair *kv, int qid) {
        int ret = KV_ERR_DD_INVALID_PARAM;
        struct spdk_nvme_qpair *qpair = NULL;

        ENTER();

        if(!kv || !kv->key.key) {
                KVNVME_ERR("Invalid Parameters passed");
                LEAVE();
                return ret;
        }

        qpair = nvme->qpairs[qid];

        if(!qpair) {
                KVNVME_ERR("No Matching I/O Queue found for the Passed CPU Core ID");
                LEAVE();
                return ret;
        }

        pthread_spin_lock(&qpair->sq_lock);
        ret = spdk_nvme_kv_cmd_store(nvme->ns, qpair, kv->keyspace_id, kv->key.key, kv->key.length, kv->value.value, kv->value.length, kv->value.offset, _kv_store_async_io_complete, (void *)kv, 0, kv->param.io_option.store_option, true);
        if(ret) {
//              KVNVME_ERR("Error in Performing Store on the KV Type SSD");
        }
        pthread_spin_unlock(&qpair->sq_lock);
        LEAVE();
        return ret;
}

int _kv_nvme_retrieve(kv_nvme_t *nvme, kv_pair *kv, int qid) {
        int ret = KV_ERR_DD_INVALID_PARAM;
        struct spdk_nvme_qpair *qpair = NULL;
        nvme_cmd_sequence_t io_sequence = {0};

        ENTER();

        if(!kv || !kv->key.key) {
                KVNVME_ERR("Invalid Parameters passed");
                LEAVE();
                return ret;
        }
        if (kv->value.length & (KV_VALUE_LENGTH_ALIGNMENT_UNIT - 1))
          return KV_ERR_MISALIGNED_VALUE_SIZE;
        if (kv->value.offset & (KV_ALIGNMENT_UNIT - 1)) {
          return KV_ERR_MISALIGNED_VALUE_OFFSET;
        }

        qpair = nvme->qpairs[qid];

        if(!qpair) {
                KVNVME_ERR("No Matching I/O Queue found for the Passed CPU Core ID");
                LEAVE();
                return ret;
        }

        //calling retrieve as get_value_size
        pthread_spin_lock(&qpair->sq_lock);
	/*
        if(kv->param.io_option.retrieve_option & KV_RETRIEVE_VALUE_SIZE){
                kv->value.length = 0;
        }
	*/

        ret = spdk_nvme_kv_cmd_retrieve(nvme->ns, qpair, kv->keyspace_id, kv->key.key, kv->key.length, kv->value.value, kv->value.length, kv->value.offset, _kv_io_complete, &io_sequence, 0, kv->param.io_option.retrieve_option);

        if(ret) {
                pthread_spin_unlock(&qpair->sq_lock);
                KVNVME_ERR("Error in Performing Retrieve on the KV Type SSD");
                LEAVE();
                return ret;
        }

        while(!io_sequence.is_completed) {
                spdk_nvme_qpair_process_completions(qpair, 0);
        }
        pthread_spin_unlock(&qpair->sq_lock);

        KVNVME_DEBUG("Result of the I/O: %d, Status of the I/O: %d", io_sequence.result, io_sequence.status);

        if(io_sequence.status == KV_SUCCESS){
          kv->value.actual_value_size = io_sequence.result - kv->value.offset;
          kv->value.length = spdk_min(kv->value.length, kv->value.actual_value_size);
        }
        else{
          kv->value.length = 0;
          kv->value.actual_value_size = 0;
        }

        LEAVE();
        return io_sequence.status;
}

int _kv_nvme_retrieve_async(kv_nvme_t *nvme, kv_pair *kv, int qid) {
        int ret = KV_ERR_DD_INVALID_PARAM;
        struct spdk_nvme_qpair *qpair = NULL;

        ENTER();

        if(!kv || !kv->key.key) {
                KVNVME_ERR("Invalid Parameters passed");
                LEAVE();
                return ret;
        }
        if (kv->value.length & (KV_VALUE_LENGTH_ALIGNMENT_UNIT- 1))
          return KV_ERR_MISALIGNED_VALUE_SIZE;
        if(kv->value.offset & (KV_ALIGNMENT_UNIT - 1)) {
          return KV_ERR_MISALIGNED_VALUE_OFFSET;
        }

        qpair = nvme->qpairs[qid];

        if(!qpair) {
                KVNVME_ERR("No Matching I/O Queue found for the Passed CPU Core ID");
                LEAVE();
                return ret;
        }

        pthread_spin_lock(&qpair->sq_lock);
        ret = spdk_nvme_kv_cmd_retrieve(nvme->ns, qpair, kv->keyspace_id, kv->key.key, kv->key.length, kv->value.value, kv->value.length, kv->value.offset, _kv_retrieve_async_io_complete, (void *)kv, 0, kv->param.io_option.retrieve_option);

        if(ret) {
//              KVNVME_ERR("Error in Performing Retrieve on the KV Type SSD");
        }
        pthread_spin_unlock(&qpair->sq_lock);
        LEAVE();
        return ret;
}

int _kv_nvme_delete(kv_nvme_t *nvme, const kv_pair *kv, int qid) {
        int ret = KV_ERR_DD_INVALID_PARAM;
        struct spdk_nvme_qpair *qpair = NULL;
        nvme_cmd_sequence_t io_sequence = {0};

        ENTER();

        if(!kv || !kv->key.key) {
                KVNVME_ERR("Invalid Parameters passed");
                LEAVE();
                return ret;
        }

        qpair = nvme->qpairs[qid];

        if(!qpair) {
                KVNVME_ERR("No Matching I/O Queue found for the Passed CPU Core ID");
                LEAVE();
                return ret;
        }

        pthread_spin_lock(&qpair->sq_lock);
        ret = spdk_nvme_kv_cmd_delete(nvme->ns, qpair, kv->keyspace_id, kv->key.key, kv->key.length, kv->value.length, kv->value.offset, _kv_io_complete, &io_sequence, 0, kv->param.io_option.delete_option);

        if(ret) {
                pthread_spin_unlock(&qpair->sq_lock);
                KVNVME_ERR("Error in Performing Key Delete on the KV Type SSD");
                LEAVE();
                return ret;
        }

        while(!io_sequence.is_completed) {
                spdk_nvme_qpair_process_completions(qpair, 0);
        }
        pthread_spin_unlock(&qpair->sq_lock);

        KVNVME_DEBUG("Result of the I/O: %d, Status of the I/O: %d", io_sequence.result, io_sequence.status);
        LEAVE();
        return io_sequence.status;
}

int _kv_nvme_delete_async(kv_nvme_t *nvme, const kv_pair *kv, int qid) {
        int ret = KV_ERR_DD_INVALID_PARAM;
        struct spdk_nvme_qpair *qpair = NULL;

        ENTER();

        if(!kv || !kv->key.key) {
                KVNVME_ERR("Invalid Parameters passed");
                LEAVE();
                return ret;
        }

        qpair = nvme->qpairs[qid];

        if(!qpair) {
                KVNVME_ERR("No Matching I/O Queue found for the Passed CPU Core ID");
                LEAVE();
                return ret;
        }

        pthread_spin_lock(&qpair->sq_lock);
        ret = spdk_nvme_kv_cmd_delete(nvme->ns, qpair, kv->keyspace_id, kv->key.key, kv->key.length, kv->value.length, kv->value.offset, _kv_async_io_complete, (void *)kv, 0, kv->param.io_option.delete_option);

        if(ret) {
                //KVNVME_ERR("Error in Performing Key Delete on the KV Type SSD: ret=%d\n",ret);
        }
        pthread_spin_unlock(&qpair->sq_lock);
        LEAVE();
        return ret;
}

int _kv_nvme_format(kv_nvme_t *nvme, int ses) {
        int ret = KV_ERR_DD_INVALID_PARAM;
        uint32_t ns_id = 0;
        struct spdk_nvme_format format = {};

        ENTER();

        // Assuming the Device's default format type is 0 (Data size 512, Metadata size 0)
        format.lbaf = 0;
        format.ms = 0;
        format.pi = 0;
        format.pil = 0;

        // ses = 0, map erase ses = 1, userdata Erase
	if(ses != 0 && ses != 1){
		KVNVME_ERR("invalid ses value(%d). changed ses=1", ses);
		ses = 1;
	}

        format.ses = ses;

        ns_id = spdk_nvme_ns_get_id(nvme->ns);

        if(!ns_id) {
                KVNVME_ERR("Invalid Namespace ID: %d", ns_id);
                return ret;
        }

        KVNVME_INFO("Namespace ID: %d", ns_id);
        KVNVME_INFO("Ses : %d", ses);

        ret = spdk_nvme_ctrlr_format(nvme->ctrlr, ns_id, &format);

        LEAVE();
        return ret;
}

uint64_t _kv_nvme_get_used_size(kv_nvme_t* nvme) {
        uint32_t sector_size = 0;
        double utilization = 0;
        const struct spdk_nvme_ns_data *ns_data = NULL;

        ENTER();

        sector_size = spdk_nvme_ns_get_sector_size(nvme->ns);
        if(!sector_size) {
                KVNVME_ERR("Could not get the Namespace Sector size");

                LEAVE();
                return 0;
        }

        ns_data = spdk_nvme_ns_get_data(nvme->ns);
        if(!ns_data) {
                KVNVME_ERR("Could not get the Namespace data");

                LEAVE();
                return 0;
        }

        utilization = (1.0 * ns_data->nuse)/ns_data->nsze;
        KVNVME_DEBUG("Used Size of the Device: %.2f%%", utilization*100);
        // translat 0% ~100% to value 0 ~10000)
        utilization = (uint16_t)round(utilization * 10000);

        LEAVE();
        return utilization;
}

int _kv_nvme_exist(kv_nvme_t* nvme, const kv_pair* kv, int qid) {
        int ret = KV_ERR_DD_INVALID_PARAM;
        struct spdk_nvme_qpair *qpair = NULL;
        nvme_cmd_sequence_t io_sequence = {0};

        ENTER();

        if(!kv || !kv->key.key) {
                KVNVME_ERR("Invalid Parameters passed");
                LEAVE();
                return ret;
        }

        qpair = nvme->qpairs[qid];

        if(!qpair) {
                KVNVME_ERR("No Matching I/O Queue found for the Passed CPU Core ID");
                LEAVE();
                return ret;
        }

        pthread_spin_lock(&qpair->sq_lock);
        ret = spdk_nvme_kv_cmd_exist(nvme->ns, qpair, kv->keyspace_id, kv->key.key, kv->key.length, _kv_io_complete, &io_sequence, 0, kv->param.io_option.exist_option);
        if(ret) {
                pthread_spin_unlock(&qpair->sq_lock);
                KVNVME_ERR("Error in Performing Key Exist on the KV Type SSD");
                LEAVE();
                return ret;
        }

        while(!io_sequence.is_completed) {
                spdk_nvme_qpair_process_completions(qpair, 0);
        }
        pthread_spin_unlock(&qpair->sq_lock);

        //KVNVME_ERR("Result of the I/O: %d, Status of the I/O: %d", io_sequence.result, io_sequence.status);

        LEAVE();
        return io_sequence.status;
}


int _kv_nvme_exist_async(kv_nvme_t* nvme, const kv_pair* kv, int qid){
	int ret = KV_ERR_DD_INVALID_PARAM;
	struct spdk_nvme_qpair *qpair = NULL;

	ENTER();

	if(!kv || !kv->key.key) {
                KVNVME_ERR("Invalid Parameters passed");
                LEAVE();
                return ret;
	}

        qpair = nvme->qpairs[qid];

        if(!qpair) {
                KVNVME_ERR("No Matching I/O Queue found for the Passed CPU Core ID");
                LEAVE();
                return ret;
        }

        pthread_spin_lock(&qpair->sq_lock);
        ret = spdk_nvme_kv_cmd_exist(nvme->ns, qpair, kv->keyspace_id, kv->key.key, kv->key.length, _kv_async_io_complete, (void*)kv, 0, kv->param.io_option.exist_option);
        if(ret) {
                //KVNVME_ERR("Error in Performing Key Exist on the KV Type SSD");
        }
        pthread_spin_unlock(&qpair->sq_lock);
	LEAVE();
	return ret;
}


uint32_t _kv_nvme_iterate_open(kv_nvme_t *nvme, const uint8_t keyspace_id, const uint32_t bitmask, const uint32_t prefix, const uint8_t iterate_type, int qid){
        uint32_t iterator = KV_INVALID_ITERATE_HANDLE;
        uint32_t ret = KV_ERR_DD_INVALID_PARAM;
        struct spdk_nvme_qpair *qpair = NULL;
        nvme_cmd_sequence_t io_sequence = {0};
	uint8_t it_type_base = 2;
/*
	iterate handle type:
	KV_KEY_ITERATE = 0x01, KV_KEY_ITERATE_WITH_RETRIEVE = 0x02, KV_KEY_ITERATE_WITH_DELETE = 0x03,
	while, those should be specified as 0x4, 0x8, and 0x10 in Iterate Request command, respectively
*/

        ENTER();

        if(!nvme){
                KVNVME_ERR("Invalid Parameters passed");
                LEAVE();
                return ret;
        }

#ifdef GENERAL_KV_SSD
	if(iterate_type != KV_KEY_ITERATE && iterate_type != KV_KEY_ITERATE_WITH_DELETE){
		KVNVME_ERR("Invalid iterate type");
		LEAVE();
		return KV_ERR_ITERATE_ERROR;
	
	}
#else
	if(iterate_type != KV_KEY_ITERATE && iterate_type != KV_KEY_ITERATE_WITH_RETRIEVE && iterate_type != KV_KEY_ITERATE_WITH_DELETE){
		KVNVME_ERR("Invalid iterate type");
		LEAVE();
		return KV_ERR_ITERATE_ERROR;
	}
#endif

        qpair = nvme->qpairs[qid];

        if(!qpair) {
                KVNVME_ERR("No Matching I/O Queue found for the Passed CPU Core ID");
                LEAVE();
                return ret;
        }

        pthread_spin_lock(&qpair->sq_lock);
        ret = spdk_nvme_kv_cmd_iterate_open(nvme->ns, qpair, keyspace_id, bitmask, prefix, _kv_io_complete, &io_sequence, 0, (KV_ITERATE_REQUEST_OPEN | (it_type_base<<iterate_type)));
        if(ret) {
                pthread_spin_unlock(&qpair->sq_lock);
                KVNVME_ERR("Error in Performing Key Iterate on the KV Type SSD\n");
                LEAVE();
                return ret;
        }

#ifdef ITDEBUG
	iterate_handle_type = iterate_type;
#endif


	int queue_is_sync = ((nvme->io_queue_type[qid] == SYNC_IO_QUEUE) ? 1 : 0);
	if(queue_is_sync){
		while(!io_sequence.is_completed) {
			spdk_nvme_qpair_process_completions(qpair, 0);
		}
	}
	else{
		while(!io_sequence.is_completed) {
			usleep(1);
		}
	}
        pthread_spin_unlock(&qpair->sq_lock);

        KVNVME_DEBUG("Result of the I/O: %d, Status of the I/O: %d", io_sequence.result, io_sequence.status);

        //NOTE : in normal case, the first byte of cdw0(io_sequence.result) implies iterator id, which should be 1~16
	iterator = (io_sequence.status) ? io_sequence.status : (io_sequence.result & 0xFF);
        LEAVE();
        return iterator;

}

int _kv_nvme_iterate_close(kv_nvme_t *nvme, const uint8_t iterator, int qid){
        uint32_t ret = KV_ERR_DD_INVALID_PARAM;
        struct spdk_nvme_qpair *qpair = NULL;
        nvme_cmd_sequence_t io_sequence = {0};

        ENTER();

        if(iterator == KV_INVALID_ITERATE_HANDLE) {
                KVNVME_ERR("Invalid Parameters passed");
                LEAVE();
                return ret;
        }

        qpair = nvme->qpairs[qid];

        if(!qpair) {
                KVNVME_ERR("No Matching I/O Queue found for the Passed CPU Core ID");
                LEAVE();
                return ret;
        }

        pthread_spin_lock(&qpair->sq_lock);
        ret = spdk_nvme_kv_cmd_iterate_close(nvme->ns, qpair, iterator, _kv_io_complete, &io_sequence, 0, KV_ITERATE_REQUEST_CLOSE);
        if(ret) {
                pthread_spin_unlock(&qpair->sq_lock);
                KVNVME_ERR("Error in Performing Key Iterate on the KV Type SSD");
                LEAVE();
                return ret;
        }

	int queue_is_sync = ((nvme->io_queue_type[qid] == SYNC_IO_QUEUE) ? 1 : 0);
	if(queue_is_sync){
		while(!io_sequence.is_completed) {
			spdk_nvme_qpair_process_completions(qpair, 0);
		}
	}
	else{
		while(!io_sequence.is_completed) {
			usleep(1);
		}
	}
        pthread_spin_unlock(&qpair->sq_lock);

        KVNVME_DEBUG("Result of the I/O: %d, Status of the I/O: %d", io_sequence.result, io_sequence.status);

        LEAVE();

        return io_sequence.status;
}

int _kv_nvme_iterate_read(kv_nvme_t* nvme, kv_iterate* it, int qid){
        int ret = KV_ERR_DD_INVALID_PARAM;
        struct spdk_nvme_qpair *qpair = NULL;
        nvme_cmd_sequence_t io_sequence = {0};

        ENTER();

        if(!it || it->iterator == KV_INVALID_ITERATE_HANDLE || !it->kv.value.value || it->kv.value.length == 0) {
                KVNVME_ERR("Invalid Parameters passed");
		if(it && it->kv.value.value){
			it->kv.value.length = 0;
		}
                LEAVE();
                return ret;
        }

        qpair = nvme->qpairs[qid];

        if(!qpair) {
                KVNVME_ERR("No Matching I/O Queue found for the Passed CPU Core ID");
                LEAVE();
                return ret;
        }

#ifdef ITDEBUG
	static int nr_iterate_read = 0;	
	nr_iterate_read++;
	memset(it->kv.value.value,0,it->kv.value.length);

	if(!(nr_iterate_read%50)){
		io_sequence.status = KV_ERR_ITERATE_READ_EOF;
		it->kv.value.length = 1024;
	}
	else{
		io_sequence.status = KV_SUCCESS;
		it->kv.value.length = 2048;
	}

	if(iterate_handle_type == KV_KEY_ITERATE_WITH_RETRIEVE){
		it->kv.key.length= 16;
		snprintf(it->kv.value.value, it->kv.key.length+1, "%016d", nr_iterate_read);
		int kv_buffer_offset = 512;
		memset(it->kv.value.value+kv_buffer_offset,'a'+(nr_iterate_read%26), it->kv.value.length);
	}
	else{
		it->kv.key.length = 0;
		memset(it->kv.value.value,'a'+(nr_iterate_read%26), it->kv.value.length);
	}
	io_sequence.result = (it->kv.key.length<<24) | (it->kv.value.length);
#else

	pthread_spin_lock(&qpair->sq_lock);
	ret = spdk_nvme_kv_cmd_iterate_read(nvme->ns, qpair, it->iterator, it->kv.value.value, it->kv.value.length, it->kv.value.offset, _kv_io_complete, &io_sequence, 0, it->kv.param.io_option.iterate_read_option);
	if(ret) {
		pthread_spin_unlock(&qpair->sq_lock);
		KVNVME_ERR("Error in Performing Key Iterate on the KV Type SSD");
		LEAVE();
		return ret;
	}

	while(!io_sequence.is_completed) {
		spdk_nvme_qpair_process_completions(qpair, 0);
	}
	pthread_spin_unlock(&qpair->sq_lock);
#endif

        KVNVME_DEBUG("Result of the I/O: %d, Status of the I/O: %d", io_sequence.result, io_sequence.status);

	//it->iterator = (uint8_t)((io_sequence.result&0x00FF0000)>>16);	//iterator id
#ifndef GENERAL_KV_SSD
	it->kv.key.length=0;	// general kv ssd only supports key-only iterate
	uint32_t transfer_byte_size = io_sequence.result;				//lsb 3B : value length
	if(spdk_likely(it->kv.value.length >= transfer_byte_size)){
		it->kv.value.length = transfer_byte_size;
	}
	else{
		KVNVME_ERR("warning : transfer_byte_size from cdw0=%d, while value.length from caller=%d\n", transfer_byte_size, it->kv.value.length);
	}
#else
	it->kv.key.length=((io_sequence.result&0xFF000000)>>24);		//msb 1B : key length
	uint32_t value_size = (io_sequence.result&0xFFFFFF);				//lsb 3B : value length
	if(spdk_likely(it->kv.value.length >= value_size)){
		it->kv.value.length = value_size;
	}
	else{
		KVNVME_ERR("warning : value.length from cdw0=%d, while value.length from caller=%d\n", value_size, it->kv.value.length);
	}
#endif
        KVNVME_DEBUG("status=%d iterator=%d it->kv.key.length=%d it->kv.value.length=%d\n",io_sequence.status, it->iterator, it->kv.key.length, it->kv.value.length);

        LEAVE();
        return io_sequence.status;
}

int _kv_nvme_iterate_read_async(kv_nvme_t *nvme, kv_iterate* it, int qid){
        int ret = KV_ERR_DD_INVALID_PARAM;
        struct spdk_nvme_qpair *qpair = NULL;

        ENTER();

        if(!it || it->iterator == KV_INVALID_ITERATE_HANDLE || !it->kv.value.value || it->kv.value.length == 0) {
            KVNVME_ERR("Invalid Parameters passed");
            if(it && it->kv.value.value){
                it->kv.value.length = 0;
            }
            LEAVE();
            return ret;
        }

        qpair = nvme->qpairs[qid];

        if(!qpair) {
                KVNVME_ERR("No Matching I/O Queue found for the Passed CPU Core ID");
                LEAVE();
                return ret;
        }

#ifdef ITDEBUG
	struct spdk_nvme_cpl cpl;
	static int nr_iterate_async_read = 0;	
	nr_iterate_async_read++;
	memset(it->kv.value.value,0,it->kv.value.length);

	if(!(nr_iterate_async_read%50)){
		cpl.status.sc = KV_ERR_ITERATE_READ_EOF;
		it->kv.value.length = 1024;
	}
	else{
		cpl.status.sc = KV_SUCCESS;
		it->kv.value.length = 2048;
	}

	if(iterate_handle_type == KV_KEY_ITERATE_WITH_RETRIEVE){
		it->kv.key.length= 16;
		snprintf(it->kv.value.value, it->kv.key.length+1, "%016d", nr_iterate_async_read);
		int kv_buffer_offset = 512;
		memset(it->kv.value.value+kv_buffer_offset,'a'+(nr_iterate_async_read%26), it->kv.value.length);
	}
	else{
		it->kv.key.length = 0;
                memset(it->kv.value.value,'a'+(nr_iterate_async_read%26), it->kv.value.length);
	}

	cpl.cdw0 = (it->kv.key.length<<24)|(it->kv.value.length);
	_kv_iterate_read_async_cb(it, &cpl);
	ret = KV_SUCCESS;
#else
        pthread_spin_lock(&qpair->sq_lock);
        ret = spdk_nvme_kv_cmd_iterate_read(nvme->ns, qpair, it->iterator, it->kv.value.value, it->kv.value.length, it->kv.value.offset, _kv_iterate_read_async_cb, (void *)it, 0, it->kv.param.io_option.iterate_read_option);
        if(ret) {
                //KVNVME_ERR("Error in Performing Retrieve on the KV Type SSD");
        }
        pthread_spin_unlock(&qpair->sq_lock);
#endif
        LEAVE();
        return ret;

}

int kv_nvme_iterate_info(uint64_t handle, kv_iterate_handle_info* info, int nr_handle) {
        int ret = KV_ERR_DD_INVALID_PARAM;
        int handle_info_size = 16;
        int log_id = 0xd0;
        char logbuf[512];
        int i;

        ENTER();

        if(!handle || !info || nr_handle <= 0|| nr_handle > KV_MAX_ITERATE_HANDLE){
                KVNVME_ERR("Invalid paramter ");
                LEAVE();
                return ret;
        }

        memset(logbuf,0,sizeof(logbuf));
        ret = kv_nvme_get_log_page(handle, log_id, logbuf, sizeof(logbuf));
        if(ret != KV_SUCCESS){
                ret = KV_ERR_IO;
                return ret;
        }

        int offset = 0;

        /* FIXED FORMAT */
        for(i=0;i<nr_handle;i++){
                offset = (i*handle_info_size);
                info[i].handle_id = (*(uint8_t*)(logbuf + offset + 0));
                info[i].status = (*(uint8_t*)(logbuf + offset + 1));
                info[i].type = (*(uint8_t*)(logbuf + offset + 2));
                info[i].keyspace_id = (*(uint8_t*)(logbuf + offset + 3));

                info[i].prefix = (*(uint32_t*)(logbuf + offset + 4));
                info[i].bitmask = (*(uint32_t*)(logbuf + offset + 8));
                /* The bitpattern of the KV API is of big-endian mode. 
                   If the CPU is of little-endian mode, 
                   the bit pattern and bit mask should be transformed.  */
                info[i].prefix = htobe32(info[i].prefix);
                info[i].bitmask = htobe32(info[i].bitmask);

		info[i].is_eof = (*(uint8_t*)(logbuf + offset + 12));
                info[i].reserved[0] = (*(uint8_t*)(logbuf + offset + 13));
                info[i].reserved[1] = (*(uint8_t*)(logbuf + offset + 14));
                info[i].reserved[2] = (*(uint8_t*)(logbuf + offset + 15));
                KVNVME_DEBUG("handle_id=%d status=%d type=%d prefix=%08x bitmask=%08x is_eof=%d\n",
                        info[i].handle_id, info[i].status, info[i].type, info[i].prefix, info[i].bitmask, info[i].is_eof);
        }

        /*
        for(i=0;i<*nr_handle;i++){
                offset += (i*handle_info_size);
                info[i].handle_id = (*(uint8_t*)(logbuf + offset + 0));
                info[i].status = (*(uint8_t*)(logbuf + offset + 4));
                info[i].prefix = (*(uint32_t*)(logbuf + offset + 8));
                info[i].bitmask = (*(uint32_t*)(logbuf + offset + 12));
                KVNVME_DEBUG("handle_id=%d status=%d prefix=%08x bitmask=%08x\n",
		info[i].handle_id, info[i].status, info[i].prefix, info[i].bitmask);
	}
	*/
	LEAVE();
	return ret;
}
