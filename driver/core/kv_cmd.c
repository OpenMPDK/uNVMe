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
#include "kv_driver.h"
#include "kv_cmd.h"

//#define ITDEBUG
#ifdef ITDEBUG
static uint8_t iterate_handle_type;
#endif


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

        KVNVME_DEBUG("Status of the Async I/O: %d, Result of the Async I/O: %d, kv->key.key: %s", status, result, (char *)kv->key.key);

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
		//it->iterator = (uint8_t)((result&0x00FF0000)>>16);	//iterator id
		it->kv.key.length=((result&0xFF000000)>>24);		//msb 1B: key length
		uint32_t value_size = (result&0xFFFFFF);		//lsb 3B: value length
		if(spdk_likely(it->kv.value.length >= value_size)){
			it->kv.value.length = value_size;
		}
		else{
			KVNVME_ERR("warning : value.length from cdw0=%d, while value.length from caller=%d\n", value_size, it->kv.value.length);
		}
		KVNVME_DEBUG("status=%d key.length=%d value.length=%d\n",status, it->kv.key.length, it->kv.value.length);
                it->kv.param.async_cb(it, it->kv.value.length, status);
        }

        LEAVE();
}

int _kv_nvme_store(kv_nvme_t *nvme, const kv_pair *kv, int qid, uint8_t is_store) {
        int ret = KV_ERR_DD_INVALID_PARAM;
        struct spdk_nvme_qpair *qpair = NULL;
        nvme_cmd_sequence_t io_sequence = {0};

        ENTER();

        if(!kv || !kv->key.key || !kv->value.value) {
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

        KVNVME_DEBUG("Result of the I/O: %d, Status of the I/O: %d", io_sequence.result, io_sequence.status);
        LEAVE();
        return io_sequence.status;
}

int _kv_nvme_store_async(kv_nvme_t *nvme, const kv_pair *kv, int qid) {
        int ret = KV_ERR_DD_INVALID_PARAM;
        struct spdk_nvme_qpair *qpair = NULL;

        ENTER();

        if(!kv || !kv->key.key || !kv->value.value) {
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
        ret = spdk_nvme_kv_cmd_store(nvme->ns, qpair, kv->keyspace_id, kv->key.key, kv->key.length, kv->value.value, kv->value.length, kv->value.offset, _kv_async_io_complete, (void *)kv, 0, kv->param.io_option.store_option, true);
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

        if(!kv || !kv->key.key || !kv->value.value) {
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

        kv->value.length = io_sequence.result;

        LEAVE();
        return io_sequence.status;
}

int _kv_nvme_retrieve_async(kv_nvme_t *nvme, kv_pair *kv, int qid) {
        int ret = KV_ERR_DD_INVALID_PARAM;
        struct spdk_nvme_qpair *qpair = NULL;

        ENTER();

        if(!kv || !kv->key.key || !kv->value.value) {
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
        ret = spdk_nvme_kv_cmd_retrieve(nvme->ns, qpair, kv->keyspace_id, kv->key.key, kv->key.length, kv->value.value, kv->value.length, kv->value.offset, _kv_async_io_complete, (void *)kv, 0, kv->param.io_option.retrieve_option);

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
        uint64_t used_size = 0;
        const struct spdk_nvme_ns_data *ns_data = NULL;

        ENTER();

        sector_size = spdk_nvme_ns_get_sector_size(nvme->ns);

        if(!sector_size) {
                KVNVME_ERR("Could not get the Namespace Sector size");

                LEAVE();
                return used_size;
        }

        ns_data = spdk_nvme_ns_get_data(nvme->ns);

        if(!ns_data) {
                KVNVME_ERR("Could not get the Namespace data");

                LEAVE();
                return used_size;
        }

        used_size = ns_data->nuse;

        KVNVME_DEBUG("Used Size of the Device: %.2f", (float)used_size /100);

        LEAVE();
        return used_size;
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
        pthread_spin_unlock(&qpair->sq_lock);

        while(!io_sequence.is_completed) {
                spdk_nvme_qpair_process_completions(qpair, 0);
        }

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

        //NOTE : the first byte of cdw0(io_sequence.result) implies iterator id, which should be 1
        iterator = (io_sequence.result & 0xFF);
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
		if(it->kv.value.value){
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
	it->kv.key.length=((io_sequence.result&0xFF000000)>>24);		//msb 1B : key length
	uint32_t value_size = (io_sequence.result&0xFFFFFF);				//lsb 3B : value length
	if(spdk_likely(it->kv.value.length >= value_size)){
		it->kv.value.length = value_size;
	}
	else{
		KVNVME_ERR("warning : value.length from cdw0=%d, while value.length from caller=%d\n", value_size, it->kv.value.length);
	}
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
		if(it->kv.value.value){
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

