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
#include "lba_cmd.h"

static void _lba_io_complete(void *arg, const struct spdk_nvme_cpl *completion) {
        nvme_cmd_sequence_t *io_sequence = NULL;

        ENTER();

        io_sequence = (nvme_cmd_sequence_t *)arg;

        io_sequence->status = completion->status.sc;
        io_sequence->result = completion->cdw0;

        io_sequence->is_completed = 1;

        KVNVME_DEBUG("Status of the I/O: %d, Result of the I/O: %d", io_sequence->status, io_sequence->result);

        LEAVE();
}

static void _lba_async_io_complete(void *arg, const struct spdk_nvme_cpl *completion) {
        kv_pair *kv = NULL;
        unsigned int status = 0, result = 0;

        ENTER();

        kv = (kv_pair *)arg;

        status = completion->status.sc;
        result = completion->status.sct;

        KVNVME_DEBUG("Status of the Async I/O: %d, Result of the Async I/O: %d, kv->key.key: %s", status, result, (char *)kv->key.key);

        if(kv->param.async_cb) {
                kv->param.async_cb(kv, result, status);
        }

        LEAVE();
}



int _lba_nvme_write(kv_nvme_t *nvme, kv_pair *kv, int core_id, uint8_t is_store) {
        int ret = KV_ERR_DD_INVALID_PARAM;
        struct spdk_nvme_qpair *qpair = NULL;
        nvme_cmd_sequence_t io_sequence = {0};
        void *buffer = NULL;
        uint64_t lba = 0;
        char *key_id = NULL;
        char sub_key_id[LBA_SSD_KEY_ID_SIZE];

        ENTER();

        if(!kv || !kv->key.key || !kv->value.value) {
                KVNVME_ERR("Invalid Parameters passed");
                LEAVE();
                return ret;
        }

        qpair = nvme->qpairs[core_id];
        if(!qpair) {
                KVNVME_ERR("No Matching I/O Queue found for the Passed CPU Core ID");
                LEAVE();
                return ret;
        }

        buffer = (void *)(((uint64_t *)kv->value.value) + kv->value.offset);

        key_id = (char *)(kv->key.key);

        lba = *(uint64_t*)key_id;

        KVNVME_DEBUG("Complete Key ID: %s, Dissected Key ID: %s, LBA Offset: 0x%llx", key_id, sub_key_id, (unsigned long long)lba);


        pthread_spin_lock(&qpair->sq_lock);
        ret = spdk_nvme_ns_cmd_write(nvme->ns, qpair, buffer, lba, (kv->value.length / nvme->sector_size), _lba_io_complete, &io_sequence, 0);

        if(ret) {
                pthread_spin_unlock(&qpair->sq_lock);
                KVNVME_ERR("Error in Performing Write on the LBA Type SSD");
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

int _lba_nvme_write_async(kv_nvme_t *nvme, kv_pair *kv, int core_id) {
        int ret = KV_ERR_DD_INVALID_PARAM;
        struct spdk_nvme_qpair *qpair = NULL;
        void *buffer = NULL;
        uint64_t lba = 0;
        char *key_id = NULL;
        char sub_key_id[LBA_SSD_KEY_ID_SIZE];

        ENTER();

        if(!kv || !kv->key.key || !kv->value.value) {
                KVNVME_ERR("Invalid Parameters passed");
                LEAVE();
                return ret;
        }

        qpair = nvme->qpairs[core_id];

        if(!qpair) {
                KVNVME_ERR("No Matching I/O Queue found for the Passed CPU Core ID");
                LEAVE();
                return ret;
        }

        buffer = (void *)(((uint64_t *)kv->value.value) + kv->value.offset);

        key_id = (char *)(kv->key.key);

        lba = *(uint64_t*)key_id;

        KVNVME_DEBUG("Complete Key ID: %s, Dissected Key ID: %s, LBA Offset: 0x%llx", key_id, sub_key_id, (unsigned long long)lba);

        pthread_spin_lock(&qpair->sq_lock);
        ret = spdk_nvme_ns_cmd_write(nvme->ns, qpair, buffer, lba, (kv->value.length / nvme->sector_size), _lba_async_io_complete, (void *)kv, 0);
        pthread_spin_unlock(&qpair->sq_lock);
        LEAVE();
        return ret;
}

int _lba_nvme_read(kv_nvme_t *nvme, kv_pair* kv, int core_id) {
        int ret = KV_ERR_DD_INVALID_PARAM;
        struct spdk_nvme_qpair *qpair = NULL;
        nvme_cmd_sequence_t io_sequence = {0};
        void *buffer = NULL;
        uint64_t lba = 0;
        char *key_id = NULL;
        char sub_key_id[LBA_SSD_KEY_ID_SIZE];

        ENTER();

        if(!kv || !kv->key.key || !kv->value.value) {
                KVNVME_ERR("Invalid Parameters passed");
                LEAVE();
                return ret;
        }

        qpair = nvme->qpairs[core_id];

        if(!qpair) {
                KVNVME_ERR("No Matching I/O Queue found for the Passed CPU Core ID");
                LEAVE();
                return ret;
        }

        buffer = (void *)(((uint64_t *)kv->value.value) + kv->value.offset);

        key_id = (char *)(kv->key.key);

        lba = *(uint64_t*)key_id;

        KVNVME_DEBUG("Complete Key ID: %s, Dissected Key ID: %s, LBA Offset: 0x%llx", key_id, sub_key_id, (unsigned long long)lba);

        pthread_spin_lock(&qpair->sq_lock);
        ret = spdk_nvme_ns_cmd_read(nvme->ns, qpair, buffer, lba, (kv->value.length / nvme->sector_size), _lba_io_complete, &io_sequence, 0);

        if(ret) {
                pthread_spin_unlock(&qpair->sq_lock);
                KVNVME_ERR("Error in Performing Read on the LBA Type SSD");
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

int _lba_nvme_read_async(kv_nvme_t *nvme, kv_pair *kv, int core_id) {
        int ret = KV_ERR_DD_INVALID_PARAM;
        struct spdk_nvme_qpair *qpair = NULL;
        void *buffer = NULL;
        uint64_t lba = 0;
        char *key_id = NULL;
        char sub_key_id[LBA_SSD_KEY_ID_SIZE];

        ENTER();

        if(!kv || !kv->key.key || !kv->value.value) {
                KVNVME_ERR("Invalid Parameters passed");

                LEAVE();
                return ret;
        }

        qpair = nvme->qpairs[core_id];

        if(!qpair) {
                KVNVME_ERR("No Matching I/O Queue found for the Passed CPU Core ID");
                LEAVE();
                return ret;
        }

        buffer = (void *)(((uint64_t *)kv->value.value) + kv->value.offset);

        key_id = (char *)(kv->key.key);

        lba = *(uint64_t*)key_id;

        KVNVME_DEBUG("Complete Key ID: %s, Dissected Key ID: %s, LBA Offset: 0x%llx", key_id, sub_key_id, (unsigned long long)lba);

        pthread_spin_lock(&qpair->sq_lock);
        ret = spdk_nvme_ns_cmd_read(nvme->ns, qpair, buffer, lba, (kv->value.length / nvme->sector_size), _lba_async_io_complete, (void *)kv, 0);
        pthread_spin_unlock(&qpair->sq_lock);

        LEAVE();
        return ret;
}

int _lba_nvme_delete(kv_nvme_t *nvme, const kv_pair *kv, int core_id) {
	int ret = KV_ERR_DD_INVALID_PARAM;
	struct spdk_nvme_qpair *qpair = NULL;
	nvme_cmd_sequence_t io_sequence = {0};
	uint64_t lba = 0;
	char *key_id = NULL;
	char sub_key_id[LBA_SSD_KEY_ID_SIZE];
	struct spdk_nvme_dsm_range dsm_range;

	ENTER();

	if(!kv || !kv->key.key) {
		KVNVME_ERR("Invalid Parameters passed");

		LEAVE();
		return ret;
	}

	qpair = nvme->qpairs[core_id];

	if(!qpair) {
		KVNVME_ERR("No Matching I/O Queue found for the Passed CPU Core ID");
		LEAVE();
		return ret;
	}

	key_id = (char *)(kv->key.key);

	lba = *(uint64_t*)key_id;

	dsm_range.starting_lba = lba;
	dsm_range.length = kv->value.length / nvme->sector_size;
	dsm_range.attributes.raw = 0;

	KVNVME_DEBUG("Complete Key ID: %s, Dissected Key ID: %s, LBA Offset: 0x%llx", key_id, sub_key_id, (unsigned long long)lba);

	pthread_spin_lock(&qpair->sq_lock);
	ret = spdk_nvme_ns_cmd_dataset_management(nvme->ns, qpair, SPDK_NVME_DSM_ATTR_DEALLOCATE, &dsm_range, 1, _lba_io_complete, &io_sequence);

	if(ret) {
		pthread_spin_unlock(&qpair->sq_lock);
		KVNVME_ERR("Error in Performing Deallocate on the LBA Type SSD");
		LEAVE();
		return ret;
	}

	while(!io_sequence.is_completed) {
		spdk_nvme_qpair_process_completions(qpair, 0);
	}
	pthread_spin_unlock(&qpair->sq_lock);

	KVNVME_DEBUG("Result of the I/O: %d, Status of the I/O: %d", io_sequence.result, io_sequence.status);

	LEAVE();
	return ret;
}
int _lba_nvme_delete_async(kv_nvme_t *nvme, const kv_pair *kv, int core_id) {
	int ret = KV_ERR_DD_INVALID_PARAM;
	struct spdk_nvme_qpair *qpair = NULL;
	uint64_t offset_blocks, num_blocks;
	char *key_id = NULL;
	char sub_key_id[LBA_SSD_KEY_ID_SIZE];
	struct spdk_nvme_dsm_range dsm_ranges[SPDK_NVME_DATASET_MANAGEMENT_MAX_RANGES];
	struct spdk_nvme_dsm_range *range;
	uint64_t offset, remaining;
	uint64_t num_ranges_u64;
	uint16_t num_ranges;

	ENTER();

	if(!kv || !kv->key.key) {
		KVNVME_ERR("Invalid Parameters passed");

		LEAVE();
		return ret;
	}

	qpair = nvme->qpairs[core_id];

	if(!qpair) {
		KVNVME_ERR("No Matching I/O Queue found for the Passed CPU Core ID");
		LEAVE();
		return ret;
	}

	key_id = (char *)(kv->key.key);

	offset_blocks = *(uint64_t*)key_id;
	num_blocks = kv->value.length;

	num_ranges_u64 = (num_blocks + SPDK_NVME_DATASET_MANAGEMENT_RANGE_MAX_BLOCKS - 1) /
			 SPDK_NVME_DATASET_MANAGEMENT_RANGE_MAX_BLOCKS;
	if (num_ranges_u64 > SPDK_COUNTOF(dsm_ranges)) {
		SPDK_ERRLOG("Unmap request for %" PRIu64 " blocks is too large\n", num_blocks);
		return -EINVAL;
	}
	num_ranges = (uint16_t)num_ranges_u64;

	offset = offset_blocks;
	remaining = num_blocks;
	range = &dsm_ranges[0];

	/* Fill max-size ranges until the remaining blocks fit into one range */
	while (remaining > SPDK_NVME_DATASET_MANAGEMENT_RANGE_MAX_BLOCKS) {
		range->attributes.raw = 0;
		range->length = SPDK_NVME_DATASET_MANAGEMENT_RANGE_MAX_BLOCKS;
		range->starting_lba = offset;

		offset += SPDK_NVME_DATASET_MANAGEMENT_RANGE_MAX_BLOCKS;
		remaining -= SPDK_NVME_DATASET_MANAGEMENT_RANGE_MAX_BLOCKS;
		range++;
	}

	/* Final range describes the remaining blocks */
	range->attributes.raw = 0;
	range->length = remaining;
	range->starting_lba = offset;

	KVNVME_DEBUG("Complete Key ID: %s, Dissected Key ID: %s, LBA Offset: 0x%llx", key_id, sub_key_id, (unsigned long long)offset_blocks);

	pthread_spin_lock(&qpair->sq_lock);
	ret = spdk_nvme_ns_cmd_dataset_management(nvme->ns, qpair, SPDK_NVME_DSM_ATTR_DEALLOCATE, dsm_ranges, num_ranges, _lba_async_io_complete, (void *)kv);
	pthread_spin_unlock(&qpair->sq_lock);

	LEAVE();
	return ret;
}

int _lba_nvme_format(kv_nvme_t *nvme, int ses) {
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

uint64_t _lba_nvme_get_used_size(kv_nvme_t* nvme) {
        uint32_t sector_size = 0;
        uint64_t used_size = KV_ERR_INVALID_VALUE;
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

        used_size = sector_size * ns_data->nuse;

        KVNVME_INFO("Used Size of the Device: %lld MB", (unsigned long long)used_size / MB);

        LEAVE();
        return used_size;
}

