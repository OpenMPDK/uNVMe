/*-
 *   BSD LICENSE
 *
 *   Copyright (C) 2008-2012 Daisuke Aoyama <aoyama@peach.ne.jp>.
 *   Copyright (c) Intel Corporation.
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
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
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

#include <kv_types.h>
#include <kv_apis.h>
#include <sys/shm.h>
#include <sys/stat.h>

#include "spdk/stdinc.h"

#include "spdk/conf.h"
#include "spdk/endian.h"
#include "spdk/bdev.h"
#include "spdk/json.h"
#include "spdk/nvme.h"
#include "spdk/io_channel.h"
#include "spdk/string.h"

#include "spdk_internal/bdev.h"
#include "spdk_internal/log.h"

#include "kvnvme.h"

#define	MPDK_BDEV_MAX_NAME_LENGTH	16

static void bdev_nvme_get_spdk_running_config(FILE *fp);

struct nvme_ctrlr {
	uint64_t		handle;
	char			name[MPDK_BDEV_MAX_NAME_LENGTH];

	/** linked list pointer for device list */
	TAILQ_ENTRY(nvme_ctrlr)	tailq;
};

struct nvme_bdev {
	struct spdk_bdev	disk;
	struct nvme_ctrlr	*nvme_ctrlr;

	TAILQ_ENTRY(nvme_bdev)	link;
};

struct nvme_io_channel {
	uint64_t                *handle;
	struct spdk_poller      *poller;
};

struct nvme_bdev_io {
	kv_pair		kv;
	struct iovec	*iov;
	int 		direction;
};

enum data_direction {
	BDEV_DISK_READ = 0,
	BDEV_DISK_WRITE = 1
};

struct nvme_ctrlr *n_ctrlr;
static TAILQ_HEAD(, nvme_ctrlr)	g_nvme_ctrlrs = TAILQ_HEAD_INITIALIZER(g_nvme_ctrlrs);
static TAILQ_HEAD(, nvme_bdev) g_nvme_bdevs = TAILQ_HEAD_INITIALIZER(g_nvme_bdevs);

static void nvme_ctrlr_create_bdevs(struct nvme_ctrlr *nvme_ctrlr);
static int bdev_nvme_library_init(void);
static void bdev_nvme_library_fini(void);
static int bdev_nvme_queue_cmd(struct nvme_bdev *bdev, int qid,
		    struct nvme_bdev_io *bio,
		    int direction, struct iovec *iov, int iovcnt, uint64_t lba_count,
		    uint64_t offset);

static int bdev_nvme_get_ctx_size(void)
{
	return sizeof(struct nvme_bdev_io);
}

static struct spdk_bdev_module bdev_mpdk_if = {
	.name = "bdev_mpdk",
	.module_init = bdev_nvme_library_init,
	.module_fini = bdev_nvme_library_fini,
	.config_text = bdev_nvme_get_spdk_running_config,
	.config_json = NULL,
	.get_ctx_size = bdev_nvme_get_ctx_size,

};
SPDK_BDEV_MODULE_REGISTER(&bdev_mpdk_if)

static int bdev_nvme_readv(struct nvme_bdev *nbdev, struct spdk_io_channel *ch,
		struct nvme_bdev_io *bio,
		struct iovec *iov, int iovcnt, uint64_t lba_count, uint64_t offset)
{
	SPDK_DEBUGLOG(SPDK_LOG_BDEV_MPDK, "read %lu lbas with offset %#lx\n",
		      lba_count, offset);

	return bdev_nvme_queue_cmd(nbdev, ch->channel_id, bio, BDEV_DISK_READ,
				 iov, iovcnt, lba_count, offset);
}

static int bdev_nvme_writev(struct nvme_bdev *nbdev, struct spdk_io_channel *ch,
		 struct nvme_bdev_io *bio,
		 struct iovec *iov, int iovcnt, uint64_t lba_count, uint64_t offset)
{
	SPDK_DEBUGLOG(SPDK_LOG_BDEV_MPDK, "write %lu lbas with offset %#lx\n",
		      lba_count, offset);

	return bdev_nvme_queue_cmd(nbdev, ch->channel_id, bio, BDEV_DISK_WRITE,
				 iov, iovcnt, lba_count, offset);
}

static int bdev_nvme_flush(struct nvme_bdev *nbdev, struct nvme_bdev_io *bio,
		uint64_t offset, uint64_t nbytes)
{
	spdk_bdev_io_complete(spdk_bdev_io_from_ctx(bio), SPDK_BDEV_IO_STATUS_SUCCESS);

	return 0;
}

static bool bdev_nvme_io_type_supported(void __attribute__((__unused__)) *ctx, enum spdk_bdev_io_type io_type)
{
	switch (io_type) {
	case SPDK_BDEV_IO_TYPE_READ:
	case SPDK_BDEV_IO_TYPE_WRITE:
	case SPDK_BDEV_IO_TYPE_RESET:
	case SPDK_BDEV_IO_TYPE_FLUSH:
	case SPDK_BDEV_IO_TYPE_UNMAP:
		return true;

	default:
		return false;
	}
}

static int bdev_nvme_poll(void *arg)
{
	struct nvme_io_channel *ch = arg;

	kv_process_completion(*(uint64_t*)ch->handle);

	return 0; // Return value not used in the Reactor.c
}

static int bdev_nvme_create_cb(void *io_device, void *ctx_buf)
{
	uint64_t *handle = io_device;
	struct spdk_io_channel *io_channel = spdk_io_channel_from_ctx(ctx_buf);
	struct nvme_io_channel *ch = ctx_buf;

	ch->handle = handle;

	if (io_channel->channel_id == 0) {
		ch->poller = spdk_poller_register(bdev_nvme_poll, ch, 0);
	}

	return 0;
}

static void bdev_nvme_destroy_cb(void *io_device, void *ctx_buf)
{
	struct spdk_io_channel *io_channel = spdk_io_channel_from_ctx(ctx_buf);
	struct nvme_io_channel *ch = ctx_buf;

	if (io_channel->channel_id == 0) spdk_poller_unregister(&ch->poller);
}

static struct spdk_io_channel *
bdev_nvme_get_io_channel_mq(void *ctx, uint32_t channel_id)
{
	struct nvme_bdev *nvme_bdev = ctx;

	return spdk_get_io_channel_mq(&nvme_bdev->nvme_ctrlr->handle, channel_id);
}

static struct spdk_io_channel *
bdev_nvme_get_io_channel(void *ctx)
{
	return bdev_nvme_get_io_channel_mq(ctx, DEFAULT_CHANNEL_ID);
}

static int bdev_nvme_destruct(void *ctx)
{
	struct nvme_bdev *nvme_disk = ctx;
	struct nvme_ctrlr *nvme_ctrlr = nvme_disk->nvme_ctrlr;

	TAILQ_REMOVE(&g_nvme_bdevs, nvme_disk, link);
	free(nvme_disk);

	TAILQ_REMOVE(&g_nvme_ctrlrs, nvme_ctrlr, tailq);
	spdk_io_device_unregister(&nvme_ctrlr->handle, NULL);
	return 0;
}

static int bdev_nvme_unmap(struct nvme_bdev *nbdev, struct spdk_io_channel *ch,
                struct nvme_bdev_io *bio,
                uint64_t offset_blocks,
                uint64_t num_blocks);

static void bdev_nvme_get_buf_cb(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	int ret;

	ret = bdev_nvme_readv((struct nvme_bdev *)bdev_io->bdev->ctxt,
			      ch,
			      (struct nvme_bdev_io *)bdev_io->driver_ctx,
			      bdev_io->u.bdev.iovs,
			      bdev_io->u.bdev.iovcnt,
			      bdev_io->u.bdev.num_blocks,
			      bdev_io->u.bdev.offset_blocks);

	if (ret == 0) {
		return;
	} else if (ret == -ENOMEM) {
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_NOMEM);
	} else {
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static int _bdev_nvme_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		spdk_bdev_io_get_buf(bdev_io, bdev_nvme_get_buf_cb,
					bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen);
		return 0;

	case SPDK_BDEV_IO_TYPE_WRITE:
		return bdev_nvme_writev((struct nvme_bdev *)bdev_io->bdev->ctxt,
					ch,
					(struct nvme_bdev_io *)bdev_io->driver_ctx,
					bdev_io->u.bdev.iovs,
					bdev_io->u.bdev.iovcnt,
					bdev_io->u.bdev.num_blocks,
					bdev_io->u.bdev.offset_blocks);

	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
		return bdev_nvme_unmap((struct nvme_bdev *)bdev_io->bdev->ctxt,
				       ch,
				       (struct nvme_bdev_io *)bdev_io->driver_ctx,
				       bdev_io->u.bdev.offset_blocks,
				       bdev_io->u.bdev.num_blocks);

	case SPDK_BDEV_IO_TYPE_UNMAP:
		return bdev_nvme_unmap((struct nvme_bdev *)bdev_io->bdev->ctxt,
				       ch,
				       (struct nvme_bdev_io *)bdev_io->driver_ctx,
				       bdev_io->u.bdev.offset_blocks,
				       bdev_io->u.bdev.num_blocks);

	case SPDK_BDEV_IO_TYPE_FLUSH:
		return bdev_nvme_flush((struct nvme_bdev *)bdev_io->bdev->ctxt,
				       (struct nvme_bdev_io *)bdev_io->driver_ctx,
				       bdev_io->u.bdev.offset_blocks,
				       bdev_io->u.bdev.num_blocks);
	default:
		return -EINVAL;
	}
	return 0;
}

static void bdev_nvme_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	int rc = _bdev_nvme_submit_request(ch, bdev_io);

	if (rc != 0) {
		if (rc == -ENOMEM) {
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_NOMEM);
		} else {
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		}
	}
}

static const struct spdk_bdev_fn_table nvmelib_fn_table = {
	.destruct		= bdev_nvme_destruct,
	.submit_request		= bdev_nvme_submit_request,
	.io_type_supported	= bdev_nvme_io_type_supported,
	.get_io_channel		= bdev_nvme_get_io_channel,
	.get_io_channel_mq	= bdev_nvme_get_io_channel_mq
};

static void nvme_ctrlr_create_bdevs(struct nvme_ctrlr *nvme_ctrlr) {
	struct nvme_bdev	*bdev;
	uint64_t		handle = nvme_ctrlr->handle;

	bdev = calloc(1, sizeof(*bdev));
	if (!bdev) {
		return;
	}
	
	bdev->nvme_ctrlr = nvme_ctrlr;

	bdev->disk.name = spdk_sprintf_alloc("%sn%d", nvme_ctrlr->name, 1);
	if (!bdev->disk.name) {
		free(bdev);
		return;
	}
	bdev->disk.product_name = "NVMe disk";

	bdev->disk.write_cache = 0;
	bdev->disk.blocklen = kv_nvme_get_sector_size(handle);
	bdev->disk.blockcnt = kv_nvme_get_num_sectors(handle);
	bdev->disk.ctxt = bdev;
	bdev->disk.fn_table = &nvmelib_fn_table;
	bdev->disk.module = &bdev_mpdk_if;
	spdk_bdev_register(&bdev->disk);

	TAILQ_INSERT_TAIL(&g_nvme_bdevs, bdev, link);
}

static int bdev_nvme_library_init(void) {
	int i = 0;
	int nr_handle = 0;
	uint64_t arr_handle[NR_MAX_SSD];

	kv_get_device_handles(&nr_handle, arr_handle);

	n_ctrlr = calloc(nr_handle, sizeof(struct nvme_ctrlr));
	if (n_ctrlr == NULL) {
		SPDK_ERRLOG("Failed to allocate device struct\n");
		return -1;
	}
	for (i = 0; i < nr_handle; i++) {
		n_ctrlr[i].handle = arr_handle[i];

		snprintf(n_ctrlr[i].name, MPDK_BDEV_MAX_NAME_LENGTH, "unvme_bdev%d", i);
		spdk_io_device_register(&n_ctrlr[i].handle, bdev_nvme_create_cb, bdev_nvme_destroy_cb, sizeof(struct nvme_io_channel));

		nvme_ctrlr_create_bdevs(&n_ctrlr[i]);
	
		TAILQ_INSERT_TAIL(&g_nvme_ctrlrs, &n_ctrlr[i], tailq);
	}
	return 0;
}

static void bdev_nvme_library_fini(void) {
	free(n_ctrlr);
}

static void bdev_nvme_queued_done(void *ref, unsigned int sct, unsigned int sc)
{
	struct nvme_bdev_io *bio = ((kv_pair *)ref)->param.private_data;
	struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(bio);

	if(bio->kv.key.key) {
		spdk_free(bio->kv.key.key);
	}

	spdk_bdev_io_complete_nvme_status(bdev_io, sct, sc);
}

static int bdev_nvme_queue_cmd(struct nvme_bdev *bdev, int qid,
		    struct nvme_bdev_io *bio,
		    int direction, struct iovec *iov, int iovcnt, uint64_t lba_count,
		    uint64_t offset)
{
	uint64_t lba = offset;
	int rc = 0;
	uint64_t handle = bdev->nvme_ctrlr->handle;
	uint32_t ss = kv_get_sector_size(handle);
	uint64_t nbytes = lba_count * ss;
	uint32_t key_length = 16;

	bio->direction = direction;
	bio->iov = iov;

	bio->kv.key.key = spdk_dma_zmalloc(key_length, 0, NULL);
	if(!bio->kv.key.key) {
		SPDK_ERRLOG("Memory not available for Key\n");
		return -ENOMEM;
	}

	*(uint64_t*)(bio->kv.key.key) = lba;
	bio->kv.key.length = key_length;

	bio->kv.value.value = iov->iov_base;
	bio->kv.value.length = nbytes;

	bio->kv.param.async_cb = bdev_nvme_queued_done;
	bio->kv.param.private_data = bio;

	if(qid < 0) {
		SPDK_WARNLOG("Could not get the CPU Core ID, Using Default 0");
		qid = 0;
	}

	if (direction == BDEV_DISK_READ) {
		rc = kv_nvme_read_async(handle, qid, &bio->kv);
	} else {
		rc = kv_nvme_write_async(handle, qid, &bio->kv);
	}

	if (rc != 0) {
		spdk_free(bio->kv.key.key);
	}

	if (rc != 0 && rc != -ENOMEM) {
		SPDK_ERRLOG("%s failed: rc = %d\n", direction == BDEV_DISK_READ ? "read" : "write", rc);
	}

	return rc;
}

static int bdev_nvme_unmap(struct nvme_bdev *bdev, struct spdk_io_channel *ch,
		struct nvme_bdev_io *bio,
		uint64_t offset_blocks,
		uint64_t num_blocks)
{
	int rc = 0;
	uint64_t handle = bdev->nvme_ctrlr->handle;
	uint32_t key_length = 16;
	int qid = ch->channel_id;

	bio->kv.key.key = spdk_dma_zmalloc(key_length, 0, NULL);
	if(!bio->kv.key.key) {
		SPDK_ERRLOG("Memory not available for Key\n");
		return -ENOMEM;
	}

	*(uint64_t*)(bio->kv.key.key) = offset_blocks;
	bio->kv.key.length = key_length;

	bio->kv.value.value = NULL;
	bio->kv.value.length = num_blocks;
	bio->kv.param.async_cb = bdev_nvme_queued_done;
	bio->kv.param.private_data = bio;

	if(qid < 0) {
		SPDK_WARNLOG("Could not get the CPU Core ID, Using Default 0");
		qid = 0;
	}

	rc = kv_nvme_delete_async(handle, qid, &bio->kv);

	if (rc != 0) {
		spdk_free(bio->kv.key.key);
	}

	return rc;
}

static void
bdev_nvme_get_spdk_running_config(FILE *fp)
{
	/* TODO */
}

SPDK_LOG_REGISTER_COMPONENT("bdev_mpdk", SPDK_LOG_BDEV_MPDK)
