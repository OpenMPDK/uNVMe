/*-
 *   BSD LICENSE
 *
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

#include "spdk/stdinc.h"

#include "spdk/blobfs.h"
#include "spdk/bdev.h"
#include "spdk/event.h"
#include "spdk/blob_bdev.h"
#include "spdk/log.h"

#include "kv_types.h"
#include "kv_apis.h"

#define MIN_APP_HUGEMEM_SIZE_MB (256ULL)

struct spdk_bs_dev *g_bs_dev;
const char *g_bdev_name;

static void
stop_cb(void *ctx, int fserrno)
{
	spdk_app_stop(0);
}

static void
shutdown_cb(void *arg1, void *arg2)
{
	struct spdk_filesystem *fs = arg1;

	printf("done.\n");
	spdk_fs_unload(fs, stop_cb, NULL);
}

static void
init_cb(void *ctx, struct spdk_filesystem *fs, int fserrno)
{
	struct spdk_event *event;

	event = spdk_event_allocate(0, shutdown_cb, fs, NULL);
	spdk_event_call(event);
}

static void
spdk_mkfs_run(void *arg1, void *arg2)
{
	struct spdk_bdev *bdev;

	bdev = spdk_bdev_get_by_name(g_bdev_name);

	if (bdev == NULL) {
		SPDK_ERRLOG("bdev %s not found\n", g_bdev_name);
		spdk_app_stop(-1);
		return;
	}

	if (!spdk_bdev_claim(bdev, NULL, NULL)) {
		SPDK_ERRLOG("could not claim bdev %s\n", g_bdev_name);
		spdk_app_stop(-1);
		return;
	}

	printf("Initializing filesystem on bdev %s...\n", g_bdev_name);
	fflush(stdout);
	g_bs_dev = spdk_bdev_create_bs_dev(bdev);
	spdk_fs_init(g_bs_dev, NULL, init_cb, NULL);
}

int main(int argc, char **argv)
{
	struct spdk_app_opts opts = {};
	kv_sdk sdk_opt;
	char* config_json_path;
	uint64_t app_hugemem_size_mb;
	uint64_t fs_cache_size_mb;
	int rc;

	if (argc < 3) {
		fprintf(stderr, "usage: %s <bdevname> <config.json path> \n", argv[0]);
		fprintf(stderr, "ex) %s unvme_bdev0n1 lba_sdk_config.json \n", argv[0]);
		exit(1);
	}

	g_bdev_name = argv[1];
	config_json_path = argv[2];
	kv_sdk_load_option(&sdk_opt, config_json_path);

	app_hugemem_size_mb = sdk_opt.app_hugemem_size / ((uint64_t)(1024 * 1024));
	if(MIN_APP_HUGEMEM_SIZE_MB > app_hugemem_size_mb) {
		fprintf(stderr, "app_hugemem_size must be larger than MIN_APP_HUGEMEM_SIZE_MB(%llu)\n", MIN_APP_HUGEMEM_SIZE_MB);
		exit(1);
	} else {
		fs_cache_size_mb = app_hugemem_size_mb - MIN_APP_HUGEMEM_SIZE_MB;
	}

	spdk_app_opts_init(&opts);
	opts.dpdk_mem_size = app_hugemem_size_mb;
	opts.shutdown_cb = NULL;
	opts.init_from = KV_SDK_INIT_FROM_JSON;
	opts.options = (char*)&sdk_opt;

        rc = kv_sdk_init(opts.init_from, config_json_path);
        if (rc != KV_SUCCESS) {
                SPDK_ERRLOG("Error while doing sdk init.\n");
                exit(EXIT_FAILURE);
        }

	spdk_app_init(&opts);

	spdk_fs_set_cache_size(fs_cache_size_mb);

	spdk_app_start(spdk_mkfs_run, NULL, NULL);
	spdk_app_fini();

	kv_sdk_finalize();

	return 0;
}
