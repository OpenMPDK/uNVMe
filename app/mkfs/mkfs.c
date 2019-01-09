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
#include "spdk/string.h"

#include "kv_types.h"
#include "kv_apis.h"

#define MIN_APP_HUGEMEM_SIZE_MB (256ULL)

struct spdk_bs_dev *g_bs_dev;
const char *g_bdev_name;
static uint64_t g_cluster_size;

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

	if ((fs == NULL) || (fserrno != 0)) {
		SPDK_ERRLOG("Failed to initialize blobfs.\n");
		spdk_app_stop(fserrno);
		kv_sdk_finalize();
		exit(EXIT_FAILURE);
	}
	event = spdk_event_allocate(0, shutdown_cb, fs, NULL);
	spdk_event_call(event);
}

static void
spdk_mkfs_run(void *arg1, void *arg2)
{
	struct spdk_bdev *bdev;
	struct spdk_blobfs_opts blobfs_opt;

	bdev = spdk_bdev_get_by_name(g_bdev_name);

	if (bdev == NULL) {
		SPDK_ERRLOG("bdev %s not found\n", g_bdev_name);
		spdk_app_stop(-1);
		return;
	}

	printf("Initializing filesystem on bdev %s...\n", g_bdev_name);
	fflush(stdout);

	spdk_fs_opts_init(&blobfs_opt);
	if (g_cluster_size) {
		blobfs_opt.cluster_sz = g_cluster_size;
	}
	g_bs_dev = spdk_bdev_create_bs_dev(bdev, NULL, NULL);
	if (blobfs_opt.cluster_sz) {
		spdk_fs_init(g_bs_dev, &blobfs_opt, NULL, init_cb, NULL);
	} else {
		spdk_fs_init(g_bs_dev, NULL, NULL, init_cb, NULL);
	}
}

static void
mkfs_usage(void)
{
	printf(" -C cluster size\n");
}

static void
mkfs_parse_arg(int ch, char *arg)
{
	bool has_prefix;

	switch (ch) {
	case 'C':
		spdk_parse_capacity(arg, &g_cluster_size, &has_prefix);
		break;
	default:
		break;
	}

}

int main(int argc, char **argv)
{
	struct spdk_app_opts opts = {};
	kv_sdk sdk_opt;
	char* config_json_path;
	uint64_t app_hugemem_size_mb;
	uint64_t fs_cache_size_mb;
	int rc = 0;

	if (argc < 3) {
		fprintf(stderr, "usage: %s <bdevname> <config.json path> \n", argv[0]);
		fprintf(stderr, "ex) %s unvme_bdev0n1 lba_sdk_config.json \n", argv[0]);
		//exit(1);
		g_bdev_name = "unvme_bdev0n1";
		config_json_path = "lba_sdk_config.json";
	}
	else{
		g_bdev_name = argv[1];
		config_json_path = argv[2];
	}

	memset(&sdk_opt, 0, sizeof(kv_sdk));
	rc = kv_sdk_load_option(&sdk_opt, config_json_path);
	if (rc != KV_SUCCESS) {
                fprintf(stderr, "Error while loading JSON configuration.\n");
                exit(EXIT_FAILURE);
	}

	if (sdk_opt.ssd_type != LBA_TYPE_SSD) {
                fprintf(stderr, "This application does not support KV SSD.\n");
                exit(EXIT_FAILURE);
	}

	app_hugemem_size_mb = sdk_opt.app_hugemem_size / ((uint64_t)(1024 * 1024));
	if(MIN_APP_HUGEMEM_SIZE_MB > app_hugemem_size_mb) {
		fprintf(stderr, "app_hugemem_size must be larger than MIN_APP_HUGEMEM_SIZE_MB(%llu)\n", MIN_APP_HUGEMEM_SIZE_MB);
                exit(EXIT_FAILURE);
	} else {
		fs_cache_size_mb = app_hugemem_size_mb - MIN_APP_HUGEMEM_SIZE_MB;
	}

        rc = kv_sdk_init(KV_SDK_INIT_FROM_STR, &sdk_opt);
        if (rc != KV_SUCCESS) {
                fprintf(stderr, "Error while doing sdk init.\n");
                exit(EXIT_FAILURE);
        }

	spdk_app_opts_init(&opts);
	opts.mem_size = app_hugemem_size_mb;
	opts.shutdown_cb = NULL;

	spdk_fs_set_cache_size(fs_cache_size_mb);
	if ((rc = spdk_app_parse_args(argc, argv, &opts, "C:",
				      mkfs_parse_arg, mkfs_usage)) !=
	    SPDK_APP_PARSE_ARGS_SUCCESS) {
		exit(rc);
	}

	rc = spdk_app_start(&opts, spdk_mkfs_run, NULL, NULL);
	spdk_app_fini();

	kv_sdk_finalize();

	return rc;
}
