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

extern int32_t lba_nvme_process_all_cqs_thread(void *arg);
extern int32_t lba_nvme_process_cq_thread(void *arg);

extern int32_t kv_nvme_process_all_cqs_thread(void *arg);
extern int32_t kv_nvme_process_cq_thread(void *arg);

static int g_kvdd_ref_count = 0;
static TAILQ_HEAD(, kv_nvme) g_nvme_devices = TAILQ_HEAD_INITIALIZER(g_nvme_devices);
static pthread_mutex_t g_init_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_t g_aer_thread;

static bool probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid, struct spdk_nvme_ctrlr_opts *opts) {
        kv_nvme_t *nvme = NULL;

        ENTER();

        KVNVME_DEBUG("Probing the controller %s", trid->traddr);

        nvme = (kv_nvme_t *)cb_ctx;

        if(nvme->options) {
                if(nvme->options->queue_depth) {
                        opts->io_queue_size = nvme->options->queue_depth;
                        opts->io_queue_requests = nvme->options->queue_depth;
                } else {
                        opts->io_queue_size = DEFAULT_IO_QUEUE_DEPTH;
                        opts->io_queue_requests = DEFAULT_IO_QUEUE_DEPTH;
                }
        } else {
                opts->io_queue_size = DEFAULT_IO_QUEUE_DEPTH;
                opts->io_queue_requests = DEFAULT_IO_QUEUE_DEPTH;
        }

        KVNVME_DEBUG("I/O Queue Depth: %d", opts->io_queue_size-1);

        LEAVE();
        return true;
}

static void attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid, struct spdk_nvme_ctrlr *ctrlr,
                const struct spdk_nvme_ctrlr_opts *opts) {
        kv_nvme_t *nvme = NULL;

        ENTER();

        KVNVME_DEBUG("Attaching the controller %s", trid->traddr);

        nvme = (kv_nvme_t *)cb_ctx;

        nvme->ctrlr = ctrlr;
        nvme->num_io_queues = opts->num_io_queues;
        TAILQ_INSERT_TAIL(&g_nvme_devices, nvme, tailq);

        LEAVE();
}

static kv_nvme_t *get_kv_nvme_from_bdf(const char *bdf) {
        kv_nvme_t *nvme = NULL;
        char kv_nvme_traddr[SPDK_NVMF_TRADDR_MAX_LEN];

        ENTER();

        strncpy(kv_nvme_traddr, TRANSPORT_ID_STRING, TRANSPORT_ID_STRING_LEN);
        strncpy(kv_nvme_traddr + TRANSPORT_ID_STRING_LEN, bdf, BDF_STRING_LEN);

        KVNVME_DEBUG("NVMe Device Transport Address: %s", kv_nvme_traddr);

        TAILQ_FOREACH(nvme, &g_nvme_devices, tailq) {
                if(!strcmp(nvme->traddr, kv_nvme_traddr)) {
                        KVNVME_DEBUG("Found the Matching NVMe Device");
                        return nvme;
                }
        }

        KVNVME_ERR("Could not Find a Matching NVMe Device");

        LEAVE();
        return NULL;
}

static void spdk_aer_cb_fn(void *aer_cb_arg, const struct spdk_nvme_cpl *cpl) {
        kv_nvme_t *nvme = NULL;

        ENTER();

        nvme = (kv_nvme_t *)aer_cb_arg;

        if(nvme) {
                if(nvme->aer_cb_fn) {
                        KVNVME_DEBUG("Triggering Application's AER Callback Function");

                        nvme->aer_cb_fn(nvme->aer_cb_arg, cpl->cdw0, (unsigned int)cpl->status.sc);

                        KVNVME_DEBUG("Triggered Application's AER Callback Function");
                }
        }

        LEAVE();
}

static void process_all_nvme_aers_thread(void *arg) {
        kv_nvme_t *nvme = NULL;

        while(1) {
                TAILQ_FOREACH(nvme, &g_nvme_devices, tailq) {
                        if(nvme) {
                                if(nvme->ctrlr) {
                                        spdk_nvme_ctrlr_process_admin_completions(nvme->ctrlr);
                                }
                        }
                }
                sleep(1);
        }
}

static void kv_nvme_remove_hugepage_info(void){
	//remove hugepage info files used to init SPDK/DPDK
        const char *directory = "/var/run";
        const char *home_dir = getenv("HOME");
        char spdk_prefix[32] = {0, };
        char info_path[256] = {0, };

        sprintf(spdk_prefix, "spdk%d", getpid());

        if (getuid() != 0 && home_dir != NULL){
                directory = home_dir;
        }

        sprintf(info_path, "%s/.%s_config", directory, spdk_prefix);
        remove(info_path);
        sprintf(info_path, "%s/.%s_hugepage_info", directory, spdk_prefix);
        remove(info_path);
}

static void kv_nvme_remove_socket(void){
	const char *directory = "/var/run";
	const char *xdg_runtime_dir = getenv("XDG_RUNTIME_DIR");
	const char *fallback = "/tmp";
	char spdk_prefix[32] = {0};
	char cmd[512] = {0};

	sprintf(spdk_prefix, "spdk%d", getpid());

	if (getuid() != 0) {
                if (xdg_runtime_dir != NULL) {
                        directory = xdg_runtime_dir;
		} else {
			directory = fallback;
		}
	}
	sprintf(cmd, "rm -rf %s/dpdk/%s", directory, spdk_prefix);
	int ret = system(cmd);
	KVNVME_DEBUG("remove socket info: %d\n", ret);
}

int _kv_env_init(uint32_t process_mem_size_mb, struct spdk_env_opts* opts){
        int ret = KV_ERR_DD_INVALID_PARAM;

        pthread_mutex_lock(&g_init_mutex);
        if(g_kvdd_ref_count++ > 0){
                KVNVME_DEBUG("KV DD is already initialized\n");
                pthread_mutex_unlock(&g_init_mutex);
                return KV_SUCCESS;
        }
        pthread_mutex_unlock(&g_init_mutex);

	if(opts){
		ret = spdk_env_init(opts);
		KVNVME_DEBUG("mem_size_mb: %u shm_id: %u\n",opts->mem_size, opts->shm_id);
	}
	else{
		struct spdk_env_opts local_opts;
		spdk_env_opts_init(&local_opts);
		local_opts.name = "KV_Interface";
		local_opts.mem_size = process_mem_size_mb;
		local_opts.shm_id = getpid();
		ret = spdk_env_init(&local_opts);
		KVNVME_DEBUG("mem_size_mb: %u shm_id: %u\n",local_opts.mem_size, local_opts.shm_id);
	}

	if(ret) {
		KVNVME_ERR("spdk_env_init failed");
		return ret;
	}

        KVNVME_DEBUG("Initialized the KV API Environment");

        ENTER();
/*
        ret = system("NRHUGE=4096 ./user/spdk/scripts/setup.sh");

        if(ret) {
                KVNVME_ERR("Could not Initialize the SPDK Environment");

                LEAVE();
                return ret;
        }
*/
        ret = pthread_create(&g_aer_thread, NULL, (void *)&process_all_nvme_aers_thread, NULL);

        if(ret) {
                KVNVME_ERR("Could not create AERs Processing Thread");
                return ret;
        }

        pthread_setname_np(g_aer_thread, "AERs Processing Thread");

        KVNVME_DEBUG("Done\n");
        LEAVE();

        return ret;
}

int kv_env_init(uint32_t process_mem_size_mb){
	return _kv_env_init(process_mem_size_mb,NULL);
}

int kv_env_init_with_spdk_opts(struct spdk_env_opts* opts){
  if (opts == NULL)
    return KV_ERR_DD_INVALID_PARAM;
  return _kv_env_init(opts->mem_size,opts);
}

int kv_nvme_io_queue_type(uint64_t handle, int core_id) {
        int ret = KV_ERR_DD_INVALID_PARAM;
        kv_nvme_t *nvme = NULL;

        ENTER();

        if(!handle) {
                KVNVME_ERR("Invalid handle passed");

                LEAVE();
                return ret;
        }

        TAILQ_FOREACH(nvme, &g_nvme_devices, tailq) {
          if((uint64_t)nvme == handle) {
            break;
          }
        }
        if((uint64_t)nvme != handle) {
          KVNVME_ERR("Could not Find a Matching NVMe Device");
          LEAVE();
          return ret;
        }

        nvme = (kv_nvme_t *)handle;
        if(core_id < 0) {
                KVNVME_ERR("Invalid I/O Queue ID passed");

                LEAVE();
                return ret;
        }

        if(nvme->io_queue_type[core_id] == SYNC_IO_QUEUE) {
                KVNVME_DEBUG("I/O Queue Type is Sync");

                LEAVE();
                return SYNC_IO_QUEUE;
        } else if(nvme->io_queue_type[core_id] == ASYNC_IO_QUEUE) {
                KVNVME_DEBUG("I/O Queue Type is Async");

                LEAVE();
                return ASYNC_IO_QUEUE;
        } else {
                KVNVME_ERR("There is no Valid I/O Queue for the passed CPU Core ID");

                LEAVE();
                return ret;
        }
}


int kv_nvme_register_aer_callback(uint64_t handle, kv_aer_cb_fn_t aer_cb_fn, void *aer_cb_arg) {
        int ret = KV_ERR_DD_INVALID_PARAM;
        kv_nvme_t *nvme = NULL;

        ENTER();

        if(!handle) {
                KVNVME_ERR("Invalid handle passed");

                LEAVE();
                return ret;
        }

        nvme = (kv_nvme_t *)handle;

        nvme->aer_cb_fn = aer_cb_fn;
        nvme->aer_cb_arg = aer_cb_arg;

        spdk_nvme_ctrlr_register_aer_callback(nvme->ctrlr, spdk_aer_cb_fn, nvme);

        LEAVE();
        return 0;
}

int _check_ssd_type(unsigned int ssd_type) {
  int ret = KV_SUCCESS;
  switch(ssd_type) {
  case KV_TYPE_SSD:
  case LBA_TYPE_SSD:
    ret = KV_SUCCESS;
    break;
  default:
    ret = KV_ERR_DD_UNSUPPORTED_CMD;
    break;
  }
  return ret;
}

int kv_nvme_init(const char *bdf, kv_nvme_io_options *options, unsigned int ssd_type) {
        int ret = KV_ERR_DD_INVALID_PARAM, num_ns = 0;
        unsigned long long cpu_id = 0, queue_id = 0, cpu_core_mask = 0, sync_mask = 0, num_async_queues = 0, num_cq_threads = 0, cq_thread_mask = 0;
        kv_nvme_t *nvme = NULL;
        unsigned long long def_core_mask = 1, def_sync_mask = 1, def_num_cq_threads = 1;
        char kv_nvme_traddr[SPDK_NVMF_TRADDR_MAX_LEN];
        unsigned int thread_id = 0, cq_threads_cores[MAX_CPU_CORES] = {0};
        unsigned int *queues_per_thread = NULL;

        ENTER();
        if (bdf == NULL || options == NULL ) {
          KVNVME_ERR("Use invalid parameter to initialize the KV NVMe Device");
          LEAVE();
          return ret;
        }

        if (_check_ssd_type(ssd_type)) {
          KVNVME_ERR("Unsupported ssd type: 0x%x inputted.", ssd_type);
          LEAVE();
          return KV_ERR_DD_UNSUPPORTED_CMD;
        }

        nvme = calloc(1, sizeof(kv_nvme_t));
        if(!nvme) {
                KVNVME_ERR("Could not allocate memory for a KV NVMe Device Handle");

                LEAVE();
                return KV_ERR_DD_NO_AVAILABLE_RESOURCE;
        }

        nvme->options = calloc(1, sizeof(kv_nvme_io_options));
        if(!nvme->options) {
                KVNVME_ERR("Could not allocate memory for a NVMe Device option");
                LEAVE();
                return KV_ERR_DD_NO_AVAILABLE_RESOURCE;
        }
        memcpy(nvme->options, options, sizeof(kv_nvme_io_options));

        strncpy(kv_nvme_traddr, TRANSPORT_ID_STRING, TRANSPORT_ID_STRING_LEN);
        strncpy(kv_nvme_traddr + TRANSPORT_ID_STRING_LEN, bdf, BDF_STRING_LEN);

        KVNVME_DEBUG("NVMe Device Transport Address: %s", kv_nvme_traddr);

        ret = spdk_nvme_transport_id_parse(&nvme->trid, kv_nvme_traddr);

        if(ret) {
                KVNVME_ERR("Could not parse the Transport ID of the KV NVMe Device");
                free(nvme);

                LEAVE();
                return ret;
        }

        strncpy(nvme->traddr, kv_nvme_traddr, SPDK_NVMF_TRADDR_MAX_LEN);

	nvme->options->queue_depth++;
        ret = spdk_nvme_probe(&nvme->trid, nvme, probe_cb, attach_cb, NULL);

        if(ret) {
                KVNVME_ERR("SPDK NVMe Probe failed, ret = %d", ret);
                free(nvme);

                LEAVE();
                return ret;
        }

        if(!nvme->ctrlr) {
                KVNVME_ERR("Cannot Use the Requested Device %s", bdf);
                free(nvme);

                LEAVE();
                return KV_ERR_DD_NO_DEVICE;
        }

        num_ns = spdk_nvme_ctrlr_get_num_ns(nvme->ctrlr);

        KVNVME_DEBUG("Total number of Namespaces in the controller: %d", num_ns);

        nvme->ns = spdk_nvme_ctrlr_get_ns(nvme->ctrlr, 1);

        if(!nvme->ns) {
                KVNVME_ERR("Could not get the Namespace for the KV NVMe Device");
                ret = spdk_nvme_detach(nvme->ctrlr);
                free(nvme);

                LEAVE();
                return ret;
        }

        KVNVME_DEBUG("core_mask: 0x%llx, sync_mask: 0x%llx, num_cq_threads: 0x%llx, cq_thread_mask: 0x%llx",
                        (long long unsigned int) options->core_mask, (long long unsigned int) options->sync_mask,
                        (long long unsigned int) options->num_cq_threads, (long long unsigned int) options->cq_thread_mask);

        if(options && options->core_mask) {
                cpu_core_mask = options->core_mask;
                sync_mask = options->sync_mask;
        } else {
                cpu_core_mask = def_core_mask;
                sync_mask = def_sync_mask;
        }

        nvme->qpairs = calloc(MAX_CPU_CORES, sizeof(struct spdk_nvme_qpair *));

        if(!nvme->qpairs) {
                KVNVME_ERR("Could not Allocate the I/O Queues Holder");
                ret = spdk_nvme_detach(nvme->ctrlr);
                free(nvme);

                LEAVE();
                return ret;
        }

        nvme->async_qpairs = calloc(MAX_CPU_CORES, sizeof(struct spdk_nvme_qpair *));

        if(!nvme->async_qpairs) {
                KVNVME_ERR("Could not Allocate the Async I/O Queues Holder");
                free(nvme->qpairs);

                ret = spdk_nvme_detach(nvme->ctrlr);
                free(nvme);

                LEAVE();
                return ret;
        }

        nvme->io_queue_type = calloc(MAX_CPU_CORES, sizeof(unsigned int));

        if(!nvme->io_queue_type) {
                KVNVME_ERR("Could not Allocate the Sync Mask for the I/O Queues");

                free(nvme->async_qpairs);
                free(nvme->qpairs);

                ret = spdk_nvme_detach(nvme->ctrlr);
                free(nvme);

                LEAVE();
                return ret;
        }

        for(queue_id = 0; queue_id < MAX_CPU_CORES; queue_id++) {

                if(cpu_core_mask & (1ULL << queue_id)) {
			//No Queue option
                        nvme->qpairs[queue_id] = spdk_nvme_ctrlr_alloc_io_qpair(nvme->ctrlr, NULL, 0);
                } else {
                        nvme->qpairs[queue_id] = NULL;
                }

                if(!nvme->qpairs[queue_id] && (cpu_core_mask & (1ULL << queue_id))) {
                        unsigned int tmp_queue_id;

                        KVNVME_ERR("Could not Allocate the I/O Queue for the CPU Core ID: %llu", queue_id);

                        for(tmp_queue_id = 0; tmp_queue_id < queue_id; tmp_queue_id++) {
                                if(nvme->qpairs[tmp_queue_id]) {
                                        spdk_nvme_ctrlr_free_io_qpair(nvme->qpairs[tmp_queue_id]);
                                }
                        }

                        free(nvme->io_queue_type);
                        free(nvme->async_qpairs);
                        free(nvme->qpairs);

                        ret = spdk_nvme_detach(nvme->ctrlr);
                        free(nvme);

                        LEAVE();
                        return ret;
                } else if (nvme->qpairs[queue_id] && (cpu_core_mask & (1ULL << queue_id))) {
                        KVNVME_DEBUG("Successfully Created I/O Queue for the CPU Core ID: %llu with Address: 0x%llx", queue_id, (unsigned long long)nvme->qpairs[queue_id]);
                        if(sync_mask & (1ULL << queue_id)) {
                                nvme->io_queue_type[queue_id] = SYNC_IO_QUEUE;
                        } else {
                                nvme->io_queue_type[queue_id] = ASYNC_IO_QUEUE;

                                nvme->async_qpairs[num_async_queues++] = nvme->qpairs[queue_id];
                        }
                }
        }

        if(ssd_type == LBA_TYPE_SSD) {
                nvme->sector_size = spdk_nvme_ns_get_sector_size(nvme->ns);

                KVNVME_DEBUG("Sector Size of the NVMe SSD: 0x%x Bytes", nvme->sector_size);

                KVNVME_DEBUG("LBA Type SSD. Registered LBA NVMe Device Operations");

                nvme->dev_ops.write = _lba_nvme_write;
                nvme->dev_ops.read = _lba_nvme_read;
                nvme->dev_ops.delete = _lba_nvme_delete;

                nvme->dev_ops.write_async = _lba_nvme_write_async;
                nvme->dev_ops.read_async = _lba_nvme_read_async;
                nvme->dev_ops.delete_async = _lba_nvme_delete_async;
                nvme->dev_ops.format = _lba_nvme_format;
                nvme->dev_ops.get_used_size = _lba_nvme_get_used_size;
                nvme->dev_ops.exist = NULL;
                nvme->dev_ops.exist_async = NULL;
                nvme->dev_ops.iterate_open = NULL;
                nvme->dev_ops.iterate_close = NULL;
                nvme->dev_ops.iterate_read = NULL;
                nvme->dev_ops.iterate_read_async = NULL;

        } else if(ssd_type == KV_TYPE_SSD) {
                KVNVME_DEBUG("KV Type SSD. Registered KV NVMe Device Operations");

                nvme->dev_ops.write = _kv_nvme_store;
                nvme->dev_ops.read = _kv_nvme_retrieve;
                nvme->dev_ops.delete = _kv_nvme_delete;

                nvme->dev_ops.write_async = _kv_nvme_store_async;
                nvme->dev_ops.read_async = _kv_nvme_retrieve_async;
                nvme->dev_ops.delete_async = _kv_nvme_delete_async;
                nvme->dev_ops.format = _kv_nvme_format;
                nvme->dev_ops.get_used_size = _kv_nvme_get_used_size;
                nvme->dev_ops.exist = _kv_nvme_exist;
                nvme->dev_ops.exist_async = _kv_nvme_exist_async;

                nvme->dev_ops.iterate_open = _kv_nvme_iterate_open;
                nvme->dev_ops.iterate_close = _kv_nvme_iterate_close;
                nvme->dev_ops.iterate_read = _kv_nvme_iterate_read;
                nvme->dev_ops.iterate_read_async = _kv_nvme_iterate_read_async;

        } else {
                KVNVME_ERR("Invalid SSD Type. Did not Register any Device Operations. De-Initializing the Device");

                for(queue_id = 0; queue_id < MAX_CPU_CORES; queue_id++) {
                        if(nvme->qpairs[queue_id]) {
                                spdk_nvme_ctrlr_free_io_qpair(nvme->qpairs[queue_id]);
                        }
                }

                free(nvme->io_queue_type);
                free(nvme->async_qpairs);
                free(nvme->qpairs);

                ret = spdk_nvme_detach(nvme->ctrlr);
                free(nvme);

                LEAVE();
                return ret;
        }

        if(options && options->num_cq_threads) {
                num_cq_threads = options->num_cq_threads;
                cq_thread_mask = options->cq_thread_mask;

                KVNVME_DEBUG("No. of CQ Threads : %llu, No. of Async I/O Queues: %llu", num_cq_threads, num_async_queues);

/*
                if(num_cq_threads > num_async_queues) {
                        KVNVME_ERR("No. of CQ Threads cannot be more than the No. of Async I/O Queues");


                        for(queue_id = 0; queue_id < MAX_CPU_CORES; queue_id++) {
                                if(nvme->qpairs[queue_id]) {
                                        spdk_nvme_ctrlr_free_io_qpair(nvme->qpairs[queue_id]);
                                }
                        }

                        free(nvme->io_queue_type);
                        free(nvme->async_qpairs);
                        free(nvme->qpairs);

                        ret = spdk_nvme_detach(nvme->ctrlr);
                        free(nvme);

                        LEAVE();
                        return ret;
                }
*/
        } else {
                num_cq_threads = def_num_cq_threads;
                KVNVME_WARN("No. of CQ Threads from Application is not passed, Creating only one CQ thread");
        }

        nvme->num_cq_threads = num_cq_threads;


        if(nvme->num_cq_threads == 1) {
                unsigned int last_cpu_id = 0;
                process_cq_thread_arg_t *cq_thread_args = calloc(1, sizeof(process_cq_thread_arg_t));

                if(cq_thread_mask) {
                        for(cpu_id = 0; cpu_id < MAX_CPU_CORES; cpu_id++) {
                                if(cq_thread_mask & (1ULL << cpu_id)) {
                                        last_cpu_id = cpu_id;
                                }
                        }
                } else {
/*
                        for(cpu_id = 0; cpu_id < MAX_CPU_CORES; cpu_id++) {
                                if(cpu_core_mask & (1ULL << cpu_id)) {
                                        last_cpu_id = cpu_id;
                                }
                        }
*/
                        last_cpu_id = CQ_THREAD_DEFAULT_CPU;
                }

                nvme->stop_process_all_cqs = 0;

                cq_thread_args->nvme = nvme;
                cq_thread_args->cpu_id = last_cpu_id;

		if(ssd_type == KV_TYPE_SSD){
                	ret = pthread_create(&nvme->process_all_cqs_thread, NULL, (void *)&kv_nvme_process_all_cqs_thread, cq_thread_args);
		}
		else{
			if(cq_thread_mask != 0){
				ret = pthread_create(&nvme->process_all_cqs_thread, NULL, (void *)&lba_nvme_process_all_cqs_thread, cq_thread_args);
			}
			else{
				nvme->num_cq_threads = 0;
				KVNVME_ERR("Warning: CQ handling thread is not created, an application generating async io must handle CQ process by calling kv_process_completion()");
			}
		}

                if(ret) {
                        KVNVME_ERR("Could not create the Thread to Process the Completion Queues of the NVMe Device");

                        free(cq_thread_args);

                        for(queue_id = 0; queue_id < MAX_CPU_CORES; queue_id++) {
                                if(nvme->qpairs[queue_id]) {
                                        spdk_nvme_ctrlr_free_io_qpair(nvme->qpairs[queue_id]);
                                }
                        }

                        free(nvme->io_queue_type);
                        free(nvme->async_qpairs);
                        free(nvme->qpairs);

                        ret = spdk_nvme_detach(nvme->ctrlr);
                        free(nvme);

                        LEAVE();
                        return ret;
                }

                pthread_setname_np(nvme->process_all_cqs_thread, "NVMe Process All CQs Thread");

        } else {
                unsigned int num_cpu_cores = 0;
                process_cq_thread_arg_t **cq_thread_args = NULL;

                for(cpu_id = 0; cpu_id < MAX_CPU_CORES; cpu_id++) {
                        if(cq_thread_mask & (1ULL << cpu_id)) {
                                cq_threads_cores[num_cpu_cores++] = cpu_id;
                        }
                }

                cq_thread_args = calloc(num_cq_threads, sizeof(process_cq_thread_arg_t *));

                if(!cq_thread_args) {
                        KVNVME_ERR("Could not Allocate the Arguments for the CQ Processing Threads");

                        for(queue_id = 0; queue_id < MAX_CPU_CORES; queue_id++) {
                                if(nvme->qpairs[queue_id]) {
                                        spdk_nvme_ctrlr_free_io_qpair(nvme->qpairs[queue_id]);
                                }
                        }

                        free(nvme->io_queue_type);
                        free(nvme->async_qpairs);
                        free(nvme->qpairs);

                        ret = spdk_nvme_detach(nvme->ctrlr);
                        free(nvme);

                        LEAVE();
                        return ret;
                }

                queues_per_thread = calloc(num_cq_threads, sizeof(unsigned int));

                if(!queues_per_thread) {
                        KVNVME_ERR("Could not Allocate the Queues per Thread Array");

                        free(cq_thread_args);

                        for(queue_id = 0; queue_id < MAX_CPU_CORES; queue_id++) {
                                if(nvme->qpairs[queue_id]) {
                                        spdk_nvme_ctrlr_free_io_qpair(nvme->qpairs[queue_id]);
                                }
                        }

                        free(nvme->io_queue_type);
                        free(nvme->async_qpairs);
                        free(nvme->qpairs);

                        ret = spdk_nvme_detach(nvme->ctrlr);
                        free(nvme);

                        LEAVE();
                        return ret;
                }

                while(num_async_queues) {
                        for(thread_id = 0; (thread_id < num_cq_threads) && num_async_queues; thread_id++) {
                                queues_per_thread[thread_id]++;
                                num_async_queues--;
                        }
                }

                for(thread_id = 0; thread_id < num_cq_threads; thread_id++) {
                        KVNVME_DEBUG("Queues for Thread %d : %d", thread_id, queues_per_thread[thread_id]);
                }

                unsigned int async_qpair_start_index = 0;

                for(thread_id = 0; thread_id < num_cq_threads; thread_id++) {
                        nvme->stop_process_cq[thread_id] = 0;

                        cq_thread_args[thread_id] = calloc(1, sizeof(process_cq_thread_arg_t));

                        if(!cq_thread_args[thread_id]) {
                                KVNVME_ERR("Could not Allocate the CQ Thread Arguments structure for the Thread ID %d", thread_id);

                                free(queues_per_thread);

                                for(queue_id = 0; queue_id < MAX_CPU_CORES; queue_id++) {
                                        if(nvme->qpairs[queue_id]) {
                                                spdk_nvme_ctrlr_free_io_qpair(nvme->qpairs[queue_id]);
                                        }

                                        if(cq_thread_args[queue_id]) {
                                                free(cq_thread_args[queue_id]);
                                        }
                                }

                                free(cq_thread_args);

                                free(nvme->io_queue_type);
                                free(nvme->async_qpairs);
                                free(nvme->qpairs);

                                ret = spdk_nvme_detach(nvme->ctrlr);
                                free(nvme);

                                LEAVE();
                                return KV_ERR_DD_NO_AVAILABLE_RESOURCE;
                        }

                        cq_thread_args[thread_id]->nvme = nvme;
                        cq_thread_args[thread_id]->thread_id = thread_id;
                        cq_thread_args[thread_id]->cpu_id = cq_threads_cores[thread_id];
                        cq_thread_args[thread_id]->async_qpair_start_index = async_qpair_start_index;
                        cq_thread_args[thread_id]->num_async_qpairs = queues_per_thread[thread_id];

                        KVNVME_DEBUG("Thread ID: %d, async_qpair_start_index: %d, num_async_qpairs: %d", thread_id, cq_thread_args[thread_id]->async_qpair_start_index, cq_thread_args[thread_id]->num_async_qpairs);

			if(ssd_type == KV_TYPE_SSD){
                        	ret = pthread_create(&nvme->process_cq_thread[thread_id], NULL, (void *)&kv_nvme_process_cq_thread, cq_thread_args[thread_id]);
			}
			else{
				if(cq_thread_mask != 0){
					ret = pthread_create(&nvme->process_cq_thread[thread_id], NULL, (void *)&lba_nvme_process_cq_thread, cq_thread_args[thread_id]);
				}
				else{
					KVNVME_ERR("Warning: CQ handling thread is not created, an application generating async io must handle CQ process by calling kv_process_completion()");
				}
			}

                        if(ret) {
                                KVNVME_ERR("Could not create the Thread to Process the Async Completions, for thread id %d of the NVMe Device", thread_id);

                                free(queues_per_thread);

                                for(queue_id = 0; queue_id < MAX_CPU_CORES; queue_id++) {
                                        if(nvme->qpairs[queue_id]) {
                                                spdk_nvme_ctrlr_free_io_qpair(nvme->qpairs[queue_id]);
                                        }

                                        if(cq_thread_args[queue_id]) {
                                                free(cq_thread_args[queue_id]);
                                        }
                                }

                                free(cq_thread_args);

                                free(nvme->io_queue_type);
                                free(nvme->async_qpairs);
                                free(nvme->qpairs);

                                ret = spdk_nvme_detach(nvme->ctrlr);
                                free(nvme);

                                LEAVE();
                                return ret;
                        }

                        async_qpair_start_index += queues_per_thread[thread_id];
                }

                free(queues_per_thread);
                free(cq_thread_args);
        }

        LEAVE();
        return 0;
}

uint64_t kv_nvme_open(const char *bdf) {
        uint64_t handle = 0;
        kv_nvme_t *nvme = NULL;

        ENTER();

        if(!bdf) {
                KVNVME_ERR("Invalid BDF passed");

                LEAVE();
                return handle;
        }

        nvme = get_kv_nvme_from_bdf(bdf);

        if(!nvme) {
                KVNVME_ERR("Could not get a Matching KV NVMe Device for the passed BDF");

                LEAVE();
                return handle;
        }

        handle = (uint64_t)nvme;

        LEAVE();
        return handle;
}

int kv_nvme_close(uint64_t handle) {
  ENTER();

  if(!handle) {
    KVNVME_ERR("Invalid handle passed");
    LEAVE();
    return KV_ERR_DD_INVALID_PARAM;
  }

  kv_nvme_t *nvme = NULL;
  TAILQ_FOREACH(nvme, &g_nvme_devices, tailq) {
    if((uint64_t)nvme == handle) {
      KVNVME_DEBUG("Found the Matching NVMe Device");
      LEAVE();
      return KV_SUCCESS;
    }
  }
  KVNVME_ERR("Could not Find a Matching NVMe Device");

  LEAVE();
  return KV_ERR_DD_NO_DEVICE;
}

int kv_nvme_finalize(char *bdf) {
        int ret = KV_ERR_DD_INVALID_PARAM;
        unsigned int queue_id, thread_id = 0;
        kv_nvme_t *nvme = NULL;

	usleep(100);

        ENTER();

        if(!bdf) {
                KVNVME_ERR("Invalid BDF passed");

                LEAVE();
                return ret;
        }

        nvme = get_kv_nvme_from_bdf(bdf);
        if(!nvme) {
                KVNVME_ERR("Could not get a Matching KV NVMe Device for the passed BDF");

                LEAVE();
                return KV_ERR_DD_NO_DEVICE;
        }

        if(nvme->num_cq_threads == 1) {
                nvme->stop_process_all_cqs = 1;
        } else {
                for(thread_id = 0; thread_id < nvme->num_cq_threads; thread_id++) {
                        nvme->stop_process_cq[thread_id] = 1;
                }
        }

        if(nvme->num_cq_threads == 1) {
                pthread_join(nvme->process_all_cqs_thread, NULL);
        } else {
                for(thread_id = 0; thread_id < nvme->num_cq_threads; thread_id++) {
                        pthread_join(nvme->process_cq_thread[thread_id], NULL);
                }
        }

        for(queue_id = 0; queue_id < MAX_CPU_CORES; queue_id++) {
                if(nvme->qpairs[queue_id]) {
                        spdk_nvme_ctrlr_free_io_qpair(nvme->qpairs[queue_id]);
                }
        }

        free(nvme->io_queue_type);
        free(nvme->async_qpairs);
        free(nvme->qpairs);
        free(nvme->options);

        ret = spdk_nvme_detach(nvme->ctrlr);
        if(ret) {
                KVNVME_ERR("Could not detach the KV NVMe Device from the Driver");

                LEAVE();
                return ret;
        }

        TAILQ_REMOVE(&g_nvme_devices, nvme, tailq);
        free(nvme);

        kv_nvme_remove_hugepage_info();
        kv_nvme_remove_socket();

        LEAVE();
        KVNVME_DEBUG("Done\n");
        return 0;
}

int kv_nvme_is_dd_initialized(){
        return (g_kvdd_ref_count > 0) ? 1 : 0;
}


void kv_nvme_process_completion(uint64_t handle){
        kv_nvme_t *nvme = (kv_nvme_t*)handle;
        struct spdk_nvme_qpair *qpair = NULL;
        unsigned int queue_is_async = 0;
        unsigned int queue_id = 0;

        if (nvme == NULL) {
          KVNVME_ERR("Invalid handle passed");
          return;
        }
        for(queue_id = 0; queue_id < MAX_CPU_CORES; queue_id++) {
                qpair = nvme->qpairs[queue_id];
                queue_is_async = ((nvme->io_queue_type[queue_id] == ASYNC_IO_QUEUE) ? 1 : 0);

                if(qpair && queue_is_async) {
                        pthread_spin_lock(&qpair->cq_lock);
                        spdk_nvme_qpair_process_completions(qpair, 0);
                        pthread_spin_unlock(&qpair->cq_lock);
                }

                queue_is_async = 0;
        }
}

void kv_nvme_process_completion_queue(uint64_t handle, uint32_t queue_id){
        kv_nvme_t *nvme = (kv_nvme_t*)handle;
        struct spdk_nvme_qpair *qpair = NULL;
        unsigned int queue_is_async = 0;
  if (nvme == NULL) {
    KVNVME_ERR("Invalid handle passed");
    return;
  }
  if(queue_id >= MAX_CPU_CORES) {
    int qid = sched_getcpu();
    if(qid >= 0) {
      queue_id = qid;
    } else {
      KVNVME_WARN("Could not get the CPU Core ID, Using Default 0");
      queue_id = 0;
    }
  }
	qpair = nvme->qpairs[queue_id];
	queue_is_async = ((nvme->io_queue_type[queue_id] == ASYNC_IO_QUEUE) ? 1 : 0);
	if(qpair && queue_is_async) {
		pthread_spin_lock(&qpair->cq_lock);
		spdk_nvme_qpair_process_completions(qpair, 0);
		pthread_spin_unlock(&qpair->cq_lock);
	}
}
