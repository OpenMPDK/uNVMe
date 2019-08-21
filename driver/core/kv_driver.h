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

#ifndef _KV_DRIVER_H
#define _KV_DRIVER_H

#include <spdk/kvnvme_spdk.h>
#include <spdk/nvme_internal.h>
#include <kv_trace.h>
#include <kvnvme.h>
#include "kvutil.h"
#include "kv_types.h"

#define	LBA_SSD_KEY_ID_SIZE		8
#define	TRANSPORT_ID_STRING_LEN		19
#define	BDF_STRING_LEN			13

#define	VENDOR_LOG_ID			0XCA
#define	VENDOR_LOG_SIZE			512

#define	CQ_THREAD_DEFAULT_CPU		1 // CPU core 1 (Core number starts with 0)

#define	TRANSPORT_ID_STRING		"trtype:PCIe traddr:"

typedef struct kv_nvme kv_nvme_t;

/**
 * @brief NVMe Device Operations
 */
typedef struct nvme_dev_operations {
	/** Pointer to the NVMe Write and Append Function */
	int (*write)(kv_nvme_t *nvme, kv_pair *kv, int core_id, uint8_t is_store);
	/** Pointer to the NVMe Write Async Function */
	int (*write_async)(kv_nvme_t *nvme, kv_pair *kv, int core_id);
	/** Pointer to the NVMe Read Function */
	int (*read)(kv_nvme_t *nvme, kv_pair *kv, int core_id);
	/** Pointer to the NVMe Read Async Function */
	int (*read_async)(kv_nvme_t *nvme, kv_pair *kv, int core_id);
	/** Pointer to the NVMe Delete Function */
	int (*delete)(kv_nvme_t *nvme, const kv_pair *kv, int core_id);
	/** Pointer to the NVMe Delete Async Function */
	int (*delete_async)(kv_nvme_t *nvme, const kv_pair *kv, int core_id);
	/** Pointer to the NVMe Format Function */
	int (*format)(kv_nvme_t *nvme, int ses);
	/** Pointer to the NVMe Get Used Size Function */
	uint64_t (*get_used_size)(kv_nvme_t *nvme);
	/** Pointer to the NVMe Exist Function */
	int (*exist)(kv_nvme_t *nvme, const kv_pair *kv, int core_id);
	/** Pointer to the NVMe Exist Function */
	int (*exist_async)(kv_nvme_t *nvme, const kv_pair *kv, int core_id);
	/** Pointer to the NVMe Iterate Open */
	uint32_t (*iterate_open)(kv_nvme_t *nvme, const uint8_t keyspace_id, const uint32_t bitmask, const uint32_t prefix, const uint8_t iterate_type, int core_id);
	/** Pointer to the NVMe Iterate Close */
	int (*iterate_close)(kv_nvme_t *nvme, const uint8_t iterator, int core_id);
	/** Pointer to the NVMe Iterate Read */
	int (*iterate_read)(kv_nvme_t *nvme, kv_iterate* iterate, int core_id);
	/** Pointer to the NVMe Iterate Read Async */
	int (*iterate_read_async)(kv_nvme_t *nvme, kv_iterate* iterate, int core_id);
} nvme_dev_operations_t;

/**
 * @brief KV NVMe Device
 */
typedef struct kv_nvme {
	/** SPDK NVMe Controller */
	struct spdk_nvme_ctrlr *ctrlr;
	/** SPDK NVMe Namespace */
	struct spdk_nvme_ns *ns;
	/** SPDK NVMe IO Queue Pairs */
	struct spdk_nvme_qpair **qpairs;
	/** SPDK Transport ID */
	struct spdk_nvme_transport_id trid;
	/** Transport ID of the NVMe Device */
	char traddr[SPDK_NVMF_TRADDR_MAX_LEN];
	/** KV NVMe Device Entry in the List */
	TAILQ_ENTRY(kv_nvme) tailq;
	/** Number of I/O Queues */
	unsigned int num_io_queues;
	/** NVMe Device Operations Structure */
	nvme_dev_operations_t dev_ops;
	/** NVMe Device Sector Size (In case of LBA Type SSDs) */
	uint32_t sector_size;
	/** Thread to Process the Completions of all CQs */
	pthread_t process_all_cqs_thread;
	/** Stop the Processing of Completions for all CQs */
	unsigned int stop_process_all_cqs;
	/** I/O Queue Type (Sync or Async) */
	unsigned int *io_queue_type;
	/** Number of CQ Processing Threads */
	unsigned int num_cq_threads;
	/** SPDK NVMe IO Queue Pairs */
	struct spdk_nvme_qpair **async_qpairs;
	/** Stop the Processing of Completions for CQ */
	unsigned int stop_process_cq[MAX_CPU_CORES];
	/** Thread to Process the Completions of Individual CQs */
	pthread_t process_cq_thread[MAX_CPU_CORES];
	/** I/O Options for the NVMe Device */
	kv_nvme_io_options *options;
	/** Pointer to AER Callback Function */
	kv_aer_cb_fn_t aer_cb_fn;
	/** Parameter to AER Callback Function */
	void *aer_cb_arg;
} kv_nvme_t;

/**
 * @brief CQ Processing Thread Argument
 */
typedef struct process_cq_thread_arg {
	/** KV NVMe Device */
	kv_nvme_t *nvme;
	/** Async I/O Qpair Start Index in the async_qpairs array */
	unsigned int async_qpair_start_index;
	/** No. of Async I/O Qpairs handled by this thread */
	unsigned int num_async_qpairs;
	/** Async I/O CQ Processing Thread Index */
	unsigned int thread_id;
	/** CPU Core ID */
	unsigned int cpu_id;
} process_cq_thread_arg_t;

/**
 * @brief I/O or Admin Command Sequence
 */
typedef struct nvme_cmd_sequence {
	/** Status of the I/O or Admin command*/
	uint8_t status;
	/** Result of the I/O or Admin command (CDW0 Command Specific) */
	uint32_t result;
	/** I/O or Admin command Completion State*/
	unsigned int is_completed;
} nvme_cmd_sequence_t;

static inline unsigned int min(unsigned int a, unsigned int b) {
	return (a < b) ? a : b;
}

#endif
