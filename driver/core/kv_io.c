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

int32_t kv_nvme_process_all_cqs_thread(void *arg) {
        unsigned int cpu_id = 0;
        cpu_set_t cpuset;
        process_cq_thread_arg_t *pcq_arg = (process_cq_thread_arg_t *)arg;
        kv_nvme_t *nvme = (kv_nvme_t *)pcq_arg->nvme;

        ENTER();

        cpu_id = pcq_arg->cpu_id;

        CPU_ZERO(&cpuset);
        CPU_SET(cpu_id, &cpuset);

        sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

        while(!nvme->stop_process_all_cqs) {
                kv_nvme_process_completion((uint64_t)nvme);
		usleep(1);
        }

        free(arg);

        LEAVE();
        return 0;
}

int32_t kv_nvme_process_cq_thread(void *arg) {
	struct spdk_nvme_qpair *qpair = NULL;
        unsigned int queue_id = 0;
        cpu_set_t cpuset;
        process_cq_thread_arg_t *pcq_arg = (process_cq_thread_arg_t *)arg;

        ENTER();

        CPU_ZERO(&cpuset);
        CPU_SET(pcq_arg->cpu_id, &cpuset);

        sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

        while(!pcq_arg->nvme->stop_process_cq[pcq_arg->thread_id]) {
                for(queue_id = pcq_arg->async_qpair_start_index; queue_id < (pcq_arg->async_qpair_start_index + pcq_arg->num_async_qpairs); queue_id++)
                {
			qpair = pcq_arg->nvme->async_qpairs[queue_id];
			pthread_spin_lock(&qpair->cq_lock);
			spdk_nvme_qpair_process_completions(qpair, 0);
			pthread_spin_unlock(&qpair->cq_lock);
                }
		usleep(1);
        }

        free(arg);

        LEAVE();
        return 0;
}

