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

#ifndef _SPDK_NVME_H
#define _SPDK_NVME_H

#include "nvme-ioctl.h"
#include "spdk/nvme.h"

#define NUM_MAX_NVMES 1024

#define SPDK_TRADDR_MAX_LEN 256

#define SPDK_TRSVCID_MAX_LEN 32

#define SPDK_NVMF_NQN_MAX_LEN 223

#define DPDK_DEFAULT_MEM_SIZE 64

struct spdk_nvme_dev {
	struct spdk_nvme_ctrlr	*ctrlr;
	struct spdk_nvme_qpair	*io_qpair;
	unsigned int		fd;
	unsigned int		ns_id;
	char			traddr[SPDK_TRADDR_MAX_LEN + 1];
	char			trsvcid[SPDK_TRSVCID_MAX_LEN + 1];
	char			subnqn[SPDK_NVMF_NQN_MAX_LEN + 1];
};

extern bool g_spdk_enabled;

extern struct list_item list_device[NUM_MAX_NVMES];

extern char g_list_device_str[NUM_MAX_NVMES][256];

extern int g_num_list_device;

extern unsigned int g_num_ctrlr;

extern struct spdk_nvme_dev g_spdk_dev[NUM_MAX_NVMES];

extern struct spdk_nvme_transport_id g_trid;

bool spdk_bypass_cmd(char *cmd_str);

void get_device_list_from_active_spdk_proc();

bool nvme_spdk_is_bdf_dev(char *str);

int spdk_main(int argc, char **argv);

int nvme_spdk_nvmf_probe(char *traddr, char *trsvcid, char *subnqn);

int nvme_spdk_submit_cmd_passthru(unsigned int fd, struct nvme_passthru_cmd *cmd, bool admin);

void nvme_spdk_ns_sector_size(unsigned int fd, int *sector_size);

int nvme_spdk_io(unsigned int fd, struct nvme_user_io *io);

unsigned int nvme_spdk_get_fd_by_dev(const char *dev);

struct spdk_nvme_ctrlr *nvme_spdk_get_ctrlr_by_fd(unsigned int fd);

int nvme_spdk_get_nsid(unsigned int fd);

void nvme_spdk_cleanup(void);

int nvme_spdk_is_valid_fd(unsigned int fd);

void nvme_spdk_show_registers(unsigned int fd); 

#endif
