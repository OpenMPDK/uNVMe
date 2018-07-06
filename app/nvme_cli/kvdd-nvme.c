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

#include <stdbool.h>
#include <stdio.h>
#include <sys/time.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sched.h>
#include "kv_types.h"
#include "kvnvme.h"

#include "nvme-print.h"
#include "argconfig.h"
#include "kvdd-nvme.h"
#include "spdk-nvme.h"

#define DEBUG_STR(_x) printf("%s(%s, %d): _x=%s\n", __FUNCTION__, __FILE__, __LINE__, _x);

bool g_kvdd_enabled = false;
char* g_nvme_pci_dev;
uint64_t handle;
uint32_t g_status;

enum {
	kv_write = 0x0,
	kv_read = 0x1,
	kv_delete = 0x2,
	kv_exist = 0x3
};

/*
 * opcode: Not used
 * command: write
 * desc: description about command, given by call functions. (write / read / delete)
 * argc, argv: args after device_id
 *
 */

volatile unsigned int complete_cnt_arr[4];
volatile unsigned int complete_cnt_it;

int (*kv_nvme_io_ptr[4])(uint64_t, int, kv_pair*) = {(int (*)(uint64_t, int, kv_pair*))kv_nvme_write, kv_nvme_read, (int (*)(uint64_t, int, kv_pair*))kv_nvme_delete, (int (*)(uint64_t, int, kv_pair*))kv_nvme_exist};
int (*kv_nvme_aio_ptr[4])(uint64_t, int, kv_pair*) = {(int (*)(uint64_t, int, kv_pair*))kv_nvme_write_async, kv_nvme_read_async, (int (*)(uint64_t, int, kv_pair*))kv_nvme_delete_async, (int (*)(uint64_t, int, kv_pair*))kv_nvme_exist_async};
bool (*kv_io_cmd_probe[4])(const struct kv_io_config) = {kvdd_write_command_probe, kvdd_read_command_probe, kvdd_delete_command_probe, kvdd_exist_command_probe};
void (*io_cb_ptr[4])(kv_pair*, unsigned int, unsigned) = {kv_io_write_cb, kv_io_read_cb, kv_io_delete_cb, kv_io_exist_cb};

static unsigned long long elapsed_utime(struct timeval start_time, struct timeval end_time) {
	unsigned long long ret = (end_time.tv_sec - start_time.tv_sec) * 1000000 + (end_time.tv_usec - start_time.tv_usec);
	return ret;
}

int kv_get_log(char *command, const char *desc, int argc, char **argv) {
	int err;
	unsigned char *log_buffer;
	const char *log_id = "identifier of log to retrieve";
	const char *log_len = "how many bytes to retrieve";
	const char *raw_binary = "output in raw format";

	struct config {
		uint8_t  log_id;
		uint32_t log_len;
		char    *raw_binary;
	};

	struct config cfg = {
		.log_id       = 0,
		.log_len      = 0,
		.raw_binary   = 0,
	};

	const struct argconfig_commandline_options command_line_options[] = {
		{"log-id",       'i', "NUM",  CFG_BYTE,     &cfg.log_id,       required_argument, log_id},
		{"log-len",      'l', "NUM",  CFG_POSITIVE, &cfg.log_len,      required_argument, log_len},
		{"raw-binary",   'b', "FILE", CFG_STRING,     &cfg.raw_binary, required_argument, raw_binary},
		{NULL}
	};

	err = argconfig_parse(argc, argv, desc, command_line_options, &cfg, sizeof(cfg));
	if (err) {
		return err;
	}

	if (!cfg.log_len) {
		fprintf(stderr, "non-zero log-len is required param\n");
		return EINVAL;
	} else {
		log_buffer = (unsigned char*) malloc(cfg.log_len);
		if (!log_buffer) {
			return ENOMEM;
		}
	}

	err = kv_nvme_get_log_page(handle, cfg.log_id, log_buffer, cfg.log_len);
	if (err) {
		return err;
	}

	if (cfg.raw_binary) {
		// display the raw buffer
		d_raw(cfg.raw_binary, log_buffer, cfg.log_len);
	} else {
		// display the buffer with format
		printf("Device:%s log-id:%d\n", g_nvme_pci_dev, cfg.log_id);
		//kv_d(log_buffer, cfg.log_len, 16, 1);
		d(log_buffer, cfg.log_len, 16, 1);
	}

	return err;
}

int kv_format(unsigned char ses) {
	return kv_nvme_format(handle, (int)ses);
}

int kv_close_it(int opcode, char *command, const char *desc, int argc, char **argv) {
	int err = 0;
	struct timeval start_time, end_time;

	const char *iterator_id = "identifier of iterator to close";
	const char *latency = "output latency statistics";
	const char *show = "show command before sending";

	struct kv_it_config cfg = { .iterator_id = 0, .show = 0, .latency = 0};
	const struct argconfig_commandline_options command_line_options[] = {
		{"iterator_id",  'i', "NUM", CFG_BYTE, &cfg.iterator_id, required_argument, iterator_id}, // cfg.key, have no own memory
		{"show-command", 'w', "",    CFG_NONE, &cfg.show,        no_argument,       show},
		{"latency",      't', "",    CFG_NONE, &cfg.latency,     no_argument,       latency},
		{NULL}
	};

	err = argconfig_parse(argc, argv, desc, command_line_options, &cfg, sizeof(cfg));
	if (err) {
		fprintf(stderr, "ERR: Some arguments for %s command are missed\n", command);

		return err;
	}

	if(!kvdd_close_it_command_probe(&cfg)) {
		argconfig_print_help(desc, command_line_options);
		return -EINVAL;
	}

	//show command parameter when users want it
	if (cfg.show) {
		printf("    iterator_id : %u\n", cfg.iterator_id);
	}

	gettimeofday(&start_time, NULL);
	err = kv_nvme_iterate_close(handle, cfg.iterator_id);
	gettimeofday(&end_time, NULL);

	// show latency of the execution
	if (cfg.latency) {
		printf(" latency (%s): %llu us\n", command, elapsed_utime(start_time, end_time));
	}
	if (err > 0) {
		fprintf(stderr, "  FAIL: status of device=0x%x\n", err);
		return err;
	} else { // if success,
		fprintf(stderr, "%s: Success\n", command);
	}

	return 0;
}

int kv_open_it(int opcode, char *command, const char *desc, int argc, char **argv) {
	int err = 0;
	struct timeval start_time, end_time;

	const char *namespace    = "identifier of desired namespace, defaults to 0";
	const char *prefix       = "prefix of iterator, defaults to 0";
	const char *bitmask      = "bitmask of iterator, defaults to 0xffffffff";
	const char *iterate_type = "type of iterator (defaults to 1: KEY ONLY, 2: KEY_WITH_RETRIEVE, 3:KEY_WITH_DELETE)";
	const char *latency      = "output latency statistics";
	const char *show         = "show command before sending";

	struct kv_it_config cfg = {
			.iterate_type = KV_KEY_ITERATE,
			.namespace = 0,
			.bitmask = 0xffffffff,
			.prefix = 0,
			.iterator_id = 0,
			.show = 0,
			.latency = 0
	};

	const struct argconfig_commandline_options command_line_options[] = {
		{"namespace",    'n', "NUM", CFG_BYTE,     &cfg.namespace,    required_argument, namespace},
		{"prefix",       'p', "NUM", CFG_POSITIVE, &cfg.prefix,       required_argument, prefix},
		{"bitmask",      'b', "NUM", CFG_POSITIVE, &cfg.bitmask,      required_argument, bitmask},
		{"iterate_type", 'i', "NUM", CFG_POSITIVE, &cfg.iterate_type, required_argument, iterate_type},
		{"show-command", 'w', "",    CFG_NONE,     &cfg.show,         no_argument,       show},
		{"latency",      't', "",    CFG_NONE,     &cfg.latency,      no_argument,       latency},
		{NULL}
	};

	err = argconfig_parse(argc, argv, desc, command_line_options, &cfg, sizeof(cfg));
	if (err) {
		fprintf(stderr, "ERR: Some arguments for %s command are missed\n", command);
		return err;
	}

	if(!kvdd_open_it_command_probe(&cfg)) {
		argconfig_print_help(desc, command_line_options);
		return -EINVAL;
	}

	//show command parameter when users want it
	if (cfg.show) {
		printf("    namespace     : 0x%x\n", cfg.namespace);
		printf("    bitmask       : 0x%x\n", cfg.bitmask);
		printf("    prefix        : 0x%x\n", cfg.prefix);
		printf("    iterator_type : 0x%x\n", cfg.iterator_id);
	}

	gettimeofday(&start_time, NULL);
	err = kv_nvme_iterate_open(handle, cfg.namespace, cfg.bitmask, cfg.prefix, cfg.iterate_type);
	gettimeofday(&end_time, NULL);

	// show latency of the execution
	if (cfg.latency) {
		printf(" latency (%s): %llu us\n", command, elapsed_utime(start_time, end_time));
	}
	if (err > 0 && err < KV_MAX_ITERATE_HANDLE + 1) {
		fprintf(stderr, "%s: Success (returned iterate handle id=0x%x)\n", command, err);
		err = 0;
	} else { // if success,
		fprintf(stderr, "  FAIL: status of device=0x%x\n", err);
		return err;
	}

	return 0;
}

int kv_read_it(int opcode, char *command, const char *desc, int argc, char **argv) {
	struct timeval start_time, end_time;
	int err = 0;
	int qid = DEFAULT_IO_QUEUE_ID;
	FILE *f = stdout;
	kv_iterate tmp_it;

	// added newly
	const char *iterator_id = "identifier of iterator to close";
	const char *value = "value file";
	const char *value_length = "length of the given value";
	const char *io_option = "io-option for the given command. For more information, please refer to the KV SDK programming guide";

	const char *latency = "output latency statistics";
	const char *show = "show command before sending";
	const char *async = "submit command as aysnchronous mode";

	struct kv_io_config cfg = {
		.key           = 0,
		.key_length    = KV_MAX_KEY_LEN,
		.value         = 0,
		.value_offset  = 0,
		.value_length  = KV_SSD_MIN_ITERATE_READ_LEN,
		.io_option     = KV_ITERATE_READ_DEFAULT,
		.namespace     = 0,
		.async         = 0,
	};

	const struct argconfig_commandline_options command_line_options[] = {
		{"iterator_id",       'i', "NUM",  CFG_BYTE,        &cfg.iterator_id,       required_argument, iterator_id}, // cfg.key, have no own memory
		{"value",             'v', "FILE", CFG_STRING,      &cfg.value,             required_argument, value},
		{"value_size",        's', "NUM",  CFG_POSITIVE,    &cfg.value_length,      required_argument, value_length},
		{"io_option",         'o', "NUM",  CFG_POSITIVE,    &cfg.io_option,         required_argument, io_option},
		{"show-command",      'w', "",     CFG_NONE,        &cfg.show,              no_argument,       show},
		{"latency",           't', "",     CFG_NONE,        &cfg.latency,           no_argument,       latency},
		{"asynchronous",      'a', "",     CFG_NONE,        &cfg.async,             no_argument,       async},
		{NULL}
	};

	err = argconfig_parse(argc, argv, desc, command_line_options, &cfg, sizeof(cfg));
	if (err) {
		return err;
	}

	// command probing
	if (!kvdd_read_it_command_probe(&cfg)) {
		fprintf(stderr, "ERR: Some arguments for %s command are missed\n", command);
		argconfig_print_help(desc, command_line_options);
		return EINVAL;
	}

	//set it
	tmp_it.iterator = cfg.iterator_id;
	memset(&tmp_it.kv, 0, sizeof(kv_pair));

	//set the key
	tmp_it.kv.key.length = cfg.key_length;
	tmp_it.kv.key.key = kv_zalloc(tmp_it.kv.key.length);
	if (!tmp_it.kv.key.key) {
		return ENOMEM;
	}
	tmp_it.kv.keyspace_id = 0; // not used

	//set the value
	tmp_it.kv.value.offset = 0;
	tmp_it.kv.value.length = 0;
	tmp_it.kv.value.value = 0;
	if (cfg.value_length) {
		tmp_it.kv.value.length = cfg.value_length;
		tmp_it.kv.value.value = kv_zalloc(tmp_it.kv.value.length);
		if (!tmp_it.kv.value.value) {
			goto free_and_return;
		}
	}

	//set io_option
	tmp_it.kv.param.io_option.iterate_read_option = cfg.io_option;
	//if async, set cb and timer
	if (cfg.async) {
		tmp_it.kv.param.async_cb = kv_read_it_cb;
		tmp_it.kv.param.private_data = &end_time;
	}

	//show command parameter when users want it
	if (cfg.show) {
		printf("    iterator_id     : 0x%x\n", tmp_it.iterator);
		printf("    value.length    : 0x%x\n", tmp_it.kv.value.length);
		printf("    param.io_option : 0x%x\n", tmp_it.kv.param.io_option.iterate_read_option);
		printf("    asynchronous    : 0x%x\n", cfg.async);
	}

	// execute command
	if (cfg.async) {
		// set affinity
		cpu_set_t cpuset;
		CPU_ZERO(&cpuset);
		CPU_SET(1, &cpuset); // CPU 0
		sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

		gettimeofday(&start_time, NULL);
		err = kv_nvme_iterate_read_async(handle, qid, &tmp_it);
		if (err != KV_SUCCESS) return err;
		while (!complete_cnt_it) {	}
		err = g_status; // update err to device status
	} else {
		// set affinity
		cpu_set_t cpuset;
		CPU_ZERO(&cpuset);
		CPU_SET(0, &cpuset); // CPU 0
		sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

		gettimeofday(&start_time, NULL);
		err = kv_nvme_iterate_read(handle, qid, &tmp_it);
		gettimeofday(&end_time, NULL);
	}

	// show latency of the execution
	if (cfg.latency) {
		printf(" latency (%s): %llu us\n", command, elapsed_utime(start_time, end_time));
	}

	// Actions by results of the command
	if (err == KV_ERR_ITERATE_READ_EOF || err == KV_SUCCESS) { // if success,
		fprintf(stderr, "%s: Success (%u returned, value_length=%u)\n", command, err, tmp_it.kv.value.length);
		if(tmp_it.kv.value.length > 0) {
			if(cfg.value != 0) {
				f = fopen(cfg.value, "wb");
				if (f == NULL) {
					fprintf(stderr, "Unable to open %s file: %s\n", "value", cfg.value);
					return EINVAL;
				}
			}
			if (tmp_it.kv.key.length > 0) {
				char read_key[KV_MAX_KEY_LEN + 1] = {""};
				memcpy(read_key, tmp_it.kv.value.value, tmp_it.kv.key.length);
				printf("key=%s\n", read_key);
				err = fwrite(tmp_it.kv.value.value + 512, tmp_it.kv.value.length, 1, f);
			} else {
				err = fwrite(tmp_it.kv.value.value, tmp_it.kv.value.length, 1, f);
			}
			if(cfg.value != 0) {
				fclose(f);
			}
		}
	} else { // if an error occurs, display the err (device status)
		fprintf(stderr, "  FAIL: status of device=0x%x\n", err);
	}

free_and_return:
	if(tmp_it.kv.key.key) kv_free(tmp_it.kv.key.key);
	if(tmp_it.kv.value.value) kv_free(tmp_it.kv.value.value);
	if(f) fclose(f);
	return err;

}

int kv_list_it(int opcode, char *command, const char *desc, int argc, char **argv) {
	int i;
	int err = 0;
	int nr_iterate_handle = KV_MAX_ITERATE_HANDLE;

	kv_iterate_handle_info info[KV_MAX_ITERATE_HANDLE];
	err = kv_nvme_iterate_info(handle, info, nr_iterate_handle);

	printf("iterate_handle count=%d\n", nr_iterate_handle);
	printf("=======================================================================\n");
	printf("  id  | status | type | keyspace_id |   prefix   |   bitmask  | is_eof \n");
	printf("=======================================================================\n");
	if (err == KV_SUCCESS) {
		for (i = 0; i < nr_iterate_handle; i++) {
			printf(" 0x%-2x | 0x%-4x | %-4x | 0x%-9x | 0x%08x | 0x%08x | 0x%-4x \n",
				info[i].handle_id, info[i].status, info[i].type,
				info[i].keyspace_id, info[i].prefix, info[i].bitmask,
				info[i].is_eof);
		}
	}

	return err;
}

int kv_submit_io(int opcode, char *command, const char *desc, int argc, char **argv) {
	struct timeval start_time, end_time;
	int err = 0;
	int qid = DEFAULT_IO_QUEUE_ID;
	FILE *f = NULL;
	kv_pair tmp_kv;

	// added newly
	const char *key = "key to access";
	const char *key_length = "length of the given key";
	const char *value = "value file";
	const char *value_offset = "offset of the given value";
	const char *value_length = "length of the given value";
	const char *io_option = "io-option for the given command. For more information, please refer to the KV SDK programming guide";
	const char *namespace = "identifier of desired namespace";

	const char *latency = "output latency statistics";
	const char *show = "show command before sending";
	const char *dry = "show command instead of sending";
	const char *async = "submit command as aysnchronous mode";

	struct kv_io_config cfg = {
		.key           = 0,
		.key_length    = 0,
		.value         = 0,
		.value_offset  = 0,
		.value_length  = 0,
		.io_option     = 0,
		.namespace     = 0,
		.async         = 0,
	};

	const struct argconfig_commandline_options command_line_options[] = {
		{"key",               'k', "FILE", CFG_STRING,      &cfg.key,               required_argument, key}, // cfg.key, have no own memory
		{"key_length",        'l', "NUM",  CFG_SHORT,       &cfg.key_length,        required_argument, key_length},
		{"value",             'v', "FILE", CFG_STRING,      &cfg.value,             required_argument, value},
		{"value_offset",      'o', "NUM",  CFG_POSITIVE,    &cfg.value_offset,      required_argument, value_offset},
		{"value_size",        's', "NUM",  CFG_POSITIVE,    &cfg.value_length,      required_argument, value_length},
		{"io_option",         'i', "NUM",  CFG_POSITIVE,    &cfg.io_option,         required_argument, io_option},
		{"namespace",         'n', "NUM",  CFG_BYTE,        &cfg.namespace,         required_argument, namespace},
		{"show-command",      'w', "",     CFG_NONE,        &cfg.show,              no_argument,       show},
		{"dry-run",           'd', "",     CFG_NONE,        &cfg.dry_run,           no_argument,       dry},
		{"latency",           't', "",     CFG_NONE,        &cfg.latency,           no_argument,       latency},
		{"asynchronous",      'a', "",     CFG_NONE,        &cfg.async,             no_argument,       async},
		{NULL}
	};

	// determine functions by the given command string
	int kv_cmd = -1;
	if (strcmp(command, "write") == 0) {
		kv_cmd = kv_write;
		// check required arguments
	} else if (strcmp(command, "read") == 0) {
		kv_cmd = kv_read;
	} else if (strcmp(command, "delete") == 0) {
		kv_cmd = kv_delete;
	} else if (strcmp(command, "exist") == 0) {
		kv_cmd = kv_exist;
	}else {
		fprintf(stderr, "sub-command parse error\n");
		return EINVAL;
	}

	err = argconfig_parse(argc, argv, desc, command_line_options, &cfg, sizeof(cfg));
	if (err) {
		return err;
	}

	// command probing
	if (!kv_io_cmd_probe[kv_cmd](cfg)) {
		fprintf(stderr, "ERR: Some arguments for %s command are missed\n", command);
		return EINVAL;
	}

	//TODO: current KV-SSD supports only 16B key_length
	//set the key
	if (cfg.key_length) {
		tmp_kv.key.length = cfg.key_length;
		tmp_kv.key.key = kv_zalloc(tmp_kv.key.length);
		if (!tmp_kv.key.key) {
			return ENOMEM;
		}
		tmp_kv.keyspace_id = cfg.namespace;
	} else {
		fprintf(stderr, "Key is needed\n");
		return EINVAL;
	}
	if (cfg.key) {
		memcpy(tmp_kv.key.key, cfg.key, tmp_kv.key.length);
	} else {
		fprintf(stderr, "key_length is needed\n");
		return EINVAL;
	}

	//set the value
	tmp_kv.value.length = 0;
	tmp_kv.value.value = 0;
	if (cfg.value_length) {
		tmp_kv.value.length = cfg.value_length;
		tmp_kv.value.value = kv_zalloc(tmp_kv.value.length);
		if (!tmp_kv.value.value) {
			return ENOMEM;
		}
	}
	tmp_kv.value.offset = cfg.value_offset;
	//This is copying value for store a key-value pair
	if (cfg.value && value_length && kv_cmd == kv_write) {
		f = fopen(cfg.value, "r");
		if (f == NULL) {
			fprintf(stderr, "Unable to open %s file: %s\n", "value", cfg.value);
			return EINVAL;
		}
		err = fread(tmp_kv.value.value, tmp_kv.value.length, 1, f);
	}

	//set io_option
	tmp_kv.param.io_option.store_option = cfg.io_option;
	//if async, set cb and timer
	if (cfg.async) {
		tmp_kv.param.async_cb = io_cb_ptr[kv_cmd];
		tmp_kv.param.private_data = &end_time;
	}

	//show command parameter when users want it
	if (cfg.show) {
		printf("    key             : %s\n", (char*) tmp_kv.key.key);
		printf("    key_length      : 0x%06x\n", tmp_kv.key.length);
		printf("    value.value     : 0x%p\n", tmp_kv.value.value);
		printf("    value.offset    : 0x%06x\n", tmp_kv.value.offset);
		printf("    value.length    : 0x%06x\n", tmp_kv.value.length);
		printf("    param.io_option : 0x%06x\n", tmp_kv.param.io_option.store_option);
		printf("    asynchronous    : 0x%06x\n", cfg.async);
		if (cfg.dry_run) goto free_and_return;
	}

	// execute command
	if (cfg.async) {
		// set affinity
		cpu_set_t cpuset;
		CPU_ZERO(&cpuset);
		CPU_SET(1, &cpuset); // CPU 0
		sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

		gettimeofday(&start_time, NULL);
		err = kv_nvme_aio_ptr[kv_cmd](handle, qid, &tmp_kv);
		if (err != KV_SUCCESS) return err;
		while (!complete_cnt_arr[kv_cmd]) {	}
		err = g_status; // update err to device status
	} else {
		// set affinity
		cpu_set_t cpuset;
		CPU_ZERO(&cpuset);
		CPU_SET(0, &cpuset); // CPU 0
		sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

		gettimeofday(&start_time, NULL);
		err = kv_nvme_io_ptr[kv_cmd](handle, qid, &tmp_kv);
		gettimeofday(&end_time, NULL);
	}

	// show latency of the execution
	if (cfg.latency) {
		printf(" latency (%s): %llu us\n", command, elapsed_utime(start_time, end_time));
	}

	// Actions by results of the command
	if (err > 0) { // if an error occurs, display the err (device status)
		if(kv_cmd == kv_exist)
		fprintf(stderr, "  FAIL: status of device=0x%x\n", err);
	} else { // if success,
		fprintf(stderr, "%s: Success\n", command);
		if (kv_cmd == kv_read) {
			if (cfg.io_option == 2) {
				printf("value_size of the key(%s)=%u\n", (char*) tmp_kv.key.key, tmp_kv.value.length);
			} else if (!cfg.value) {
				printf("read_value=%s\n", (char*) tmp_kv.value.value);
			} else {
				f = fopen(cfg.value, "wb");
				if (f == NULL) {
					fprintf(stderr, "Unable to open %s file: %s\n", "value", cfg.value);
					return EINVAL;
				}
				err = fwrite(tmp_kv.value.value, tmp_kv.value.length, 1, f);
			}
		}
	}

free_and_return:
	if(tmp_kv.key.key) kv_free(tmp_kv.key.key);
	if(tmp_kv.value.value) kv_free(tmp_kv.value.value);
	if(f) fclose(f);
	return err;
}

int kvdd_main(int argc, char** argv) {
	int ret;
	int shm_id = 1;
	struct spdk_env_opts opts;

	// is the given device active?

	kv_nvme_io_options options = { 0 };
	options.core_mask = 3; // Use 0 core and 1 core
	options.sync_mask = 1; // Use 0 core for Sync I/O mode
	options.num_cq_threads = 1; // Use only one CQ Processing Thread
	options.cq_thread_mask = 4; // Use core 3 for completion queue
	options.mem_size_mb = DPDK_DEFAULT_MEM_SIZE;

	g_nvme_pci_dev = argv[1]; // to be cleaned

	if(argc < 2 || !nvme_spdk_is_bdf_dev(g_nvme_pci_dev)) {
		fprintf(stderr, "device information needed, example: 0000:02:00.0\n");
		return 1;
	}

	get_device_list_from_active_spdk_proc();
	for(int i = 0; i < g_num_list_device; i++) {
		if(strcmp(list_device[i].node, g_nvme_pci_dev) == 0) {
			shm_id = list_device[i].pid;
			break;
		}
	}

	spdk_env_opts_init(&opts);
	opts.name = "KV_Interface";
	opts.dpdk_mem_size = options.mem_size_mb;
	opts.shm_id = shm_id;
	kv_env_init_with_spdk_opts(&opts);
	ret = kv_nvme_init(g_nvme_pci_dev, &options, KV_TYPE_SSD);
	if (ret) {
		return ret;
	}

	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(0, &cpuset); // CPU 0
	sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

	handle = kv_nvme_open(g_nvme_pci_dev);
	if (!handle) {
		return ret;
	}

	g_kvdd_enabled = true;

	return 0;
}

void kvdd_cleanup(void) {
	int ret;

	ret = kv_nvme_close(handle);
	if (ret) {
		exit(1);
	}

	ret = kv_nvme_finalize(g_nvme_pci_dev);
	if (ret) {
		exit(1);
	}
}

bool kvdd_required_cmd(char *cmd_str) {
	if (strcmp(cmd_str, "write") == 0) {
		return true;
	}

	if (strcmp(cmd_str, "read") == 0) {
		return true;
	}

	if (strcmp(cmd_str, "delete") == 0) {
		return true;
	}

	if (strcmp(cmd_str, "exist") == 0) {
		return true;
	}

	if (strcmp(cmd_str, "format") == 0) {
		return true;
	}

	if (strcmp(cmd_str, "get-log") == 0) {
		return true;
	}

	if (strcmp(cmd_str, "list-it") == 0) {
		return true;
	}

	if (strcmp(cmd_str, "close-it") == 0) {
		return true;
	}

	if (strcmp(cmd_str, "open-it") == 0) {
		return true;
	}

	if (strcmp(cmd_str, "read-it") == 0) {
		return true;
	}

	return false;
}

bool kvdd_write_command_probe(const struct kv_io_config kv_cfg) {
	if (!kv_cfg.value_length) {
		fprintf(stderr, "ERR: write command requires a value_length\n");
		fprintf(stderr, "ex) -s 4096\n");
		return false;
	}
	if (!kv_cfg.value) {
		fprintf(stderr, "ERR: write command requires a file for value\n");
		fprintf(stderr, "ex) -v value.txt\n");
		return false;
	}
	return true;
}

bool kvdd_read_command_probe(const struct kv_io_config kv_cfg) {
	if (!kv_cfg.value_length) {
		if (kv_cfg.io_option == 2) {
			return true;
		} else {
			fprintf(stderr, "ERR: read command requires a value_length\n");
			fprintf(stderr, "ex) -s 4096\n");
			return false;
		}
	}
	return true;
}

bool kvdd_delete_command_probe(const struct kv_io_config kv_cfg) {
	return true;
}

bool kvdd_exist_command_probe(const struct kv_io_config kv_cfg) {
	if (kv_cfg.key == 0 || kv_cfg.key_length == 0) {
		return false;
	} else {
		return true;
	}
}

bool kvdd_close_it_command_probe(const struct kv_it_config* kv_it_cfg) {
	if (kv_it_cfg->iterator_id < 1 || kv_it_cfg->iterator_id > KV_MAX_ITERATE_HANDLE) {
		fprintf(stderr, "iterator_id must be given (1 ~ %u\n", KV_MAX_ITERATE_HANDLE);
		return false;
	}
	return true;
}

bool kvdd_open_it_command_probe(const struct kv_it_config* kv_it_cfg) {
	if (kv_it_cfg->iterate_type < 1 || kv_it_cfg->iterate_type > 3) {
		fprintf(stderr, "Invalid iterate_type 0x%x\n", kv_it_cfg->iterate_type);
		return false;
	}

	if (kv_it_cfg->namespace != KV_KEYSPACE_IODATA && kv_it_cfg->namespace != KV_KEYSPACE_METADATA) {
		fprintf(stderr, "Invalid namespace 0x%x\n", kv_it_cfg->namespace);
		return false;
	}

	return true;
}

bool kvdd_read_it_command_probe(const struct kv_io_config* kv_it_cfg) {
	if (kv_it_cfg->iterator_id < 1 || kv_it_cfg->iterator_id > KV_MAX_ITERATE_HANDLE) {
		fprintf(stderr, "iterator_id must be given (1 ~ %u\n", KV_MAX_ITERATE_HANDLE);
		return false;
	}
	if (kv_it_cfg->value_length < KV_SSD_MIN_ITERATE_READ_LEN || kv_it_cfg->value_length > KV_SSD_MAX_ITERATE_READ_LEN) {
		fprintf(stderr, "invalid iterate read length \n");
		return false;
	}
	
	return true;
}

void kv_io_write_cb(kv_pair *kv, unsigned int result, unsigned int status) {
	if(status != KV_SUCCESS){
		fprintf(stderr, "[%s] error. key=%s option=%d value.length=%d value.offset=%d status code=%d\n",
			__FUNCTION__, (char*)kv->key.key, kv->param.io_option.store_option, kv->value.length, kv->value.offset, status);
		exit(1);
	}
	complete_cnt_arr[kv_write]++;
	struct timeval *end = kv->param.private_data;
	gettimeofday(end, NULL);
	g_status = status;
}

void kv_io_read_cb(kv_pair *kv, unsigned int result, unsigned int status) {
	if(status != KV_SUCCESS){
		fprintf(stderr, "[%s] error. key=%s option=%d value.length=%d value.offset=%d status code=%d\n",
			__FUNCTION__, (char*)kv->key.key, kv->param.io_option.retrieve_option, kv->value.length, kv->value.offset, status);
		exit(1);
	}
	complete_cnt_arr[kv_read]++;
	struct timeval *end = kv->param.private_data;
	gettimeofday(end, NULL);
	g_status = status;
}

void kv_io_delete_cb(kv_pair *kv, unsigned int result, unsigned int status) {
	if(status != KV_SUCCESS){
		fprintf(stderr, "[%s] error. key=%s option=%d value.length=%d value.offset=%d status code=%d\n",
			__FUNCTION__, (char*)kv->key.key, kv->param.io_option.delete_option, kv->value.length, kv->value.offset, status);
		exit(1);
	}
	complete_cnt_arr[kv_delete]++;
	struct timeval *end_time = kv->param.private_data;
	gettimeofday(end_time, NULL);
	g_status = status;
}

void kv_io_exist_cb(kv_pair *kv, unsigned int result, unsigned int status) {
	if(status != KV_SUCCESS && status != KV_ERR_NOT_EXIST_KEY){
		fprintf(stderr, "[%s] error. key=%s option=%d value.length=%d value.offset=%d status code=%d\n",
			__FUNCTION__, (char*)kv->key.key, kv->param.io_option.delete_option, kv->value.length, kv->value.offset, status);
		exit(1);
	}
	complete_cnt_arr[kv_exist]++;
	struct timeval *end_time = kv->param.private_data;
	gettimeofday(end_time, NULL);
	g_status = status;
}

void kv_read_it_cb(kv_iterate *it, unsigned int result, unsigned int status) {
	if(status != KV_SUCCESS && status != KV_ERR_ITERATE_READ_EOF){
		fprintf(stderr, "[%s] error. iterator_id=%d key=%s option=%d value.length=%d value.offset=%d status code=%d\n",
			__FUNCTION__, it->iterator, (char*)it->kv.key.key, it->kv.param.io_option.iterate_read_option, it->kv.value.length, it->kv.value.offset, status);
		exit(1);
	}
	complete_cnt_it++;
	struct timeval *end_time = it->kv.param.private_data;
	gettimeofday(end_time, NULL);
	g_status = status;
}
