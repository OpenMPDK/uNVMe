/*
 * kvdd_nvme.h
 *
 *  Created on: Dec 15, 2017
 *      Author: root
 */

#ifndef APP_NVME_CLI_KVDD_NVME_H_
#define APP_NVME_CLI_KVDD_NVME_H_
#endif /* APP_NVME_CLI_KVDD_NVME_H_ */

#include <stdint.h>
#include "kvnvme.h"

struct kv_io_config {
	char* key;
	uint16_t key_length;
	char* value;
	uint32_t value_offset;
	uint32_t value_length;
	int   io_option;
	uint8_t namespace;
	int   show;
	int   dry_run;
	int   latency;
	int   async;
	uint8_t iterator_id;
};

struct kv_it_config {
	uint8_t  namespace;
	uint32_t prefix;
	uint32_t bitmask;
	uint8_t  iterate_type;
	uint8_t  iterator_id;
	int32_t  show;
	int32_t  latency;
};

extern bool g_kvdd_enabled;

int kv_get_log(char *command, const char *desc, int argc, char **argv);

int kv_format(unsigned char ses);

int kv_close_it(int opcode, char *command, const char *desc, int argc, char **argv);

int kv_open_it(int opcode, char *command, const char *desc, int argc, char **argv);

int kv_read_it(int opcode, char *command, const char *desc, int argc, char **argv);

int kv_list_it(int opcode, char *command, const char *desc, int argc, char **argv);

int kv_submit_io(int opcode, char *command, const char *desc, int argc, char **argv);

int kvdd_main(int argc, char** argv);

void kvdd_cleanup(void);

bool kvdd_required_cmd(char *cmd_str);

bool kvdd_write_command_probe(const struct kv_io_config kv_cfg);

bool kvdd_read_command_probe(const struct kv_io_config kv_cfg);

bool kvdd_delete_command_probe(const struct kv_io_config kv_cfg);

bool kvdd_exist_command_probe(const struct kv_io_config kv_cfg);

bool kvdd_close_it_command_probe(const struct kv_it_config* kv_cfg);

bool kvdd_open_it_command_probe(const struct kv_it_config* kv_cfg);

bool kvdd_read_it_command_probe(const struct kv_io_config* kv_cfg);

void kv_io_write_cb(kv_pair *kv, unsigned int result, unsigned int status);

void kv_io_read_cb(kv_pair *kv, unsigned int result, unsigned int status);

void kv_io_delete_cb(kv_pair *kv, unsigned int result, unsigned int status);

void kv_io_exist_cb(kv_pair *kv, unsigned int result, unsigned int status);

void kv_read_it_cb(kv_iterate *it, unsigned int result, unsigned int status);
