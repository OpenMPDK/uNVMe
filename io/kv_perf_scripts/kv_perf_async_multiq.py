#!/usr/bin/env python3

#Async key_perf with multiple queues

import subprocess
import os
import sys

euid = os.geteuid()
if euid:
	print('Script not started as root. Running sudo..')
	args = ['sudo', sys.executable] + sys.argv + [os.environ]
	os.execlpe('sudo', *args)

SEQ_INQ = 0
RAND = 3

WRITE_READ_DELETE = "-w -r -e"
WRITE_READ = "-w -r"
ONLY_READ = "-r"
ONLY_WRITE = "-w"

MASK = 0xFF

# PCIe address of KV SSD
PCI_ADDRESS = "0000:02:00.0"

# Number of keys to transfer
NUM_KEYS = 10000

# Type of the test to run
WORKLOAD = WRITE_READ_DELETE
#WORKLOAD = WRITE_READ
#WORKLOAD = ONLY_READ
#WORKLOAD = ONLY_WRITE

# Key distribution
KEY_DIST = SEQ_INQ
#KEY_DIST=$RAND

# Value size
VALUE_SIZE = 4096

# Default configuration file path
JSON_PATH = './kv_perf_default_config.json'

cmd = '../kv_perf -d ' + str(PCI_ADDRESS) + ' -m 1 -c ' + str(hex(MASK)) + ' -s 0 -n ' +\
      str(NUM_KEYS) + ' ' + str(WORKLOAD) + ' -t 1 -p ' +\
      str(KEY_DIST) + ' -v ' + str(VALUE_SIZE) + ' -j ' + str(JSON_PATH)
print(cmd)
subprocess.check_call(cmd, shell=True)
