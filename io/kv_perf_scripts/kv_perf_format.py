#!/usr/bin/env python3

# format KV / LBA SSD

import subprocess
import os
import sys

euid = os.geteuid()
if euid:
    print('Script not started as root. Running sudo..')
    args = ['sudo', sys.executable] + sys.argv + [os.environ]
    os.execlpe('sudo', *args)

#TYPE 0=LBA SSD, 1=KV SSD
TYPE = 1

# PCIe address of KV SSD
PCI_ADDRESS = "0000:02:00.0"

# Default configuration file path
JSON_PATH = './kv_perf_default_config.json'

cmd = '../kv_perf -z -d ' + str(PCI_ADDRESS) + ' -m ' + str(TYPE) + ' -j ' + str(JSON_PATH)
print(cmd)
subprocess.check_call(cmd, shell=True)
