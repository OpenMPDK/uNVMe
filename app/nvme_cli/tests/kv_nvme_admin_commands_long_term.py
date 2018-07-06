#!/usr/bin/env python3

import subprocess
import os
import sys 
import time
import datetime

euid = os.geteuid()
if euid:
    print('Script not started as root. Running sudo..')
    args = ['sudo', sys.executable] + sys.argv + [os.environ]
    os.execlpe('sudo', *args)

cmds = "../kv_nvme list"
print(cmds)
subprocess.check_call(cmds, shell=True)
is_start = input("do you want to satrt kv_nvme long_term test wih fio? (Y/n) ")
if is_start == "n":
    print("End long_term test")
    exit(0)

bdf_address = input("Input bdf address of your device (0000:02:00.0): ")
if bdf_address == "": 
    bdf_address = "0000:02:00.0"

time_limit_txt = input("How long do you want to test in seconds? (60) ")
if time_limit_txt == "": 
    time_limit_txt = "60"

time_limit = int(time_limit_txt)

commands = ["list-ctrl", "id-ctrl", "smart-log", "list", "get-log", "list-ns"]
arguments = ["", "", "", "", " --log-len=64 --log-id=0xc0", ""] 
start_time = time.time()
# your code
while ((time.time() - start_time) < time_limit) :
    for i in range(0, 6): 
        cmds = "../kv_nvme " + commands[i] + " " + bdf_address + arguments[i]
        print(cmds)
        out = subprocess.call(cmds, shell=True)
        if out != 0:
            print("ERROR OCUURED!")
            exit(-1)

