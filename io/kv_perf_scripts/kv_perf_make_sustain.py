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

# Default configuration file path
JSON_PATH = './kv_perf_default_config.json'

# format
device = '0000:02:00.0'
cmd = '../kv_perf -z -d ' + device + ' -j ' + str(JSON_PATH)
print(cmd)
subprocess.check_call(cmd, shell=True)

# init fill
value_size = 4096
insert_cnt = 1000000
loop_cnt = 220  #4KB value * 1M keys * 220 loop = around 840GB
offset = insert_cnt
queue_depth = 128
outfile = 'result'
miscompare = False

perf_idx = 0
time_idx = 0
key_size_idx = 0
value_size_idx = 0

initfill_total_value_size = 0
initfill_total_key_size = 0
initfill_total_elapsed_time = 0
f = open(outfile, 'a+')
f.write('initfill_result\n')
f.close()

for i in range(0, loop_cnt * offset, offset):
    if miscompare:
        cmd = '../kv_perf -w -x -d ' + device + ' -s 0 -a ' + str(queue_depth) +' -v ' + str(value_size) + ' -n '+ str(insert_cnt) + ' -p 0 -o ' + str(i) + ' -j ' + str(JSON_PATH)
    else:
        cmd = '../kv_perf -w -d ' + device + ' -s 0 -a ' + str(queue_depth) +' -v ' + str(value_size) + ' -n '+ str(insert_cnt) + ' -p 0 -o ' + str(i) + ' -j ' + str(JSON_PATH)
    print(cmd)
    out = subprocess.check_output(cmd, shell=True).decode('utf-8').split('\n')
    #print(out)
    for k, o in enumerate(reversed(out)):
        #print(-(k+1), o)
        index = -(k+1)
        tmp_list = o.split(':')
        if (len(tmp_list) == 4):
            if tmp_list[0] == '0000': #put ops
                if tmp_list[2] == '00.0 Device\'s Write OPS/sec': #put ops
                    perf_idx = index
        elif (len(tmp_list) == 2):
            if tmp_list[0] == 'Test time': # elapsed time
                time_idx = index
            elif tmp_list[0] == '[WRITE] Total Value Size': # total value size
                value_size_idx = index
            elif tmp_list[0] == '[WRITE] Total Key Size': # total key size
                key_size_idx = index
                break
    #print(out[perf_idx])  # put ops (e.g.0000:02:00.0 Device's Write OPS/sec: 55248.62)
    #print(out[time_idx]) # time (e.g.Test time: 181 usec)
    #print(out[key_size_idx]) # put key size ([WRITE] Total Key Size: 160)
    #print(out[value_size_idx]) # put value size ([WRITE] Total Value Size: 54272)
	
    perf = float(out[perf_idx].split(':')[-1])
    initfill_total_elapsed_time += int(out[time_idx].split(':')[-1].split()[0])
    initfill_total_key_size += int(out[key_size_idx].split(':')[-1])
    initfill_total_value_size += int(out[value_size_idx].split(':')[-1])

    result_str = str(datetime.datetime.now())
    result_str += str(int(i / offset))
    result_str += ' ' + str(perf)
    result_str += ' ' + str(initfill_total_elapsed_time)
    result_str += ' ' + str(initfill_total_key_size)
    result_str += ' ' + str(initfill_total_value_size)
    f = open(outfile, 'a+')
    f.write(result_str + '\n')
    f.close()


# make sustained (overwrite keys from [0] to [insert_cnt * loop_cnt - 1])
sustained_insert_cnt = 1000000
sustained_loop_cnt = 170 #4KB value * 1M keys * 150 loop = around 572GB
start_key = 0
end_key = insert_cnt * loop_cnt - 1;

sustained_total_value_size = 0;
sustained_total_key_size = 0;
sustained_total_elapsed_time = 0;
f = open(outfile, 'a+')
f.write('sustained_result\n')
f.close()

# overwrite keys
for i in range(0, sustained_loop_cnt):
   if miscompare:
       cmd = '../kv_perf -w -x -d ' + device + ' -s 0 -a ' + str(queue_depth) +' -v ' + str(value_size) + ' -n '+ str(sustained_insert_cnt) + ' -p 3 -y ' + str(start_key) + '-' + str(end_key) + ' -j ' + str(JSON_PATH)
   else:
       cmd = '../kv_perf -w -d ' + device + ' -s 0 -a ' + str(queue_depth) +' -v ' + str(value_size) + ' -n '+ str(sustained_insert_cnt) + ' -p 3 -y ' +  str(start_key) + '-' + str(end_key) + ' -j ' + str(JSON_PATH)
   print(cmd)
   out = subprocess.check_output(cmd, shell=True).decode('utf-8').split('\n')
   perf = float(out[perf_idx].split(':')[-1])
   sustained_total_elapsed_time += int(out[time_idx].split(':')[-1].split()[0])
   sustained_total_key_size += int(out[key_size_idx].split(':')[-1])
   sustained_total_value_size += int(out[value_size_idx].split(':')[-1])

   result_str = str(datetime.datetime.now())
   result_str += str(i)
   result_str += ' ' + str(perf)
   result_str += ' ' + str(sustained_total_elapsed_time)
   result_str += ' ' + str(sustained_total_key_size)
   result_str += ' ' + str(sustained_total_value_size)
   f = open(outfile, 'a+')
   f.write(result_str + '\n')
   f.close()

print('########## Init Fill')
print('total_elapsed_time(ms): ' + str(initfill_total_elapsed_time))
print('total_key_size(byte): ' + str(initfill_total_key_size))
print('total_value_size(byte): ' + str(initfill_total_value_size))


print('########## Sustained')
print('total_elapsed_time(ms): ' + str(sustained_total_elapsed_time))
print('total_key_size(byte): ' + str(sustained_total_key_size))
print('total_value_size(byte): ' + str(sustained_total_value_size))
