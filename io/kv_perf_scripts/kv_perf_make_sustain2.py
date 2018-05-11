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

device = '0000:02:00.0'

# Default configuration file path
JSON_PATH = './kv_perf_default_config.json'

# format
cmd = '../kv_perf -z -d ' + device + ' -j ' + str(JSON_PATH)
print(cmd)
subprocess.check_call(cmd, shell=True)

# init fill
value_list = [8192, 5120, 4096, 3072]
#print(' '.join(map(str, value_list)))
offset = 1000000
insert_cnt = 1000000 # Even Number!!!
loop_cnt = 176
second_loop_cnt = 10
queue_depth = 128
outfile = 'result'
miscompare = False

perf_idx = 0
time_idx = 0
put_key_size_idx = 0
put_value_size_idx = 0
del_key_size_idx = 0
del_value_size_idx = 0

initfill_total_elapsed_time = 0;
initfill_total_key_size = 0;
initfill_total_value_size = 0;
f = open(outfile, 'a+')
f.write('initfill_result\n')
f.close()
for i in range(0, loop_cnt):
    if miscompare:
        cmd = '../kv_perf -w -x -d ' + device + ' -s 0 -a ' + str(queue_depth) +' -v ' + ' '.join(map(str, value_list))+ ' -n '+ str(insert_cnt) + ' -p 0 -o ' + str(i * offset) + ' -j ' + str(JSON_PATH)
    else:
        cmd = '../kv_perf -w -d ' + device + ' -s 0 -a ' + str(queue_depth) +' -v ' + ' '.join(map(str, value_list))+ ' -n '+ str(insert_cnt) + ' -p 0 -o ' + str(i * offset) + ' -j ' + str(JSON_PATH)
    print(cmd)
    out = subprocess.check_output(cmd, shell=True).decode('utf-8').split('\n')
    #print(out)
    for k, o in enumerate(reversed(out)):
    #    print(-(k+1), o)
        index = -(k+1)
        tmp_list = o.split(':')
        if (len(tmp_list) == 4):
            if tmp_list[0] == '0000':
                if tmp_list[2] == '00.0 Device\'s Write OPS/sec': #put ops
                    perf_idx = index
        elif (len(tmp_list) == 2):
            if tmp_list[0] == 'Test time': # elapsed time
                time_idx = index
            elif tmp_list[0] == '[WRITE] Total Value Size': # total value size
                put_value_size_idx = index
            elif tmp_list[0] == '[WRITE] Total Key Size': # total key size
                put_key_size_idx = index
                break
    #print(out[perf_idx])  # put ops (e.g.0000:02:00.0 Device's Write OPS/sec: 49504.95)
    #print(out[time_idx]) # time (Test time: 202 usec)
    #print(out[put_key_size_idx]) # put key size ([WRITE] Total Key Size: 160)
    #print(out[put_value_size_idx]) # put value size ([WRITE] Total Value Size: 54272)
    perf = float(out[perf_idx].split(':')[-1])
    initfill_total_elapsed_time += int(out[time_idx].split(':')[-1].split()[0])
    initfill_total_key_size += int(out[put_key_size_idx].split(':')[-1])
    initfill_total_value_size += int(out[put_value_size_idx].split(':')[-1])

    result_str = str(datetime.datetime.now())
    result_str += str(i)
    result_str += ' ' + str(perf)
    result_str += ' ' + str(initfill_total_elapsed_time)
    result_str += ' ' + str(initfill_total_key_size)
    result_str += ' ' + str(initfill_total_value_size)
    f = open(outfile, 'a+')
    f.write(result_str + '\n')
    f.close()

# sustained
insert_cnt = int(insert_cnt / 2)
offset_list = [offset * loop_cnt, 0, insert_cnt]
pattern_list = [4, 0, 4]
insert_cnt_list = [insert_cnt , 0, insert_cnt]

sustained_total_elapsed_time = 0;
sustained_del_total_key_size = 0;
sustained_del_total_value_size = 0;
sustained_put_total_key_size = 0;
sustained_put_total_value_size = 0;
f = open(outfile, 'a+')
f.write('sustainded_result\n')
f.close()
for j in range(0, 2 * second_loop_cnt):
    if not (j % 2) :
        offset_list[2] -= insert_cnt
        temp = offset_list[2]
    else:
        offset_list[2] = temp + insert_cnt
    for i in range(0, loop_cnt):
        cmd = '../kv_perf -b -d ' + device + ' -s 0 -a ' + str(queue_depth) +' -v ' + ' '.join(map(str, value_list))+ ' -n '+ ' '.join(map(str, insert_cnt_list)) + ' -p ' + ' '.join(map(str, pattern_list)) + ' -o ' + ' '.join(map(str, offset_list)) + ' -g 1' + ' -j ' + str(JSON_PATH)
        print(cmd)
        offset_list[0] += insert_cnt_list[0]
        offset_list[2] += insert_cnt_list[2] * 2
        out = subprocess.check_output(cmd, shell=True).decode('utf-8').split('\n')
        #print(out)
        for k, o in enumerate(reversed(out)):
            #print(-(k+1), o)
            index = -(k+1)
            tmp_list = o.split(':')
            if (len(tmp_list) == 4):
                if tmp_list[0] == '0000':
                    if tmp_list[2] == '00.0 Device\'s Blend OPS/sec': #blend ops
                        perf_idx = index
            elif (len(tmp_list) == 3):
                if tmp_list[0] == 'OPS/sec': # elapsed time (need to add + usec)
                    time_idx = index
            elif (len(tmp_list) == 2):
                if tmp_list[0] == '[DEL] Total Value Size': # total value size
                    del_value_size_idx = index
                elif tmp_list[0] == '[DEL] Total Key Size': # total key size
                    del_key_size_idx = index
                elif tmp_list[0] == '[WRITE] Total Value Size': # total value size
                    put_value_size_idx = index
                elif tmp_list[0] == '[WRITE] Total Key Size': # total key size
                    put_key_size_idx = index
                    break
		
        #print(out[perf_idx])  # blend ops (e.g.0000:02:00.0 Device's Blend OPS/sec: 51020.41)
        #print(out[time_idx]) # time (e.g.OPS/sec: 51020.41, Test time: 196 usec)
        #print(out[del_key_size_idx]) # del key size (e.g.[DEL] Total Key Size: 80)
        #print(out[del_value_size_idx]) # del value size (e.g.[DEL] Total Value Size: 23552)
        #print(out[put_key_size_idx]) # put key size (e.g.[WRITE] Total Key Size: 80)
        #print(out[put_value_size_idx]) # put value size (e.g.[WRITE] Total Value Size: 23552)
        
        perf = float(out[perf_idx].split(':')[-1])
        sustained_total_elapsed_time += int(out[time_idx].split(':')[-1].split()[0])
        sustained_del_total_key_size += int(out[del_key_size_idx].split(':')[-1])
        sustained_del_total_value_size += int(out[del_value_size_idx].split(':')[-1])
        sustained_put_total_key_size += int(out[put_key_size_idx].split(':')[-1])
        sustained_put_total_value_size += int(out[put_value_size_idx].split(':')[-1])

        result_str = str(datetime.datetime.now())
        result_str += str(i + j * loop_cnt)
        result_str += ' ' + str(perf)
        result_str += ' ' + str(sustained_total_elapsed_time)
        result_str += ' ' + str(sustained_del_total_key_size)
        result_str += ' ' + str(sustained_del_total_value_size)
        result_str += ' ' + str(sustained_put_total_key_size)
        result_str += ' ' + str(sustained_put_total_value_size)
        f = open(outfile, 'a+')
        f.write(result_str + '\n')
        f.close()

print('########## Init Fill')
print('total_elapsed_time(ms): ' + str(initfill_total_elapsed_time))
print('total_key_size(byte): ' + str(initfill_total_key_size))
print('total_value_size(byte): ' + str(initfill_total_value_size))

print('########## Sustained')
print('total_elapsed_time(ms): ' + str(sustained_total_elapsed_time))
print('total_del_key_size(byte): ' + str(sustained_del_total_key_size))
print('total_del_value_size(byte): ' + str(sustained_del_total_value_size))
print('total_put_key_size(byte): ' + str(sustained_put_total_key_size))
print('total_put_value_size(byte): ' + str(sustained_put_total_value_size))
