#!/usr/bin/env python3

import subprocess
import os
import sys
import time

# Default configuration file path
JSON_PATH = './kv_perf_default_config.json'

euid = os.geteuid()
if euid:
    print('Script not started as root. Running sudo..')
    args = ['sudo', sys.executable] + sys.argv + [os.environ]
    os.execlpe('sudo', *args)

device = '0000:02:00.0'
cmd = '../kv_perf -z -d ' + device + ' -j ' + str(JSON_PATH)
print(cmd)
subprocess.check_call(cmd, shell=True)

value_list = [v for r in (range(4096, 8193, 64), range(4096-64, 1023, -64)) for v in r]
#value_list = [4, 5, 6, 7, 8, 3, 2, 1]
#value_list = list(map(lambda x: x*1024, value_list))
insert_cnt = 1000000
queue_depth = 128
#print('expected usage(byte): ', insert_cnt * (sum(value_list) + len(value_list) * 16))
#print('expected usage(GB): ', insert_cnt * (sum(value_list) + len(value_list) * 16) / (1024 * 1024 * 1024))
outfile = 'result'

column_name = ['value', '1st_put', '2nd_put', '3rd_put', '1st_get', '2nd_get', '3rd_get', 'del', '1st_miscompare', '2nd_miscompare', '3rd_miscompare']
f = open(outfile, 'a+')
f.write(' '.join(column_name) + '\n')
f.close()

for i, value in enumerate(value_list):
    put_perf = [[],[],[]]
    get_perf = [[],[],[]]
    del_perf = []
    miscompare = [[],[],[]]

    for j in range(0, 3):
        cmd = '../kv_perf -x -d ' + device + ' -s 0 -a ' + str(queue_depth) + ' -v ' + str(value) + ' -n ' + str(insert_cnt) + ' -j ' + str(JSON_PATH)
        print(cmd)
        out = subprocess.check_output(cmd, shell=True).decode('utf-8').split('\n')[:-1]
        for k, o in enumerate(reversed(out)):
            #print(-(k+1), o)
            index = -(k+1)
            tmp_list = o.split(':')
            if (len(tmp_list) == 4):
                if tmp_list[0] == '0000':
                    if tmp_list[2] == '00.0 Device\'s Write OPS/sec': #put ops
                        write_perf_idx = index
                    elif tmp_list[2] == '00.0 Device\'s Read OPS/sec': #get ops
                        read_perf_idx = index
            elif (len(tmp_list) == 2):
                if tmp_list[0] == 'Miscompare count': # miscompare count
                    miscompare_idx = index
                    break

        #print(out[write_perf_idx]) # put ops (0000:02:00.0 Device's Write OPS/sec: 54054.05)
        #print(out[read_perf_idx]) # get ops (0000:02:00.0 Device's Read OPS/sec: 26954.18)
        #print(out[miscompare_idx]) # miscompare (Miscompare count: 0)
        #print(float(out[-5].split(':')[-1])) # put ops
        #print(float(out[-4].split(':')[-1])) # get ops
        #print(int(out[-30].split(':')[-1])) # miscompare
        put_perf[0].append(float(out[write_perf_idx].split(':')[-1]))
        get_perf[0].append(float(out[read_perf_idx].split(':')[-1]))
        miscompare[0].append(int(out[miscompare_idx].split(':')[-1]))

        cmd = '../kv_perf -x -d ' + device + ' -s 0 -a ' + str(queue_depth) + ' -v ' + str(value) + ' -n ' + str(insert_cnt) + ' -j ' + str(JSON_PATH)
        print(cmd)
        out = subprocess.check_output(cmd, shell=True).decode('utf-8').split('\n')[:-1]
        put_perf[1].append(float(out[write_perf_idx].split(':')[-1]))
        get_perf[1].append(float(out[read_perf_idx].split(':')[-1]))
        miscompare[1].append(int(out[miscompare_idx].split(':')[-1]))

        cmd = '../kv_perf -x -e -d ' + device + ' -s 0 -a ' + str(queue_depth) + ' -v ' + str(value) + ' -n ' + str(insert_cnt) + ' -j ' + str(JSON_PATH)
        print(cmd)
        out = subprocess.check_output(cmd, shell=True).decode('utf-8').split('\n')[:-1]
        #print(out)
        for k, o in enumerate(reversed(out)):
            #print(-(k+1), o)
            index = -(k+1)
            tmp_list = o.split(':')
            if (len(tmp_list) == 4):
                if tmp_list[0] == '0000':
                    if tmp_list[2] == '00.0 Device\'s Write OPS/sec': #put ops
                        write_perf_idx = index
                    elif tmp_list[2] == '00.0 Device\'s Read OPS/sec': #get ops
                        read_perf_idx = index
                    elif tmp_list[2] == '00.0 Device\'s Delete OPS/sec': #del ops
                        del_perf_idx = index
            elif (len(tmp_list) == 2):
                if tmp_list[0] == 'Miscompare count': # miscompare count
                    miscompare_idx = index
                    break

        #print(out[write_perf_idx]) # put ops (0000:02:00.0 Device's Write OPS/sec: 54054.05)
        #print(out[read_perf_idx]) # get ops (0000:02:00.0 Device's Read OPS/sec: 26954.18)
        #print(out[del_perf_idx]) # del ops (0000:02:00.0 Device's Delete OPS/sec: 0.00)
        #print(out[miscompare_idx]) # miscompare (Miscompare count: 0)
        #print(float(out[-5].split(':')[-1])) # put ops
        #print(float(out[-4].split(':')[-1])) # get ops
        #print(float(out[-3].split(':')[-1])) # del ops
        #print(int(out[-47].split(':')[-1])) # miscompare
        put_perf[2].append(float(out[write_perf_idx].split(':')[-1]))
        get_perf[2].append(float(out[read_perf_idx].split(':')[-1]))
        del_perf.append(float(out[del_perf_idx].split(':')[-1]))
        miscompare[2].append(int(out[miscompare_idx].split(':')[-1]))

    result_str = str(value)
    result_str += ' ' + str(sum(put_perf[0]) / len(put_perf[0]))
    result_str += ' ' + str(sum(put_perf[1]) / len(put_perf[1]))
    result_str += ' ' + str(sum(put_perf[2]) / len(put_perf[2]))
    result_str += ' ' + str(sum(get_perf[0]) / len(get_perf[0]))
    result_str += ' ' + str(sum(get_perf[1]) / len(get_perf[1]))
    result_str += ' ' + str(sum(get_perf[2]) / len(get_perf[2]))
    result_str += ' ' + str(sum(del_perf) / len(del_perf))
    result_str += ' ' + str(sum(miscompare[0]) / len(miscompare[0]))
    result_str += ' ' + str(sum(miscompare[1]) / len(miscompare[1]))
    result_str += ' ' + str(sum(miscompare[2]) / len(miscompare[2]))

    print(' '.join(column_name))
    print(result_str)
    f = open(outfile, 'a+')
    f.write(result_str + '\n')
    f.close()
