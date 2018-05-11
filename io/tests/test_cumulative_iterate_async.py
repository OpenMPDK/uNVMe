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

# init fill
insert_cnt = 200000
offset = insert_cnt
queue_depth = 128
outfile = 'result'
miscompare = False
nr_iterate_session = 3;
loop_cnt = (int)(220000000 / (nr_iterate_session * insert_cnt))  #4KB value * 1M keys * 220 loop = around 840GB
print(loop_cnt)
f = open('result_async.txt', 'w')
for i in range(0, nr_iterate_session):
    f.write('thread' + str(i) + ':read      time(msec)    ')
f.write('\n')
f.close()

devNull = open('/dev/null', 'w')

for i in range(0, loop_cnt * offset, offset):
    cmd = '../cumulative_iterate_async ' + str(i) + ' ' + str(nr_iterate_session) + ' ' + str(insert_cnt)
    print(cmd)
    out = subprocess.call(cmd, shell=True, stderr=devNull)

devNull.close()
