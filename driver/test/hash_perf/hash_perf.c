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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <inttypes.h>
#include <syslog.h>
#include <unistd.h>
#include <sys/time.h>

#include "EagleHashIP.h"

void show_elapsed_time(struct timeval* start, struct timeval* end, char* msg, int repeat_count){
	long secs_used;
	long micros_used;
	printf("%s start: %lds, %ldus\n", (msg) ? msg : "", start->tv_sec, start->tv_usec);
	printf("%s end: %lds, %ldus\n",(msg) ? msg : "", end->tv_sec, end->tv_usec);
	secs_used=(end->tv_sec - start->tv_sec); //avoid overflow by subtracting first
	micros_used= ((secs_used*1000000) + end->tv_usec) - (start->tv_usec);
	printf("%s elapsed: %ldus %.3fms\n", (msg) ? msg : "", micros_used, ((float)micros_used) / 1000);
	if(repeat_count) {
		printf("==================================================\n");
		printf("%s elapsed: %.3fus per operation\n", (msg) ? msg : "", ((float)micros_used) / repeat_count);
		printf("==================================================\n\n\n");
	}
}

void show_log(char* in, unsigned long long* out, int out_len){
	int i;
	printf("in = %s\n",in);
	printf("out= ");
	for(i=0;i<out_len;i++){
		printf("%08llx ",out[i]);
	}	
	printf("\n");
}

int hash_perf(){
	int repeat = 1000 * 10000;
	int i,j;
	int len = 32;
	char in[len];
	int out_len = 8;
	unsigned long long out[out_len];

	struct timeval start = {0};
	struct timeval end = {0};

	unsigned long long seed = 0x0102030405060708LL;
	//Hash128_1_P128
	memset(out,0,sizeof(out));
	gettimeofday(&start, NULL);
	for(i=0;i<repeat;i++){
		snprintf(in,len, "mountain%08x",i);
		Hash128_1_P128(in, len, seed, out);
//		show_log(in,out,out_len);
	}
	gettimeofday(&end, NULL);
	show_elapsed_time(&start, &end, "Eagle Hash128_1_P128", repeat);


	//Hash128_2_P128
	for(i=0;i<repeat;i++){
		snprintf(in,len, "mountain%08x",i);
		Hash128_2_P128(in, len, seed, out);
//		show_log(in,out,out_len);
	}
	gettimeofday(&end, NULL);
	show_elapsed_time(&start, &end, "Eagle Hash128_2_P128", repeat);


	//MurmurHash3_x64_128
/*
	for(i=0;i<repeat;i++){
		snprintf(in,len, "mountain%08x",i);
		MurmurHash3_x64_128(in, len, seed, out);
//		show_log(in,out,out_len);
	}
	gettimeofday(&end, NULL);
	show_elapsed_time(&start, &end, "MurmurHash3_x64_128", repeat);
*/

	return 0;
}

int main(){
	int ret = 0;
	ret = hash_perf();
	return ret;
}
