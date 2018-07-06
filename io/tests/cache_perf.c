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

#include "kvutil.h"
#include "kvcache.h"

#define fail_unless(_c) do{     \
        if(!(_c)){              \
                printf("fail!!\n"); \
                exit(-1);       \
        }                       \
}while(0)

int cache_perf(void){
	printf("%s start\n",__FUNCTION__);

	int key_length = 16;
	int value_size = 4096;
	int insert_count = 10 * 10000;
	int ret = 0;

	unsigned char keybuf[32];
        kv_pair** kv = (kv_pair**)malloc(sizeof(kv_pair*)*insert_count);
        fail_unless(kv != NULL);

	printf("%s value_size=%d insert_count=%d\n",__FUNCTION__,value_size,insert_count);

	int i;
	struct timeval start;
	struct timeval end;

	//Init Cache
	gettimeofday(&start, NULL);
	kv_cache_init();
        gettimeofday(&end, NULL);
        show_elapsed_time(&start,&end,"kv_cache_init",0,0,NULL);
	
	//Prepare App Memory 
	fprintf(stderr,"Setup App: ");
	gettimeofday(&start, NULL);
        for(i=0;i<insert_count;i++){
                if(!(i%10000)){
                        fprintf(stderr,"%d ", i);
                }
                kv[i] = (kv_pair*)malloc(sizeof(kv_pair));
		fail_unless(kv[i] != NULL);

		kv[i]->key.key = malloc(key_length + 1);
		fail_unless(kv[i]->key.key != NULL);
		kv[i]->key.length = key_length;

                kv[i]->value.value = malloc(value_size);
		fail_unless(kv[i]->value.value != NULL);
                kv[i]->value.length = value_size;
                memset(kv[i]->value.value,'a'+(i%26),value_size);
        }
        gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
        show_elapsed_time(&start,&end,"Setup App",insert_count,0,NULL);

	//Cache Write
	fprintf(stderr,"kv_cache_write: ");
	gettimeofday(&start, NULL);
	for(i=0;i<insert_count;i++){
		if(!(i%10000)){
			fprintf(stderr,"%d ", i);
		}
		sprintf(kv[i]->key.key, "key%13d",i);
		fail_unless(0 == kv_cache_write(kv[i]));
		//printf("CACHE WRITE key=%s value=%s\n",(char*)kv[i]->key.key, (char*)kv[i]->value.value);
	}
	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	show_elapsed_time(&start,&end,"kv_cache_write",insert_count,value_size,NULL);

	//Cache Read
	fprintf(stderr,"kv_cache_read: ");
	gettimeofday(&start, NULL);
	for(i=insert_count-1;i>=0;i--){
		if(!(i%10000)){
			fprintf(stderr,"%d ",i);
		}
		memset(kv[i]->key.key,0,key_length);
		sprintf(kv[i]->key.key, "key%13d",i);
		fail_unless(0 == kv_cache_read(kv[i]));
		//printf("CACHE READ key=%s value=%s\n",(char*)kv[i]->key.key, (char*)kv[i]->value.value);
	}
	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	show_elapsed_time(&start,&end,"kv_cache_read",insert_count,value_size,NULL);

	//Cache Delete
	fprintf(stderr,"kv_cache_delete: ");
        gettimeofday(&start, NULL);
        for(i=0;i<insert_count;i++){
                if(!(i%10000)){
                        fprintf(stderr,"%d ",i);
                }
		memset(kv[i]->key.key,0,key_length);
		sprintf(kv[i]->key.key, "key%13d",i);
                fail_unless(0 == kv_cache_delete(kv[i]));
        }
        gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
        show_elapsed_time(&start,&end,"kv_cache_delete", insert_count,value_size,NULL);

	//Finalized Memory
	fprintf(stderr,"Teardown Memory: ");
	gettimeofday(&start, NULL);
	for(i=0;i<insert_count;i++){
		if(!(i%10000)){
			fprintf(stderr,"%d ",i);
		}
		if(kv[i]->key.key) free(kv[i]->key.key);
		if(kv[i]->value.value) free(kv[i]->value.value);
		if(kv[i]) free(kv[i]);
	}
	free(kv);
	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	show_elapsed_time(&start,&end,"Teardown Memory",insert_count,0,NULL);

	//Init Cache
	gettimeofday(&start, NULL);
	ret = kv_cache_finalize();
        gettimeofday(&end, NULL);
        show_elapsed_time(&start,&end,"kv_cache_finalize",insert_count,0,NULL);
	
	fail_unless(ret == 0);
	printf("%s done\n",__FUNCTION__);

	return ret;
}

int main(void)
{
	int ret = cache_perf();
	return ret;
}

