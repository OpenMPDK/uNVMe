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

#include <pthread.h>

#define fail_unless(_c) do{     \
	if(!(_c)){ 		\
		printf("fail!!\n"); \
		exit(-1);	\
	}			\
}while(0)


typedef struct {
	int tid;
}thread_param;

#define KEY_LENGTH (16)

kv_pair** kv;
int value_size;
int insert_count;

void * cache_write(void * data){
	int i;
	int tid = ((thread_param*)data)->tid;
	unsigned char keybuf[32]={0,};
	kv_key key = {keybuf, KEY_LENGTH};
	
        for(i=0+(tid*insert_count);i<insert_count+(tid*insert_count);i++){
                //fprintf(stderr,"[%d]kv_cache_write: %s\n", tid, (char *)key.key);
                memset(key.key, 0, KEY_LENGTH);
                sprintf(key.key, "th%d%13d",tid,i);
                kv[i]->key = key;
                fail_unless(0 == kv_cache_write(kv[i]));
        }
}


void * cache_read(void * data){
	int i;
	int tid = ((thread_param*)data)->tid;
        unsigned char keybuf[32]={0,};
        kv_key key = {keybuf, KEY_LENGTH};

        for(i=insert_count+(tid*insert_count)-1; i>=0+(tid*insert_count);i--){
		//fprintf(stderr,"[%d]kv_cache_read: %s\n", tid, (char *)key.key);
                memset(key.key, 0, KEY_LENGTH);
                sprintf(key.key, "th%d%13d",tid,i);
                kv[i]->key = key;
                fail_unless(0 == kv_cache_read(kv[i]));
        }
}

void * cache_delete(void * data){
	int i;
	int tid = ((thread_param*)data)->tid;
        unsigned char keybuf[32]={0,};
        kv_key key = {keybuf, KEY_LENGTH};

        for(i=0+(tid*insert_count);i<insert_count+(tid*insert_count);i++){
		//fprintf(stderr,"[%d]kv_cache_delete: %s\n", tid, (char *)key.key);
                memset(key.key, 0, KEY_LENGTH);
                sprintf(key.key, "th%d%13d",tid,i);
                kv[i]->key = key;
                fail_unless(0 == kv_cache_delete(kv[i]));
        }
}


void test(void){
	printf("%s start\n",__FUNCTION__);
	value_size = 4096;
	int total_count = 50 * 10000;
	int j;
	
	for(j=1;j<=8;j*=2){
		insert_count = total_count/j;
		int ret = 0;
		int nthreads = j;

		kv = (kv_pair**)malloc(sizeof(kv_pair*)*insert_count*nthreads);
		fail_unless(kv != NULL);

		printf("\n*******************************************************\n");
		printf("%s value_size=%d insert_count=%d, num_threads=%d\n",__FUNCTION__,value_size,insert_count, nthreads);
		printf("*******************************************************\n");

		int i;
		struct timeval start;
		struct timeval end;

		//Init Cach
		gettimeofday(&start, NULL);
		kv_cache_init();
		gettimeofday(&end, NULL);
		show_elapsed_time(&start,&end,"kv_cache_init",0,0,NULL);
	
		//Prepare App Memory 
		fprintf(stderr,"Setup App: ");
		gettimeofday(&start, NULL);
		for(i=0;i<insert_count*nthreads;i++){
		        if(!(i%10000)){
		                fprintf(stderr,"%d ",i);
		        }
		        kv[i] = (kv_pair*)malloc(sizeof(kv_pair));
			fail_unless(kv[i] != NULL);
		        kv[i]->value.value = malloc(value_size);
		        kv[i]->value.length = value_size;
			fail_unless(kv[i]->value.value != NULL);
		        memset(kv[i]->value.value,'a'+(i%26),value_size);
		}
		gettimeofday(&end, NULL);
		fprintf(stderr,"Done\n");
		show_elapsed_time(&start,&end,"Setup App",insert_count*nthreads,0,NULL);

		//Create & Run Write Thread
		pthread_t t[nthreads];
		thread_param p[nthreads];	
		int status[nthreads];

		gettimeofday(&start, NULL);
		for(i=0;i<nthreads;i++){
			p[i].tid = i;
			fail_unless(0 <= pthread_create(&t[i], NULL, cache_write, &p[i]));
		}

		for(i=0;i<nthreads;i++){
			pthread_join(t[i],(void**)&status[i]);
		}
		gettimeofday(&end, NULL);
		show_elapsed_time(&start,&end,"kv_cache_muti_write",insert_count*nthreads,value_size,NULL);

		//Run Read Thread
		gettimeofday(&start, NULL);
		for(i=0;i<nthreads;i++){
			p[i].tid = i;
			fail_unless(0 <= pthread_create(&t[i], NULL, cache_read, &p[i]));
		}

		for(i=0;i<nthreads;i++){
			pthread_join(t[i],(void**)&status[i]);
		}
		gettimeofday(&end, NULL);
		show_elapsed_time(&start,&end,"kv_cache_multi_read",insert_count*nthreads,value_size,NULL);

		//Run Delete Thread
		gettimeofday(&start, NULL);
		for(i=0;i<nthreads;i++){
		        p[i].tid = i;
		        fail_unless(0 <= pthread_create(&t[i], NULL, cache_delete, &p[i]));
		}

		for(i=0;i<nthreads;i++){
		        pthread_join(t[i],(void**)&status[i]);
		}
		gettimeofday(&end, NULL);
		show_elapsed_time(&start,&end,"kv_cache_multi_delete",insert_count*nthreads,value_size,NULL);
	
		//Finalized Memory
		fprintf(stderr,"Teardown Memory: ");
		gettimeofday(&start, NULL);
		for(i=0;i<insert_count*nthreads;i++){
			if(!(i%10000)){
				fprintf(stderr,"%d ",i);
			}
			if(kv[i]->value.value) free(kv[i]->value.value);
			kv[i]->value.value = NULL;

			if(kv[i]) free(kv[i]);
			kv[i] = NULL;
		}
		free(kv);
		gettimeofday(&end, NULL);
		fprintf(stderr,"Done\n");
		show_elapsed_time(&start,&end,"Teardown Memory",insert_count,0,NULL);

		//Finalize Cache
		gettimeofday(&start, NULL);
		ret = kv_cache_finalize();
		gettimeofday(&end, NULL);
		show_elapsed_time(&start,&end,"kv_cache_finalize",0,0,NULL);
	
		fail_unless(ret == 0);
		printf("%s done\n",__FUNCTION__);
	}

}


int main(void)
{
	test();
	return 0;
}
