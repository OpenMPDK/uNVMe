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
#include <check.h>
#include <unistd.h>
#include <sys/time.h>

#include "kvutil.h"
#include "kvslab.h"
#include "kvnvme.h"


START_TEST(alloc_perf){
	printf("%s start\n",__FUNCTION__);
	int ret;

	int i;
	struct timeval start;
	struct timeval end;

	int key_length = 32;
	int value_size = 4096;
	int insert_count = 1000;
        kv_pair** kv = (kv_pair**)malloc(sizeof(kv_pair*)*insert_count);
        fail_unless(kv != NULL);


	for(value_size = 1024; value_size <= 4096; value_size *= 2){
		printf("*****************************************\n");
		printf("%s value_size=%d insert_count=%d\n",__FUNCTION__,value_size,insert_count);
		printf("*****************************************\n\n");
		
		//POSIX ALLOC
		gettimeofday(&start, NULL);
		for(i=0;i<insert_count;i++){
		        if(!(i%10000)){
		                fprintf(stderr,"POSIX Setup Memory: %d\n",i);
		        }
		        kv[i] = posix_alloc_pair(key_length, value_size,0);
		        fail_unless(kv[i] != NULL);
		}
		gettimeofday(&end, NULL);
		show_elapsed_time(&start,&end,"POSIX Setup Memory",insert_count, 0, NULL);


		//POSIX FREE
		gettimeofday(&start, NULL);
		for(i=0;i<insert_count;i++){
			if(!(i%10000)){
				fprintf(stderr,"POSIX Teardown Memory: %d\n",i);
			}
			posix_free_pair(kv[i]);
		}
		gettimeofday(&end, NULL);
		show_elapsed_time(&start,&end,"POSIX Teardown Memory",insert_count, 0, NULL);
	
	
		//SLAB ALLOC
		int nr_slab = 1;
		size_t add_mem_size = (size_t)0;
		kvslab_init(1ull*1024*1024*1024, SLAB_MM_ALLOC_POSIX, nr_slab);
	
		gettimeofday(&start, NULL);
		for(i=0;i<insert_count;i++){
		        if(!(i%10000)){
		                fprintf(stderr,"SLAB Setup Memory: %d\n",i);
		        }
		        kv[i] = slab_alloc_pair(key_length, value_size,0);
		        fail_unless(kv[i] != NULL);
		}
		gettimeofday(&end, NULL);
		show_elapsed_time(&start,&end,"SLAB Setup Memory",insert_count, 0, NULL);


		//SLAB FREE
		gettimeofday(&start, NULL);
		for(i=0;i<insert_count;i++){
			if(!(i%10000)){
				fprintf(stderr,"SLAB Teardown Memory: %d\n",i);
			}
			slab_free_pair(kv[i]);
		}
		gettimeofday(&end, NULL);
		show_elapsed_time(&start,&end,"SLAB Teardown Memory",insert_count, 0, NULL);

		kvslab_destroy();
	}

	if(kv) free(kv);
	kv = NULL;

}
END_TEST

int main(void)
{
	setlogmask(LOG_UPTO(LOG_DEBUG));

	Suite *s1 = suite_create("slab");
	TCase *tc1 = tcase_create("slab");
	int seconds_per_day = 24*60*60;
	tcase_set_timeout(tc1, seconds_per_day);
	SRunner *sr = srunner_create(s1);
	int nf;

	suite_add_tcase(s1,tc1);
	tcase_add_test(tc1, alloc_perf);

	srunner_run_all(sr, CK_ENV);
	nf = srunner_ntests_failed(sr);
	srunner_free(sr);

	return nf == 0 ? 0 : 1;
}

