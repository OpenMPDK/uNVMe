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
#include "kvradix.h"
#include "kvcache.h"

START_TEST(test_kv_test){
	art_tree t;
	int res = art_tree_init(&t);
	fail_unless(res == 0);

	unsigned char key[32];
	int value_size= 256;
	int i;
	for(i=0;i<100;i++){
		memset(key,0,sizeof(key));
		sprintf((char*)key, "key%13d",i);
		unsigned char* value = malloc(value_size);
		memset(value,0,value_size);
		memset(value,'a'+(i%26),value_size);
		value[value_size-1]=0;
		printf("SET: %c\n",value[0]);
		fail_unless(NULL == art_insert(&t, key, strlen((char*)key), value));
	}


	for(i=0;i<100;i+=2){
		memset(key,0,sizeof(key));
		sprintf((char*)key, "key%13d",i);
		char* deleted = art_delete(&t, key, strlen((char*)key));
		if(deleted)
			printf("DELETED: %s\n",key);

	}

	for(i=0;i<100;i++){
		memset(key,0,sizeof(key));
		sprintf((char*)key, "key%13d",i);
		char* v = (char*)art_search(&t, key, strlen((char*)key));
		//fail_unless(v != NULL, "search result is NULL");
		printf("SEARCH: key=%s value=%s\n",key,(v == NULL)? "NULL" : v);
		free(v);
		v = NULL;
	}

	res = art_tree_destroy(&t);
	fail_unless(res == 0);
}
END_TEST

START_TEST(test_big_insert){
	printf("%s start\n",__FUNCTION__);
	art_tree t;

	int res = art_tree_init(&t);
	fail_unless(res == 0);

	unsigned char key[32];
	int value_size = 2048;
	int insert_count = 1*1000*1000;
        char** value = (char**)malloc(sizeof(char*)*1*1000*1000);
        fail_unless(value != NULL);

	printf("%s value_size=%d insert_count=%d\n",__FUNCTION__,value_size,insert_count);

	struct timeval start;
	struct timeval end;
	int i;

	//Setup Memory
	fprintf(stderr,"Setup Memory: ");
	gettimeofday(&start, NULL);
        for(i=0;i<insert_count;i++){
                if(!(i%10000)){
                        fprintf(stderr,"%d ",i);
                }
                value[i] = malloc(value_size);
                fail_unless(value[i] != NULL);
        }
        gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
        show_elapsed_time(&start,&end,"Setup Memory",insert_count,0,NULL);

	//Memset
	fprintf(stderr,"Memset: ");
        gettimeofday(&start, NULL);
        for(i=0;i<insert_count;i++){
                if(!(i%10000)){
                        fprintf(stderr,"%d ", i);
                }
                memset(value[i],'a'+(i%26),value_size);
                value[i][value_size-1]=0;
        }
        gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
        show_elapsed_time(&start,&end,"Memset",insert_count,value_size,NULL);

	//Tree Insert
	fprintf(stderr,"Insert: ");
	gettimeofday(&start, NULL);
	for(i=0;i<insert_count;i++){
		if(!(i%10000)){
			fprintf(stderr,"%d ",i);
		}
		sprintf((char*)key, "key%13d",i);
		fail_unless(NULL == art_insert(&t, key, strlen((char*)key), value[i]));
	}
	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	show_elapsed_time(&start,&end,"Insert",insert_count,value_size,NULL);

	//Tree Search
	fprintf(stderr,"Search: ");
	gettimeofday(&start, NULL);
	for(i=insert_count-1;i>=0;i--){
		if(!(i%10000)){
			fprintf(stderr,"%d ",i);
		}
		sprintf((char*)key, "key%13d",i);
		char* v = art_search(&t, key, strlen((char*)key));
		fail_unless(NULL != v);
	}
	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	show_elapsed_time(&start,&end,"Search",insert_count,value_size,NULL);

	//Tree Delete
	fprintf(stderr,"Delete: ");
        gettimeofday(&start, NULL);
        for(i=0;i<insert_count;i++){
                if(!(i%10000)){
			fprintf(stderr,"%d ",i);
                }
                sprintf(key, "key%13d",i);
                char* deleted = art_delete(&t, key, strlen(key));
                fail_unless(NULL != deleted);
        }
        gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
        show_elapsed_time(&start,&end,"Delete",insert_count,value_size,NULL);

	//Teardown Memory
	fprintf(stderr,"Teardoen: ");
	gettimeofday(&start, NULL);
	for(i=0;i<insert_count;i++){
		if(!(i%10000)){
			fprintf(stderr,"%d ",i);
		}
		if(value[i]) free(value[i]);
	}
	free(value);
	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	show_elapsed_time(&start,&end,"Teardown Memory",insert_count,0,NULL);

	res = art_tree_destroy(&t);
	fail_unless(res == 0);
	printf("%s done\n",__FUNCTION__);
}
END_TEST

int main(void)
{
	setlogmask(LOG_UPTO(LOG_DEBUG));

	Suite *s1 = suite_create("art");
	TCase *tc1 = tcase_create("art");
	int seconds_per_day = 24*60*60;
	tcase_set_timeout(tc1, seconds_per_day);
	SRunner *sr = srunner_create(s1);
	int nf;

	// Add the art tests
	suite_add_tcase(s1, tc1);
	//    tcase_add_test(tc1, test_kv_test);
	tcase_add_test(tc1, test_big_insert);

	srunner_run_all(sr, CK_ENV);
	nf = srunner_ntests_failed(sr);
	srunner_free(sr);

	return nf == 0 ? 0 : 1;
}

