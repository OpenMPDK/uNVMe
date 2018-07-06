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
#include "kv_types.h"
#include "kv_apis.h"
#include "kvnvme.h"


START_TEST(sdk_exist_append_iterate){
	printf("%s start\n",__FUNCTION__);

	int key_length = 16;
	int value_size = 4096;
	int insert_count = 10;

	int i;
	int ret;
	struct timeval start;
	struct timeval end;

	/*sdk init from seperate data stucture*/
	gettimeofday(&start, NULL);
	kv_sdk sdk_opt;
	memset(&sdk_opt,0 ,sizeof(kv_sdk));
	sdk_opt.use_cache = false;
	sdk_opt.cache_algorithm = CACHE_ALGORITHM_RADIX;
	sdk_opt.cache_reclaim_policy = CACHE_RECLAIM_LRU;
	sdk_opt.slab_size = 512*1024*1024;
	sdk_opt.slab_alloc_policy = SLAB_MM_ALLOC_HUGE;
	sdk_opt.ssd_type = KV_TYPE_SSD;
	sdk_opt.nr_ssd = 1;
	sdk_opt.log_level = 0;
	strcpy(sdk_opt.log_file, "/tmp/kvsdk.log");

	strcpy(sdk_opt.dev_id[0], "0000:02:00.0");
	sdk_opt.dd_options[0].core_mask = 1;
	sdk_opt.dd_options[0].sync_mask = 0xF;
	sdk_opt.dd_options[0].num_cq_threads = 1;
	sdk_opt.dd_options[0].cq_thread_mask = 2;
	sdk_opt.dd_options[0].queue_depth = 256;

	ret = kv_sdk_init(KV_SDK_INIT_FROM_STR, &sdk_opt);
	fail_unless(ret == KV_SUCCESS);
	gettimeofday(&end, NULL);
	show_elapsed_time(&start,&end,"sdk_init",0, 0, NULL);

	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(0,&cpuset);
	sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

	int current = kv_get_core_id();

	int nr_handle = 0;
	uint64_t arr_handle[MAX_CPU_CORES];
	memset((char*)arr_handle, 0, sizeof(arr_handle));
	ret = kv_get_devices_on_cpu(current, &nr_handle, arr_handle);
	if(ret != KV_SUCCESS){
		printf("Fail to acquire valid KV SSD Handle\n");
		exit(1);
	}

	printf("current core(%d) is allowed to access on handle [",current);
	for(i=0;i<nr_handle;i++){
		printf("%ld,",arr_handle[i]);
	}
	printf("]\n");

	uint64_t handle = arr_handle[0];
	
        kv_pair** kv = (kv_pair**)malloc(sizeof(kv_pair*)*insert_count);
        fail_unless(kv != NULL);

	fprintf(stderr, "%s value_size=%d insert_count=%d\n",__FUNCTION__,value_size,insert_count);

	uint64_t total_size = 0;
	uint64_t used_size = 0;
	total_size = kv_get_total_size(handle);
	used_size = kv_get_used_size(handle);
	if (total_size != UINT64_MAX) {
		fprintf(stderr, "Total Size of the NVMe Device: %lld MB\n", (unsigned long long)total_size / MB);
	}
	if (used_size != UINT64_MAX) {
		fprintf(stderr, "Used Size of the NVMe Device: %.2f %s\n", (float)used_size / 100, "%");
	}

	double waf = .0;
	waf = kv_get_waf(handle);
	if (waf != UINT32_MAX){
		waf /= 10;
		fprintf(stderr, "WAF Before doing I/O: %f\n", waf);
	}

	char logbuf[512];
	int log_id = 0xC0;
	int line = 16;
	ret = kv_get_log_page(handle, log_id, logbuf, sizeof(logbuf));
	if(ret == KV_SUCCESS){
		for(i=0;i<(signed)sizeof(logbuf);i++){
			if(!(i%line)){
				fprintf(stderr, "%04x: ", i);
			}
			fprintf(stderr, "%02x ", *(unsigned char*)(logbuf+i));

			if(i>0 && !((i+1)%line)){
				fprintf(stderr, "\n");
			}
		}
	}

	//Prepare App Memory 
	fprintf(stderr,"Setup App: ");
	gettimeofday(&start, NULL);
        for(i=0;i<insert_count;i++){
                if(!(i%10000)){
                        fprintf(stderr,"%d ",i);
                }
                kv[i] = (kv_pair*)malloc(sizeof(kv_pair));
		fail_unless(kv[i] != NULL);

                kv[i]->key.key = malloc(key_length + 1);
		fail_unless(kv[i]->key.key != NULL);
		kv[i]->key.length = key_length;
                memset(kv[i]->key.key,0,key_length + 1);

                kv[i]->keyspace_id = KV_KEYSPACE_IODATA;

                kv[i]->value.value = malloc(value_size*2); //for retrieve appended kv pair
		fail_unless(kv[i]->value.value != NULL);
                kv[i]->value.length = value_size;
		kv[i]->value.offset = 0;
		memset(kv[i]->value.value,'a'+(i%26), value_size);
        }
        gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
        show_elapsed_time(&start,&end,"Setup App",insert_count, 0, NULL);


	//SDK Write
	fprintf(stderr,"kv_store: \n");
	gettimeofday(&start, NULL);
	for(i=0;i<insert_count;i++){
		if(!(i%10000)){
			fprintf(stderr,"%d ", i);
		}
		sprintf(kv[i]->key.key, "mountain%08x",i);
		kv[i]->key.length=strlen(kv[i]->key.key);
		kv[i]->param.io_option.store_option = KV_STORE_DEFAULT;
		kv[i]->param.private_data = NULL;
		kv[i]->param.async_cb = NULL;
		fail_unless(KV_SUCCESS == kv_store(handle, kv[i]));
		//printf("WRITE k=%s v=%s\n",(char*)kv[i]->key.key,(char*)kv[i]->value.value);
	}
	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	show_elapsed_time(&start,&end,"kv_store",insert_count, value_size, NULL);

        //SDK Append
	// it is monitored that sending an append cmd make kv_ssd hanng which need to download fw aga    in. so fix it not to send to avoid it . (2017/09/29
	/*
	fprintf(stderr,"kv_append: ");
        gettimeofday(&start, NULL);
        for(i=0;i<insert_count;i++){
                if(!(i%10000)){
                        fprintf(stderr,"%d ", i);
                }
                sprintf(kv[i]->key.key, "mountain%08x",i);
                kv[i]->key.length=strlen(kv[i]->key.key);
                kv[i]->param.io_option.append_option = KV_APPEND_DEFAULT;
                kv[i]->param.private_data = NULL;
                kv[i]->param.async_cb = NULL;
                fail_unless(0 == kv_append(handle, kv[i]));
		//printf("APPEND k=%s v=%s\n",(char*)kv[i]->key.key,(char*)kv[i]->value.value);
        }
        gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
        show_elapsed_time(&start,&end,"kv_append",insert_count, value_size, NULL);
	*/

	//SDK Read
	fprintf(stderr,"kv_retrieve: ");
	gettimeofday(&start, NULL);
	for(i=0;i<insert_count;i++){
		if(!(i%10000)){
			fprintf(stderr,"%dn",i);
		}
		sprintf(kv[i]->key.key, "mountain%08x",i);
		kv[i]->key.length=strlen(kv[i]->key.key);
		kv[i]->param.io_option.retrieve_option = KV_RETRIEVE_DEFAULT;
		kv[i]->param.private_data = NULL;
		kv[i]->param.async_cb = NULL;
		fail_unless(KV_SUCCESS == kv_retrieve(handle, kv[i])); //NOTE: a length of the data retrieved should be same with 2*value_size
		//printf("READ k=%s v=%s\n",(char*)kv[i]->key.key,(char*)kv[i]->value.value);
	}
	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	show_elapsed_time(&start,&end,"kv_retrieve",insert_count, value_size, NULL);

	//SDK Exist
	fprintf(stderr,"kv_exist: - before delete ");
	gettimeofday(&start, NULL);
	for(i=0;i<insert_count;i++){
		if(!(i%10000)){
			fprintf(stderr,"%d ",i);
		}
		sprintf(kv[i]->key.key, "mountain%08x",i);
		kv[i]->key.length=strlen(kv[i]->key.key);
		kv[i]->param.io_option.exist_option = KV_EXIST_DEFAULT;
		kv[i]->param.private_data = NULL;
		kv[i]->param.async_cb = NULL;
		fail_unless(KV_SUCCESS == kv_exist(handle, kv[i]));
	}
	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	show_elapsed_time(&start,&end,"kv_exist - before delete",insert_count, 0, NULL);

	//SDK Delete
	fprintf(stderr,"kv_delete: ");
	gettimeofday(&start, NULL);
	for(i=0;i<insert_count;i++){
		if(!(i%10000)){
			fprintf(stderr,"%d ",i);
		}
		sprintf(kv[i]->key.key, "mountain%08x",i);
		kv[i]->key.length=strlen(kv[i]->key.key);
		kv[i]->param.io_option.delete_option = KV_DELETE_DEFAULT;
		kv[i]->param.private_data = NULL;
		kv[i]->param.async_cb = NULL;
		fail_unless(KV_SUCCESS == kv_delete(handle, kv[i]));
	}
	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	show_elapsed_time(&start,&end,"kv_delete",insert_count, 0, NULL);

	//SDK Exist
	fprintf(stderr,"kv_exist: - after delete");
	gettimeofday(&start, NULL);
	for(i=0;i<insert_count;i++){
		if(!(i%10000)){
			fprintf(stderr,"%d ",i);
		}
		sprintf(kv[i]->key.key, "mountain%08x",i);
		kv[i]->key.length=strlen(kv[i]->key.key);
		kv[i]->param.io_option.exist_option = KV_EXIST_DEFAULT;
		kv[i]->param.private_data = NULL;
		kv[i]->param.async_cb = NULL;
		fail_unless(KV_ERR_NOT_EXIST_KEY == kv_exist(handle, kv[i]));
	}
	gettimeofday(&end, NULL);
	fprintf(stderr,"Done\n");
	show_elapsed_time(&start,&end,"kv_exist - after delete",insert_count, 0, NULL);

	//Teardown Memory
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
	show_elapsed_time(&start,&end,"Teardown Memory",insert_count, 0, NULL);

	waf = kv_get_waf(handle);
	if (waf != UINT32_MAX){
		waf /= 10;
		printf("WAF After doing I/O: %f\n\n", waf);
	}

	//Finalize SDK
	gettimeofday(&start, NULL);
	kv_sdk_finalize();
	gettimeofday(&end, NULL);
	show_elapsed_time(&start,&end,"nvme_finalize",0, 0, NULL);
}
END_TEST

int main(void)
{
	kv_sdk_info();

	setlogmask(LOG_UPTO(LOG_DEBUG));

	Suite *s1 = suite_create("sdk_exist_append_iterate");
	TCase *tc1 = tcase_create("sdk_exist_append_iterate");
	int seconds_per_day = 24*60*60;
	tcase_set_timeout(tc1, seconds_per_day);
	SRunner *sr = srunner_create(s1);
	int nf;

	suite_add_tcase(s1, tc1);
	tcase_add_test(tc1, sdk_exist_append_iterate);

	srunner_run_all(sr, CK_ENV);
	nf = srunner_ntests_failed(sr);
	srunner_free(sr);

	return nf == 0 ? 0 : 1;
}

