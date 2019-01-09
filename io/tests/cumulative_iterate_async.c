#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <inttypes.h>
#include <syslog.h>
#include <unistd.h>
#include <sys/time.h>
#include <signal.h>

#include "kvutil.h"
#include "kv_types.h"
#include "kv_apis.h"
#include "kvnvme.h"

#define DISPLAY_CNT (10000)
#define fail_unless(_c) do{     \
        if(!(_c)){              \
                fprintf(stdout, "fail at LINE: %d\n", __LINE__); \
                exit(1);	\
        }                       \
}while(0)

typedef struct {
	struct timeval start;
	struct timeval end;
	int tid;
	int cmd_order;
} time_stamp;

typedef struct {
	int tid;
	int core_id;
	uint64_t device_handle;
	struct timeval *t_start;
	struct timeval *t_end;
} thread_param;

// thread variables
unsigned int* g_store_complete_count;
unsigned int g_iterate_read_complete_count[KV_MAX_ITERATE_HANDLE];

int g_submitted_num_cmd[KV_MAX_ITERATE_HANDLE];
int g_num_read_key[KV_MAX_ITERATE_HANDLE];
long* g_total_time;
pthread_mutex_t* g_mutex_eof_flag;
int* g_eof_flag;
struct timeval* g_end;

int g_nr_it_handle = 1;

// global variables
int nthreads = 0;
int nr_handle = 0;

uint8_t iterate_type = KV_KEY_ITERATE;

int start_point;
int key_length = 16;
int value_size = 4096;
int insert_count = 10 * 10000;
int iterate_read_count = 10000;
int iterate_buffer_size = 32 * 1024; //32KB
kv_pair** kv;
kv_iterate** it;

void myalarm() {
	int i;
	for (i = 0; i < nthreads; i++) {
		fprintf(stdout, "    [tid=%d]num_read_key=%d, submit_count=%d, complete_count=%u\n", i, g_num_read_key[i], g_submitted_num_cmd[i], g_iterate_read_complete_count[i]);
	}
	fprintf(stdout, "\n");
        fflush(stdout);
        alarm(3);
}

void async_iterate_read_cb(kv_iterate* it, unsigned int result, unsigned int status) {
	int cur_num_read_key = 0;
	if (!it) {
		fprintf(stdout, "%s fails: kv_iterate is NULL\n", __FUNCTION__);
		exit(1);
	}

	time_stamp* stamp = (time_stamp*)it->kv.param.private_data;

	if ((status != KV_SUCCESS) && (status != KV_ERR_ITERATE_READ_EOF)) {
		fprintf(stdout, "[%s] error. result=%d status=%d\n", __FUNCTION__, result, status);
		exit(1);
	}

	// async callback completion sequence validation logic
	if (status == KV_ERR_ITERATE_READ_EOF) {
		pthread_mutex_lock(&g_mutex_eof_flag[stamp->tid]);

		// when first eof read, check it to g_eof_flag
		if (!g_eof_flag[stamp->tid]) {
			g_eof_flag[stamp->tid] = 1;
			gettimeofday(&g_end[stamp->tid], NULL);
			fprintf(stdout, "    [EOF]    \n");
		}
		pthread_mutex_unlock(&g_mutex_eof_flag[stamp->tid]);
	}

//	fprintf(stdout, "[%dth / %d] Iterate Read Result: , it->value.length=%d status=%d\n\n", ((struct time_stamps*)(it->kv.param.private_data))->cmd_order, submitted_num_cmd, it->kv.value.length, status);
	gettimeofday(&stamp->end, NULL);
	pthread_mutex_lock(&g_mutex_eof_flag[stamp->tid]);
	if (it->kv.value.length > 0) {
		if(it->kv.key.length == 0) {
			memcpy(&cur_num_read_key, it->kv.value.value, KV_ITERATE_READ_BUFFER_OFFSET);
			g_num_read_key[stamp->tid] += cur_num_read_key;
		} else {
			g_num_read_key[stamp->tid]++;
		}
	}
	pthread_mutex_unlock(&g_mutex_eof_flag[stamp->tid]);

	g_iterate_read_complete_count[stamp->tid]++;
}

void async_store_cb(kv_pair* kv, unsigned int result, unsigned int status) {
	if (status != KV_SUCCESS) {
		fprintf(stdout,
				"[%s] error. key=%s option=%d value.length=%d value.offset=%d status code=%d\n",
				__FUNCTION__, (char*) kv->key.key,
				kv->param.io_option.delete_option, kv->value.length,
				kv->value.offset, status);
		exit(1);
	}
	time_stamp* stamp = (time_stamp*)kv->param.private_data;
	gettimeofday(&stamp->end, NULL);
	g_store_complete_count[stamp->tid]++;
}

void prepare_app_memory(int nthreads) {
	int i;
	int ret;

	kv = (kv_pair**) malloc(sizeof(kv_pair*) * insert_count * nthreads);
	fail_unless(kv != NULL);
	for (i = 0; i < insert_count * nthreads; i++) {
		if (!(i % DISPLAY_CNT)) {
			fprintf(stdout, "%d ", i);
			fflush(stdout);
		}
		kv[i] = (kv_pair*) malloc(sizeof(kv_pair));
		fail_unless(kv[i] != NULL);

		kv[i]->keyspace_id = KV_KEYSPACE_IODATA;
		kv[i]->key.key = malloc(key_length);
		fail_unless(kv[i]->key.key != NULL);
		kv[i]->key.length = key_length;
		memset(kv[i]->key.key, 0, key_length);

		kv[i]->value.value = malloc(value_size); //for retrieve appended kv pair
		fail_unless(kv[i]->value.value != NULL);
		kv[i]->value.length = value_size;
		kv[i]->value.offset = 0;
		memset(kv[i]->value.value, 'a' + (i % 26), value_size);

		kv[i]->param.private_data = malloc(sizeof(time_stamp));
		fail_unless(kv[i]->param.private_data != NULL);
		memset(kv[i]->param.private_data, 0, sizeof(time_stamp));
	}

	it = (kv_iterate**) malloc(sizeof(kv_iterate*) * iterate_read_count * nthreads);
	fail_unless(it != NULL);
	for (i = 0; i < iterate_read_count * nthreads; i++) {
		it[i] = (kv_iterate*)malloc(sizeof(kv_iterate));
		fail_unless(it[i]);
		it[i]->iterator = KV_INVALID_ITERATE_HANDLE;

		it[i]->kv.value.value = malloc(iterate_buffer_size);
		fail_unless(it[i]->kv.value.value);
		it[i]->kv.value.length = iterate_buffer_size;
		it[i]->kv.value.offset = 0;
		memset(it[i]->kv.value.value, 0, it[i]->kv.value.length);

		it[i]->kv.key.key = malloc(key_length);
		fail_unless(it[i]->kv.key.key);
		it[i]->kv.key.length = key_length;
		memset(it[i]->kv.key.key, 0, it[i]->kv.key.length);

		it[i]->kv.param.private_data = malloc(sizeof(time_stamp));
		fail_unless(it[i]->kv.param.private_data);
	}

	g_store_complete_count = (int*)calloc(nthreads, sizeof(int));
	fail_unless(g_store_complete_count != NULL);

	g_total_time = (long*)calloc(nthreads, sizeof(long));
	fail_unless(g_total_time);
	g_mutex_eof_flag = (pthread_mutex_t*)calloc(nthreads, sizeof(pthread_mutex_t));
	fail_unless(g_mutex_eof_flag);
	g_eof_flag = (int*)calloc(nthreads, sizeof(int));
	fail_unless(g_eof_flag);
	g_end = (struct timeval*)calloc(nthreads, sizeof(struct timeval));
	fail_unless(g_end);
}

void teardown_memory(int nthreads) {
	int i;

	for (i = 0; i < insert_count * nthreads; i++) {
		if (!(i % 10000)) {
			fprintf(stdout, "%d ", i);
			fflush(stdout);
		}
		if (kv[i]->key.key) free(kv[i]->key.key);
		if (kv[i]->value.value) free(kv[i]->value.value);
		if (kv[i]) free(kv[i]);
	}
	if (kv) free(kv);

	for (i = 0; i < iterate_read_count * nthreads; i++) {
		if (it[i]->kv.key.key) free(it[i]->kv.key.key);
		if (it[i]->kv.value.value) free(it[i]->kv.value.value);
		if (it[i]->kv.param.private_data) free(it[i]->kv.param.private_data);
		if (it[i]) free(it[i]);
	}
	if (it) free(it);

	if(g_store_complete_count) free(g_store_complete_count);

	if(g_total_time) free(g_total_time);
	if(g_mutex_eof_flag) free(g_mutex_eof_flag);
	if(g_eof_flag) free(g_eof_flag);
	if(g_end) free(g_end);
}

void* sdk_write(void* data) {
	thread_param *param = (thread_param*) data;
	int i, tid = param->tid;
	int core_id = param->core_id;
	uint64_t handle = param->device_handle;

	//set affinity as core_id
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(core_id, &cpuset);
	sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

	fprintf(stderr, "[cid=%d][tid=%d][handle=%lu]kv_store_async: ", core_id, tid, handle);
	//prepare
	for (i = 0 + (tid * insert_count); i < insert_count + (tid * insert_count); i++) {
		int tmp_key = i + (start_point * g_nr_it_handle);
		memset(kv[i]->key.key, 0, key_length);
		memcpy(kv[i]->key.key + ((size_t)key_length > sizeof(int) ? (key_length - sizeof(i)) : 0), &tmp_key, MIN((size_t)key_length, sizeof(int)));
		kv[i]->param.async_cb = async_store_cb;
		kv[i]->param.io_option.store_option = KV_STORE_DEFAULT;
		((time_stamp *) kv[i]->param.private_data)->tid = tid;
	}

	//IO
	gettimeofday(param->t_start, NULL);
	for (i = 0 + (tid * insert_count); i < insert_count + (tid * insert_count); i++) {
		if (!((i - (tid * insert_count)) % DISPLAY_CNT)) {
			fprintf(stderr, "%d ", (i - (tid * insert_count)));
		}
		gettimeofday(&((time_stamp *) kv[i]->param.private_data)->start, NULL);
		fail_unless(0 == kv_store_async(handle, kv[i]));
	}
	fprintf(stderr, "Done\n");

	while (g_store_complete_count[tid] != insert_count) {
		usleep(1);
	}
	gettimeofday(param->t_end, NULL);
}

void* sdk_iterate(void* data) {
	thread_param *param = (thread_param*) data;
	int i, ret, tid = param->tid;
	int core_id = param->core_id;
	uint64_t handle = param->device_handle;

	//set affinity as core_id
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(core_id, &cpuset);
	sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

	uint32_t bitmask = 0xFFFFFFFF;
	uint32_t prefix = 0;
	uint8_t keyspace_id = KV_KEYSPACE_IODATA;
	memcpy(&prefix, &tid, sizeof(int));
	uint32_t iterator = KV_INVALID_ITERATE_HANDLE;
	fprintf(stdout, "[tid=%d]Iterate Open : keyspace_id=%d bitmask=%x bit_pattern=%x\n", tid, keyspace_id, bitmask, prefix);

	iterator = kv_iterate_open(handle, keyspace_id, bitmask, prefix, iterate_type);
	if(iterator != KV_INVALID_ITERATE_HANDLE && iterator != KV_ERR_ITERATE_ERROR){
		fprintf(stdout, "[tid=%d]Iterate open success : iterator id=%d\n", tid, iterator);
ITER_READ_PART:
		for (i = 0 + (tid * iterate_read_count); i < iterate_read_count + (tid * iterate_read_count); i++) {
			it[i]->iterator = iterator;
			it[i]->kv.value.length = iterate_buffer_size;
			it[i]->kv.value.offset = 0;
			it[i]->kv.param.io_option.iterate_read_option = KV_ITERATE_READ_DEFAULT;
			it[i]->kv.param.async_cb = async_iterate_read_cb;
			((time_stamp*)(it[i]->kv.param.private_data))->tid = tid;
		}
		gettimeofday(param->t_start, NULL);
		for (i = 0 + (tid * iterate_read_count); i < iterate_read_count + (tid * iterate_read_count); i++) {
			//fprintf(stderr, "Iterate Read Request=%d\n", it[i]->value.length);
			ret = kv_iterate_read_async(handle, it[i]);
			g_submitted_num_cmd[tid]++;
		}

		while (g_iterate_read_complete_count[tid] < iterate_read_count) {
			usleep(1);
		}
		gettimeofday(param->t_end, NULL);
		if (g_eof_flag[tid] == 0) {
			g_submitted_num_cmd[tid] = 0;
			g_total_time[tid] += ((param->t_end->tv_sec - param->t_start->tv_sec) * 1000000) + param->t_end->tv_usec - param->t_start->tv_usec;
			g_iterate_read_complete_count[tid] = 0;
			goto ITER_READ_PART;
		}
		g_total_time[tid] += ((g_end[tid].tv_sec - param->t_start->tv_sec) * 1000000) + g_end[tid].tv_usec - param->t_start->tv_usec;

		kv_iterate_close(handle, iterator);
	}
}

int sdk_iterate_async(void) {
	fprintf(stderr,"%s start\n", __FUNCTION__);
	srand(time(NULL)); //for random prefix

	// local global variables
	int i;
	int ret;
	struct timeval start;
	struct timeval end;
	struct latency_stat stat;
	kv_sdk sdk_opt;

	// local thread variables
	int set_cores[MAX_CPU_CORES];
	uint64_t arr_handle[NR_MAX_SSD];
	pthread_t t[MAX_CPU_CORES * NR_MAX_SSD];
	thread_param p[MAX_CPU_CORES * NR_MAX_SSD];
	int status[MAX_CPU_CORES * NR_MAX_SSD];
	struct timeval thread_start[MAX_CPU_CORES * NR_MAX_SSD];
	struct timeval thread_end[MAX_CPU_CORES * NR_MAX_SSD];

	memset((char*) arr_handle, 0, sizeof(arr_handle));
	memset((char*) set_cores, 0, sizeof(set_cores));

	/*sdk init from seperate data stucture*/
	gettimeofday(&start, NULL);
	memset(&sdk_opt, 0, sizeof(kv_sdk));
	strcpy(sdk_opt.dev_id[0], "0000:02:00.0");
	sdk_opt.dd_options[0].core_mask = 0x0;
	for(i = 0; i < g_nr_it_handle; i++) {
		sdk_opt.dd_options[0].core_mask |= (1 << i);
	}
	sdk_opt.dd_options[0].cq_thread_mask = 0x80;
	sdk_opt.dd_options[0].queue_depth = 128;
	ret = kv_sdk_init(KV_SDK_INIT_FROM_STR, &sdk_opt);
	fail_unless(ret == KV_SUCCESS);
	gettimeofday(&end, NULL);
	show_elapsed_time(&start, &end, "sdk_init", 0, 0, NULL);

	for (i = 0; i < sdk_opt.nr_ssd; i++) {
		for (int j = 0; j < MAX_CPU_CORES; j++) {
			if ((sdk_opt.dd_options[i].core_mask & (1ULL << j)) == 0)
				continue;
			if (!set_cores[j]) {
				set_cores[j] = 1;
				//get accesible cores by current core j
				ret = kv_get_devices_on_cpu(j, &nr_handle, arr_handle);
				fprintf(stderr,"current core(%d) is allowed to access on handle [", j);
				//make threads which number is same with the number of accessible devices
				for (int k = 0; k < nr_handle; k++) {
					fprintf(stderr,"%ld,", arr_handle[k]);
					p[nthreads].tid = nthreads;
					p[nthreads].core_id = j;
					p[nthreads].device_handle = arr_handle[k];
					p[nthreads].t_start = &thread_start[nthreads];
					p[nthreads].t_end = &thread_end[nthreads];
					nthreads++;
				}
				fprintf(stderr,"]\n");
			}
		}
	}
	fprintf(stdout, "%s value_size=%d insert_count=%d\n", __FUNCTION__, value_size, insert_count);

	//Prepare App Memory
	fprintf(stdout, "Setup App: ");
	gettimeofday(&start, NULL);
	prepare_app_memory(nthreads);
	gettimeofday(&end, NULL);
	fprintf(stdout, "Done\n");
	show_elapsed_time(&start, &end, "Setup App", insert_count, 0, NULL);


	//Run Write Thread
	for (i = 0; i < nthreads; i++) {
		fail_unless(0 <= pthread_create(&t[i], NULL, sdk_write, &p[i]));
	}

	for (i = 0; i < nthreads; i++) {
		pthread_join(t[i], (void**) &status[i]);
	}
	show_elapsed_time_cumulative(thread_start, thread_end, nthreads, "kv_store_async", 0, 0, NULL);

	//check if iterator is already opened, and close it if so.
	int nr_iterate_handle = KV_MAX_ITERATE_HANDLE;
	kv_iterate_handle_info info[KV_MAX_ITERATE_HANDLE];
	ret = kv_iterate_info(arr_handle[0], info, nr_iterate_handle);
	if (ret == KV_SUCCESS) {
		fprintf(stderr,"iterate_handle count=%d\n", nr_iterate_handle);
		for (i = 0; i < nr_iterate_handle; i++) {
			fprintf(stderr,"iterate_handle_info[%d] : info.handle_id=%d info.status=%d info.type=%d info.prefix=%08x info.bitmask=%08x info.is_eof=%d\n",
					i + 1, info[i].handle_id, info[i].status, info[i].type, info[i].prefix, info[i].bitmask, info[i].is_eof);
			if (info[i].status == ITERATE_HANDLE_OPENED) {
				fprintf(stderr,"close iterate_handle : %d\n", info[i].handle_id);
				kv_iterate_close(arr_handle[0], info[i].handle_id);
			}
		}
	}

	//Run Iterate Thread
	signal(SIGALRM, myalarm);
	alarm(3);
	for (i = 0; i < nthreads; i++) {
		fail_unless(0 <= pthread_create(&t[i], NULL, sdk_iterate, &p[i]));
	}

	for (i = 0; i < nthreads; i++) {
		pthread_join(t[i], (void**) &status[i]);
	}
	show_elapsed_time_cumulative(thread_start, thread_end, nthreads, "kv_iterate_read_async", 0, 0, NULL);

	FILE* fp = fopen("result_async.txt", "a");
	for (i = 0; i < nthreads; i++) {
		fprintf(fp, "% 12d   % 11ld    ", g_num_read_key[i], g_total_time[i]/1000);
	}
	fprintf(fp, "\n");
	fclose(fp);
finalize:
	//Teardown Memory
	fprintf(stdout, "Teardown Memory: ");
	gettimeofday(&start, NULL);
	teardown_memory(nthreads);
	gettimeofday(&end, NULL);
	fprintf(stdout, "Done\n");
	show_elapsed_time(&start, &end, "Teardown Memory", insert_count, 0, NULL);

	//Finalize SDK
	gettimeofday(&start, NULL);
	kv_sdk_finalize();
	gettimeofday(&end, NULL);
	show_elapsed_time(&start, &end, "kv_sdk_finalize", 0, 0, NULL);

	return 0;
}

int main(int argc, char** argv) {
	if (argc == 1) {
		fprintf(stderr, "FAIL: %s need at least two argument\n", __FILE__ );
		exit(1);
	}
	if (argc > 1) {
		start_point = atoi(argv[1]);
	}
	if (argc > 2) {
		g_nr_it_handle = atoi(argv[2]);
		if(g_nr_it_handle < 1 || g_nr_it_handle > 7) {
			fprintf(stderr, "FAIL: invalid it_handle range");
			exit(1);
		}
	}
	if (argc > 3) {
		insert_count = atoi(argv[3]);
	}
	int ret = sdk_iterate_async();
	return ret;
}


