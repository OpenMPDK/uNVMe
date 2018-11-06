#include "kvutil.h"

key_policy g_key_policy[16] = {
	{PREFIX_SEQ_INC, "PREFIX_SEQ_INC"},
	{POSTFIX_SEQ_INC, "POSTFIX_SEQ_INC"},
	{RANDOM, "RANDOM"},
	{DEBUG_KEY, "DEBUG_KEY"},
	{DB_BENCH_KEY, "DB_BENCH_KEY"},
	{READ_RANDOM_NUMBER, "READ_RANDOM_NUMBER"},
};

static int rand_init = 0;
//static void (*hash_arr[4])(const void* key, int len, unsigned int seed, void* out) = {NULL, Hash128_1_P128, Hash128_2_P128, MurmurHash3_x64_128};
static void (*hash_arr[4])(const void* key, int len, unsigned int seed, void* out) = {NULL, Hash128_1_P128, Hash128_2_P128, Hash128_2_P128};

void show_key(kv_key* origin_key, kv_key* hashed_key){
        char k[17] = {0,};
        memcpy(k, origin_key->key, 16);
        printf("in = %s\n", k);
        printf("out= %llx ", ((unsigned long long*)hashed_key->key)[0]);
        printf("%llx\n", ((unsigned long long*)hashed_key->key)[1]);
}

void show_elapsed_time(struct timeval* start, struct timeval* end, char* msg, int repeat_count, uint64_t value_size, struct latency_stat *stat){
        long secs_used;
        long micros_used;
        secs_used=(end->tv_sec - start->tv_sec); //avoid overflow by subtracting first
        micros_used= ((secs_used*1000000) + end->tv_usec) - (start->tv_usec);

        printf("======================================================\n");
        printf("%s start: %lds.%ldus end: %lds.%ldus\n", (msg) ? msg : "", start->tv_sec, start->tv_usec, end->tv_sec, end->tv_usec);
        printf("%s total elapsed: %ldus %.3fms\n", (msg) ? msg : "", micros_used, ((float)micros_used) / 1000);
        if(repeat_count) {
                float latency = (float)micros_used / repeat_count;
                printf("%s latency: %.3f us per operation\n", (msg) ? msg : "", latency);
                float ops = 1000000 / latency;
                printf("%s ops: %.3f\n", (msg) ? msg : "", ops);
                if(value_size > 0){
                        uint64_t throughput = ops * value_size / KB;
                        printf("%s throughput: %ld KB\n", (msg) ? msg : "", throughput);
                }
                if (stat) {
                        printf("%s latency QoS: \n", (msg) ? msg : "");
                        print_latency_stat(stat);
                }
        }
        printf("======================================================\n\n\n");
}

void show_elapsed_time_cumulative(struct timeval* start, struct timeval* end, int num_timeval, char* msg, int repeat_count_each, uint64_t value_size, struct latency_stat *stat){
	float total_latency = 0.f;
        float total_avg_latency = 0.f;
        float total_ops = 0.f;

        for(int i=0; i<num_timeval; i++){
                long secs_used=(end[i].tv_sec - start[i].tv_sec); //avoid overflow by subtracting first
                long micros_used= ((secs_used*1000000) + end[i].tv_usec) - (start[i].tv_usec);
                float latency = (float)micros_used / repeat_count_each;
                float ops = 1000000 / latency;

		total_latency += latency;
                total_ops += ops;
        }
	//total_avg_latency = total_latency / num_timeval;
        total_avg_latency = 1000000 / total_ops;

        printf("======================================================\n");
        printf("%s latency: %.3f us per operation\n", (msg) ? msg : "", total_avg_latency);
        printf("%s ops: %.3f\n", (msg) ? msg : "", total_ops);
        if(value_size > 0){
                uint64_t total_throughput = total_ops * value_size / KB;
                printf("%s throughput: %ld KB\n", (msg) ? msg : "", total_throughput);
        }
        if (stat) {
                printf("%s latency QoS: \n", (msg) ? msg : "");
                print_latency_stat(stat);
        }
        printf("======================================================\n\n\n");
}

void set_hash_func(int hash_no){
        hash_func = hash_arr[hash_no];
        printf("hash_func: ");
        switch(hash_no){
                case NONE: printf("none\n"); break;
                case EAGLE_HASH128_1_P128: printf("Ealge_1\n"); break;
                case EAGLE_HASH128_2_P128: printf("Eagle_2\n"); break;
                case MURMUR_HASH3_x64_128: printf("Murmur3\n"); break;
        }
}

kv_key* do_hash(int hash_no, kv_key* origin_key, kv_key* hashed_key){
        if (!hash_no) return origin_key;
	unsigned long long seed = 0x0102030405060708LL;
        hash_func(origin_key->key, origin_key->length, seed, hashed_key->key);
        return hashed_key;
}

static void init_rand(){
	srand(time(NULL));
	rand_init=1;
}

int gen_rand_key(char* key_buf, int len){
	if(!key_buf || len<=0)
		return -1;

	if(rand_init == 0){
		init_rand();
	}

	int i;
	for(i=0;i<len;i++){
		key_buf[i] = (rand()%26+'a');
	}
	return 0;
}

int gen_rand_num(int max){
	return rand()%max;
}

int get_power_of(int n){
	if(n<=1)
		return 0;
	int power = 0;
	do{
		power++;
		n>>=1;
	}while(n>1);
	return power;
}

int gen_debug_key(char* key_buf, int value_size, int idx){
	if(!key_buf || value_size<=0)
		return -1;

	int32_t key = ((get_power_of(value_size))<<24)|(idx&0x00FFFFFF);
	memcpy(key_buf,(char*)&key,4);
	sprintf(key_buf+4,"%06d%06d",value_size,idx);
	return 0;
}

int gen_db_bench_key(char* start, int key_size_, uint64_t num) {
	if(!start || key_size_<=0)
		return -1;

	char* pos = start;
	int i;
	int bytes_to_fill = MIN(key_size_, 8);
	for (i = 0; i <bytes_to_fill; i++) {
		pos[i] = (num >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
	}
	pos += bytes_to_fill;
	if (key_size_ > pos - start) {
		memset(pos, '0', key_size_ - (pos - start));
	}
	*(volatile char*)pos = *(volatile char*)pos;

	//for check key pattern
	/*
	   for(i = 0; i<key_size_;i++){
	   printf("%02x ", (unsigned char)start[i]);
	   }
	   printf("\n");
	 */
	return 0;
}

void dump_key_value(char* key, int key_len, char* value, int value_len, bool show_value){
	if(!key || !value)
		return;

	int i;
	printf("|");
	for(i=0;i<key_len;i++){
		if(key[i])
			printf("%c",key[i]);
	}
	printf("| key length=%d\n",key_len);

	if(show_value){
		int linecnt=0;
		int col=32;

		printf("    ");
		for(i=0;i<col;i++){
			printf("%3d",i);
		}
		printf("\n");

		printf("    ");
		for(i=0;i<col;i++){
			printf("---");
		}
		printf("\n");

		int space_skip_cnt=0;
		for(i=0;i<value_len;i++){
			if(value[i] == 0){
				//Note : as currently, there is no way to know actual read,
				//       i intented to skip traverse all buffer to display.
				//      so, count zero and skip if it prolong longer than 1K
				space_skip_cnt++;
				if(space_skip_cnt > 1024){
					break;
				}
				continue;
			}
			if(!(i%col)){
				if(i) printf("\n");
				printf("%03d:",++linecnt);
			}
			printf("%3c",value[i]);
			space_skip_cnt=0;
		}
		printf("\n");
		printf("value length=%d\n",value_len);
	}
	else{
		printf("value length=%d\n",value_len);
	}
}
