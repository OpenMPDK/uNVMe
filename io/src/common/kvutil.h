#ifndef _UTIL_H_
#define _UTIL_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <inttypes.h>
#include <syslog.h>
#include <unistd.h>
#include <sys/time.h>
#include <time.h>

#include "EagleHashIP.h"
#include "kv_types.h"
#include "latency_stat.h"

#define SQUARE(d)           ((d) * (d))
#define VAR(s, s2, n)       \
    (((n) < 2) ? 0.0 : ((s2) - SQUARE(s) / (n)) / ((n) - 1))
#define STDDEV(s, s2, n)    (((n) < 2) ? 0.0 : sqrt(VAR((s), (s2), (n))))
#define MIN(a, b)   ((a) < (b) ? (a) : (b))
#define MAX(a, b)   ((a) > (b) ? (a) : (b))
#define NELEM(a)    ((sizeof(a)) / sizeof((a)[0]))

#define KV_MEM_ALIGN(d, n)      ((size_t)(((d) + (n - 1)) & ~(n - 1)))
#define KV_PTR_ALIGN(p, n)  \
    (void *) (((uintptr_t) (p) + ((uintptr_t) n - 1)) & ~((uintptr_t) n - 1))


#ifndef KB
#define KB	(1024)
#endif

#ifndef MB
#define MB	(1024 * KB)
#endif

#ifndef GB
#define GB	(1024 * MB)
#endif

enum hash_{
        NONE=0,
        EAGLE_HASH128_1_P128,
        EAGLE_HASH128_2_P128,
        MURMUR_HASH3_x64_128
};

/*
	DB_BENCH_KEY= Generate db_bench-style Key (increase upper 8B, fill '\0' to lower 8B : 2017-04-20
	DEBUG_KEY = SIM 내 16B key 에 대해 상위 4B의 하위 28b만을 key로 다룸 : 2017-04-07
	READ_RANDOM_NUMBER = 순차 증가하는 키를 SEQ WRITE 이후, RANDOM READ
*/
enum key_generate_policy{
	PREFIX_SEQ_INC=0,
	POSTFIX_SEQ_INC=1,
	RANDOM=2,
	DEBUG_KEY=3, 	
        DB_BENCH_KEY=4, 
        READ_RANDOM_NUMBER=5, 
};

typedef struct key_policy{
	int idx;
	char name[128];
}key_policy;

extern key_policy g_key_policy[16];

void dump_key_value(char* key, int key_len, char* value, int value_len, bool show_value);
int gen_debug_key(char* key_buf, int value_size, int idx);
int gen_rand_key(char* key_buf, int len);
int gen_db_bench_key(char* start, int key_size_, uint64_t num);
int gen_rand_num(int max);
int get_power_of(int n);

void set_hash_func(int hash_no);
void (*hash_func)(const void* key, int len, unsigned int seed, void* out);
void show_key(kv_key* origin_key, kv_key* hashed_key);

void show_elapsed_time(struct timeval* start, struct timeval* end, char* msg, int repeat_count, uint64_t value_size, struct latency_stat *stat);
void show_elapsed_time_cumulative(struct timeval* start, struct timeval* end, int num_timeval, char* msg, int repeat_count_each, uint64_t value_size, struct latency_stat *stat);

#endif
