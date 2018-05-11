#ifndef __LATENCY_STAT_H__
#define __LATENCY_STAT_H__

#include <sys/time.h>

#define BUCKET_SIZE (790u)

struct time_stamp {
        struct timeval start;
        struct timeval end;
};

struct latency_stat {
  unsigned long long max;
  unsigned long long min;
  unsigned long long samples;
  unsigned long long sum;
  unsigned int count_for_bucket[BUCKET_SIZE];
  double mean;
  double variance;
};

void reset_latency_stat(struct latency_stat *);
void add_latency_stat(struct latency_stat *, const struct timeval *, const struct timeval *);
void print_latency_stat(struct latency_stat *);

#endif
