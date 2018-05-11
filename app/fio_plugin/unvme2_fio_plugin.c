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

#include <ctype.h>
#include <regex.h>
#include "kvnvme.h"
#include "kv_apis.h"
#include "kv_types.h"

#include "config-host.h"
#include "fio.h"
#include "optgroup.h"

#define KEY_LENGTH (16)
#define REDUNDANT_MEM_SIZE (24*1024*1024)
#define MAX_NUM_TOTAL_THREAD (128)
#define MEM_ALIGN(d, n) ((size_t)(((d) + (n - 1)) & ~(n - 1)))
#define MIN(a,b) ((a)<(b)?(a):(b))
#define MAX(a,b) ((a)>(b)?(a):(b))
#define CHECK_FIO_VERSION(a,b) (((a)*100) + (b))

struct kv_fio_request {
	struct io_u             *io;
	struct kv_fio_thread    *fio_thread;
	kv_pair                 kv;
	uint8_t                 key[KEY_LENGTH + 1];
};

static int td_count;
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
static kv_sdk g_sdk_opt;

struct thread_info {
	uint64_t	handle;
	int		core_id;
	uint32_t	sector_size;
};
static uint32_t g_numjobs_possible;
struct thread_info *g_threads_info;

struct kv_fio_thread {
	struct thread_data      *td;

	struct io_u             **iocq;			// io completion queue
	pthread_mutex_t         *iocq_count_mutex;      // mutex for iocq_count
	unsigned int            iocq_count;		// number of iocq entries filled by last getevents
	unsigned int            iocq_size;		// number of iocq entries allocated
	unsigned int            iocq_idx;		// idx of iocq entries filled from last getevents
	unsigned int            event_idx;		// event idx used when event() is called
	unsigned int            ssd_type;		// KV or LBA
	int                     core_id;		// core id for io threads
	int			fio_q_finished;		// return values when kv_fio_queue finished
	uint32_t		sector_size;		// device sector size used for calculate LBA addr
	uint64_t                device_handle;		// device handle used for IO
	char                    nvme_pci_dev[13];	// BDF of nvme dev
	struct fio_file         *current_f;		// fio_file given by user
};

int (*kv_fio_write) (uint64_t handle, const kv_pair *kv);
int (*kv_fio_read) (uint64_t handle, kv_pair *kv);

static int is_async(struct thread_data *td)
{
	return ((g_sdk_opt.ssd_type == LBA_TYPE_SSD) || (!td->o.sync_io && td->o.iodepth != 1));
}

static int is_sync(struct thread_data *td)
{
	return !is_async(td);
}

static void set_kv_io_function(struct thread_data *td)
{
	if (is_async(td)) {
		kv_fio_write = kv_nvme_write_async;
		kv_fio_read = kv_nvme_read_async;
	} else {
		kv_fio_write = kv_nvme_write;
		kv_fio_read = kv_nvme_read;
	}
}

static void remove_white_space(char *arg)
{
	char *tmp_ptr = arg;
	while((*(arg+=!isspace(*tmp_ptr++)) = *tmp_ptr));
}

static int kv_fio_parse(char *arg)
{
	int ret = -EINVAL;
	regex_t regex;

	remove_white_space(arg);
	
	ret = regcomp(&regex, "[0-9][0-9][0-9][0-9].[0-9][0-9].[0-9][0-9].[0-9]", 0);
	if (ret) {
		fprintf(stderr, "Could not compile regex\n");
		return ret;
	}

	ret = regexec(&regex, arg, 0, NULL, 0);
	regfree(&regex);

	if (!ret) {
		ret = KV_SUCCESS;
	}

	return ret;
}

static void kv_fio_option_check(struct thread_data *td)
{
	if (!td->o.use_thread) {
		log_err("must set thread=1 when using spdk plugin\n");
		exit(1);
	}
	if (td->files_size != 1) {
		log_err("filename must have only one argument\n");
		exit(1);
	}
}

static int kv_fio_parse_filename(struct thread_data *td)
{
	int ret = -EINVAL;
	regex_t regex;

	ret = regcomp(&regex, "[a-z,A-Z,0-9,_,-]*.json", 0);
	if (ret) {
		fprintf(stderr, "Could not compile regex\n");
	}

	ret = regexec(&regex, td->files[0]->file_name, 0, NULL, 0);
	regfree(&regex);

	if (!ret) { // json config file
		ret = kv_sdk_load_option(&g_sdk_opt, td->files[0]->file_name);  // load information from config.json to sdk_opt
		if (!ret) {
			for(int i = 0; i < g_sdk_opt.nr_ssd; i++){
				g_sdk_opt.dd_options[i].queue_depth = td->o.iodepth;
			}
		}
	} else { // not json file; try to parse
		g_sdk_opt.nr_ssd = 1;
		g_sdk_opt.ssd_type = LBA_TYPE_SSD;
		g_sdk_opt.dd_options[0].core_mask = 0x1;
		g_sdk_opt.dd_options[0].num_cq_threads = 1;
		g_sdk_opt.dd_options[0].cq_thread_mask = 0x2;
		g_sdk_opt.dd_options[0].queue_depth = td->o.iodepth;

		ret = kv_fio_parse(td->files[0]->file_name);
		if (!ret) {
			memcpy(g_sdk_opt.dev_id[0], td->files[0]->file_name, 12);
			g_sdk_opt.dev_id[0][4] = g_sdk_opt.dev_id[0][7] = g_sdk_opt.dev_id[0][10] = ':';
		} else {
			log_err("filename is neither json configuration nor BDF address\n");
			ret = -EINVAL;
		}
	}

	if (!ret) {
		if (is_sync(td)) {
			for(int i = 0; i < g_sdk_opt.nr_ssd; i++){
				g_sdk_opt.dd_options[i].sync_mask = g_sdk_opt.dd_options[i].core_mask;
			}
		}
	}

	return ret;
}

static uint32_t kv_fio_parse_device_descriptions(struct thread_info *tinfo)
{
        uint64_t core_mask[NR_MAX_SSD] = {0, };
        uint64_t core_id[NR_MAX_SSD] = {0, };
        int dev_done = 0;
        uint32_t numjobs_possible = 0;
	uint32_t sector_size = 0;

        for (int i = 0; i < g_sdk_opt.nr_ssd; i++) {
                core_mask[i] = g_sdk_opt.dd_options[i].core_mask;
                core_id[i] = 0;
        }

        while (dev_done < g_sdk_opt.nr_ssd) {
                for(int i = 0; i < g_sdk_opt.nr_ssd; i++) {
                        if (core_mask[i] == 0) continue;
                        while (core_mask[i]) {
                                if (core_mask[i] % 2) {
                                        tinfo[numjobs_possible].handle = g_sdk_opt.dev_handle[i];
					sector_size = kv_get_sector_size(g_sdk_opt.dev_handle[i]);
					assert(sector_size != 0 && sector_size != KV_ERR_SDK_INVALID_PARAM);
					tinfo[numjobs_possible].sector_size = sector_size;
                                        tinfo[numjobs_possible].core_id = core_id[i]++;
                                        numjobs_possible++;
                                        if(numjobs_possible > MAX_NUM_TOTAL_THREAD) {
                                                log_err("Number of total threads(jobs) is more than MAX_NUM_TOTAL_THREAD(%d)\n", MAX_NUM_TOTAL_THREAD);
                                                exit(1);
                                        }
                                        core_mask[i] >>= 1;
                                        break;
                                }
                                core_id[i]++;
                                core_mask[i] >>= 1;
                        }
                        if (core_mask[i] == 0) {
                                dev_done++;
                        }
                }
        }

	return numjobs_possible;
}

static size_t kv_fio_calc_hugemem_size(struct thread_data *td)
{
	size_t max_block_size, total_io_block_size;

	max_block_size = MAX(MAX(td->o.max_bs[DDIR_WRITE], td->o.max_bs[DDIR_READ]), td->o.max_bs[DDIR_TRIM]);
	total_io_block_size = (max_block_size * td->o.iodepth * td->o.numjobs);
	return total_io_block_size + REDUNDANT_MEM_SIZE;
}

static int kv_fio_setup(struct thread_data *td)
{
	int ret;

	struct kv_fio_thread *fio_thread;
	struct fio_file *f;
	unsigned int i;

	pthread_mutex_lock(&mutex);

	kv_fio_option_check(td);

	if (!kv_nvme_is_dd_initialized()) { // init once
		fprintf(stderr, "unvme2_fio_plugin is built with fio version=%d.%d\n",FIO_MAJOR_VERSION, FIO_MINOR_VERSION);
		kv_sdk_info();

		ret = kv_fio_parse_filename(td);
		assert(ret == KV_SUCCESS);

		g_sdk_opt.app_hugemem_size = kv_fio_calc_hugemem_size(td);
		ret = kv_sdk_init(KV_SDK_INIT_FROM_STR, &g_sdk_opt);
		assert(ret == KV_SUCCESS);

		// set core_id, dev handle for all threads
		g_threads_info = (struct thread_info*) calloc(MAX_NUM_TOTAL_THREAD, sizeof(struct thread_info));

		g_numjobs_possible = kv_fio_parse_device_descriptions(g_threads_info);

		// WARNING: USER MAY GIVE ACCURATE 'numjobs' value
		if (td->o.numjobs != g_numjobs_possible) {
			fprintf(stderr, "WARN: numjobs is differ from the expected(numjobs=%u, expected(possible) jobs=%u)\n", td->o.numjobs, g_numjobs_possible);
		}

		set_kv_io_function(td);

	}

	// set thread data
	fio_thread = calloc(1, sizeof(*fio_thread));
	assert(fio_thread != NULL);

#if (CHECK_FIO_VERSION(FIO_MAJOR_VERSION, FIO_MINOR_VERSION) >= CHECK_FIO_VERSION(2,14))
	td->io_ops_data = fio_thread; // IO engine private data(void *)
#else
	td->io_ops->data = fio_thread;
#endif
	fio_thread->td = td;

	if (is_sync(td)) {
		// set return type of queue ops and engine's flag for sync IO
		fio_thread->fio_q_finished = FIO_Q_COMPLETED;
		td->io_ops->flags |= FIO_SYNCIO;
#if (CHECK_FIO_VERSION(FIO_MAJOR_VERSION, FIO_MINOR_VERSION) >= CHECK_FIO_VERSION(2,14))
		td_set_ioengine_flags(td);
#endif
	} else {
		fio_thread->fio_q_finished = FIO_Q_QUEUED;
		td->o.iodepth++;

		fio_thread->iocq_size = td->o.iodepth;
		fio_thread->iocq = calloc(fio_thread->iocq_size, sizeof(struct iio_u *));
		assert(fio_thread->iocq != NULL);

		fio_thread->iocq_count_mutex = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t));
		assert(fio_thread->iocq_count_mutex);
		pthread_mutex_init(fio_thread->iocq_count_mutex, NULL);

		fio_thread->iocq_count = 0;
		fio_thread->iocq_idx = 0;
		fio_thread->event_idx = 0;
	}

	fio_thread->device_handle = g_threads_info[td_count % g_numjobs_possible].handle;
	fio_thread->core_id = g_threads_info[td_count % g_numjobs_possible].core_id;
	fio_thread->sector_size = g_threads_info[td_count % g_numjobs_possible].sector_size;
	fio_thread->ssd_type = g_sdk_opt.ssd_type; // this affects key generation

	for_each_file(td, f, i) {
		fio_thread->current_f = f;
		f->real_file_size = kv_nvme_get_total_size(fio_thread->device_handle);
		assert(f->real_file_size);
			
#if (CHECK_FIO_VERSION(FIO_MAJOR_VERSION, FIO_MINOR_VERSION) >= CHECK_FIO_VERSION(2,15))
		f->filetype = FIO_TYPE_BLOCK;
#else
		f->filetype = FIO_TYPE_BD;
#endif

		fio_file_set_size_known(f);
	}

	td_count++;

	pthread_mutex_unlock(&mutex);

	return 0;
}

static int kv_fio_open(struct thread_data *td, struct fio_file *f)
{
#if (CHECK_FIO_VERSION(FIO_MAJOR_VERSION, FIO_MINOR_VERSION) >= CHECK_FIO_VERSION(2,14))
        struct kv_fio_thread *fio_thread = td->io_ops_data;
#else
        struct kv_fio_thread *fio_thread = td->io_ops->data;
#endif
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(fio_thread->core_id, &cpuset);
        sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

	return 0;
}

static int kv_fio_close(struct thread_data *td, struct fio_file *f)
{
	return 0;
}

static int kv_fio_iomem_alloc(struct thread_data *td, size_t total_mem)
{
	size_t aligned_total_mem = MEM_ALIGN(total_mem, 4*1024); // 4KB aligned size
	td->orig_buffer = kv_zalloc(aligned_total_mem);
	return td->orig_buffer == NULL;
}

static void kv_fio_iomem_free(struct thread_data *td)
{
	kv_free(td->orig_buffer);
}

static int kv_fio_io_u_init(struct thread_data *td, struct io_u *io_u)
{
#if (CHECK_FIO_VERSION(FIO_MAJOR_VERSION, FIO_MINOR_VERSION) >= CHECK_FIO_VERSION(2,14))
	struct kv_fio_thread	*fio_thread = td->io_ops_data;
#else
	struct kv_fio_thread	*fio_thread = td->io_ops->data;
#endif
	struct kv_fio_request	*fio_req;

	fio_req = calloc(1, sizeof(*fio_req));
	if (fio_req == NULL) {
		return 1;
	}
	fio_req->io = io_u;
	fio_req->fio_thread = fio_thread;

	io_u->engine_data = fio_req;

	return 0;
}

static void kv_fio_io_u_free(struct thread_data *td, struct io_u *io_u)
{
	struct kv_fio_request *fio_req = io_u->engine_data;

	if (fio_req) {
		assert(fio_req->io == io_u);
		free(fio_req);
		io_u->engine_data = NULL;
	}
}

static void kv_fio_completion_cb(kv_pair *kv, unsigned int result, unsigned int status)
{
	assert(status == KV_SUCCESS);
	assert(kv != NULL);

	struct kv_fio_request		*fio_req = kv->param.private_data;
	struct kv_fio_thread		*fio_thread = fio_req->fio_thread;

	assert(fio_thread->iocq_count < fio_thread->iocq_size);

	fio_thread->iocq[fio_thread->iocq_idx] = fio_req->io;
	fio_thread->iocq_idx = (fio_thread->iocq_idx + 1) % fio_thread->iocq_size;

	pthread_mutex_lock(fio_thread->iocq_count_mutex);
	fio_thread->iocq_count++;
	pthread_mutex_unlock(fio_thread->iocq_count_mutex);
}

static int kv_fio_queue(struct thread_data *td, struct io_u *io_u)
{
	int ret = -EINVAL;

#if (CHECK_FIO_VERSION(FIO_MAJOR_VERSION, FIO_MINOR_VERSION) >= CHECK_FIO_VERSION(2,14))
	struct kv_fio_thread	*fio_thread = td->io_ops_data;
#else
	struct kv_fio_thread	*fio_thread = td->io_ops->data;
#endif
	struct kv_fio_request	*fio_req = io_u->engine_data;

	uint64_t handle = fio_thread->device_handle;
	uint32_t sector_size = fio_thread->sector_size;

	if(fio_thread->ssd_type == LBA_TYPE_SSD) {
		*(uint64_t*)(fio_req->key) = io_u->offset / sector_size; //lba addr
	} else { // KV_TYPE_SSD
		snprintf((char*)fio_req->key, KEY_LENGTH+1, "%016llu", io_u->offset / io_u->xfer_buflen);
	}

	kv_pair* kv = &fio_req->kv;
	kv->key.key = fio_req->key;
	kv->key.length = KEY_LENGTH;

	kv->value.value = io_u->buf;
	kv->value.length = io_u->xfer_buflen;
	kv->value.offset = 0;

	if (is_async(td)) {
		kv->param.async_cb = kv_fio_completion_cb;
		kv->param.private_data = fio_req;
	}

	switch (io_u->ddir) {
	case DDIR_READ:
		kv->param.io_option.retrieve_option = KV_RETRIEVE_DEFAULT;
		ret = -EINVAL;
		while(ret) {
			ret = kv_fio_read(handle, kv);
		}
		break;
	case DDIR_WRITE:
		kv->param.io_option.store_option = KV_STORE_DEFAULT;
		ret = -EINVAL;
		while(ret) {
			ret = kv_fio_write(handle, kv);
		}
		break;
	default: // NOT support DDIR_TRIM, DDIR_SYNC, DDIR_DATASYNC
		break;
	}

	return (ret)? (FIO_Q_COMPLETED) : (fio_thread->fio_q_finished);
        // FIO_Q_COMPLETED = 0, /* completed sync */
        // FIO_Q_QUEUED    = 1, /* queued, will complete async */
        // FIO_Q_BUSY      = 2, /* no more room, call ->commit() */
}

static struct io_u *kv_fio_event(struct thread_data *td, int event)
{
#if (CHECK_FIO_VERSION(FIO_MAJOR_VERSION, FIO_MINOR_VERSION) >= CHECK_FIO_VERSION(2,14))
	struct kv_fio_thread *fio_thread = td->io_ops_data;
#else
	struct kv_fio_thread *fio_thread = td->io_ops->data;
#endif
	unsigned int idx;

	assert(event >= 0);
	idx = fio_thread->event_idx;
	assert(fio_thread->iocq[idx] != NULL);

	fio_thread->event_idx = (fio_thread->event_idx + 1) % fio_thread->iocq_size;

	return fio_thread->iocq[idx];
}

static int kv_fio_getevents(struct thread_data *td, unsigned int min,
			      unsigned int max, const struct timespec *t)
{
#if (CHECK_FIO_VERSION(FIO_MAJOR_VERSION, FIO_MINOR_VERSION) >= CHECK_FIO_VERSION(2,14))
	struct kv_fio_thread *fio_thread = td->io_ops_data;
#else
	struct kv_fio_thread *fio_thread = td->io_ops->data;
#endif
	struct timespec t0, t1;
	int iocq_count = 0;
	uint64_t timeout = 0;

	if (t) {
		timeout = t->tv_sec * 1000000000L + t->tv_nsec;
		clock_gettime(CLOCK_MONOTONIC_RAW, &t0);
	}
	assert(min <= max);

	for (;;) {
		pthread_mutex_lock(fio_thread->iocq_count_mutex);
		if (fio_thread->iocq_count >= min) {
			iocq_count = MIN(fio_thread->iocq_count, max);
			fio_thread->iocq_count -= iocq_count;
			pthread_mutex_unlock(fio_thread->iocq_count_mutex);

			return iocq_count;
		}
		pthread_mutex_unlock(fio_thread->iocq_count_mutex);

		if (t) {
			clock_gettime(CLOCK_MONOTONIC_RAW, &t1);
			uint64_t elapse = ((t1.tv_sec - t0.tv_sec) * 1000000000L)
					  + t1.tv_nsec - t0.tv_nsec;
			if (elapse > timeout) {
				break;
			}
		}
	}

	pthread_mutex_lock(fio_thread->iocq_count_mutex);
	iocq_count = fio_thread->iocq_count;
	fio_thread->iocq_count = 0;
	pthread_mutex_unlock(fio_thread->iocq_count_mutex);

	return iocq_count;
}

static int kv_fio_invalidate(struct thread_data *td, struct fio_file *f)
{
	//TODO: This should probably send a flush to the device, but for now just return successful. */
	return 0;
}

static void kv_fio_cleanup(struct thread_data *td)
{
#if (CHECK_FIO_VERSION(FIO_MAJOR_VERSION, FIO_MINOR_VERSION) >= CHECK_FIO_VERSION(2,14))
	struct kv_fio_thread	*fio_thread = td->io_ops_data;
#else
	struct kv_fio_thread	*fio_thread = td->io_ops->data;
#endif
	if (is_async(td)) {
		free(fio_thread->iocq_count_mutex);
		td->o.iodepth--;
		td->latency_qd--;
	}
	free(fio_thread);
	pthread_mutex_lock(&mutex);
	td_count--;
	if (td_count == 0) {
		if(g_threads_info) {
			free(g_threads_info);
		}
		kv_sdk_finalize();
	}
	pthread_mutex_unlock(&mutex);
}

/* FIO imports this structure using dlsym */
struct ioengine_ops ioengine = {
	.name			= "unvme2_fio",
	.version		= FIO_IOOPS_VERSION,
	.queue			= kv_fio_queue,		//do io
	.getevents		= kv_fio_getevents,	//(async) called after queueing to check the IO reqs are completed
	.event			= kv_fio_event,		//(async) called after getevents, to collect io_req resources
	.cleanup		= kv_fio_cleanup,	//finalize
	.open_file		= kv_fio_open,		//do nothing
	.close_file		= kv_fio_close,		//do nothing
	.invalidate		= kv_fio_invalidate,	//flush, but do nothing for now
	.iomem_alloc		= kv_fio_iomem_alloc,	//alloc resources for IO req
	.iomem_free		= kv_fio_iomem_free,	//free resources for IO req
	.setup			= kv_fio_setup,		//initialize
	.io_u_init		= kv_fio_io_u_init,	//alloc resources for kv_fio req (called n times while n = iodepth)
	.io_u_free		= kv_fio_io_u_free,	//free resources for kv_fio req
	.flags			= FIO_RAWIO | FIO_NOEXTEND | FIO_NODISKUTIL | FIO_MEMALIGN,
};
