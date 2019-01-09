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

#define DEFAULT_KEY_SIZE ("16")
#define REDUNDANT_MEM_SIZE (256ULL*1024*1024)
#define MEM_ALIGN(d, n) ((size_t)(((d) + (n - 1)) & ~(n - 1)))
#define PTR_ALIGN(ptr, mask)    \
        (char *) (((uintptr_t) (ptr) + (mask)) & ~(mask))
#define MIN(a,b) ((a)<(b)?(a):(b))
#define MAX(a,b) ((a)>(b)?(a):(b))
#define CHECK_FIO_VERSION(a,b) (((a)*100) + (b))
#define ZERO_VALUE_MAGICNUM (777)


struct kv_fio_request {
	struct io_u             *io;
	struct kv_fio_thread    *fio_thread;
	kv_pair                 kv;
	uint8_t                 *key;
	uint16_t		key_size;
};

struct kv_dev_info {
	struct fio_file		*f;
	uint64_t		dev_handle;
	uint64_t		core_mask;
	uint64_t		cq_thread_mask;
	uint32_t		sector_size;
	struct kv_dev_info	*next;
};

struct kv_fio_thread {
	struct thread_data      *td;
	struct io_u             **iocq;			// io completion queue
	pthread_mutex_t		*head_mutex;
	unsigned int            ssd_type;		// KV or LBA
	unsigned int            iocq_size;		// number of iocq entries allocated
	volatile unsigned int	iocq_head;
	volatile unsigned int	iocq_tail;
	int			core_id;		// core id for io threads
	int			fio_q_finished;		// return values when kv_fio_queue finished
	int			qid;			// qpair id for io
	uint16_t		key_size;		// (KV) key length
	struct kv_dev_info	*dev_info;		// devices' information of given io job(thread)
};

struct kv_fio_engine_options { //fio options
        void    	*pad;
        char    	*json_path;
        uint16_t    	key_size;
};

static struct fio_option options[] = {
        {
                .name   = "json_path",
                .lname  = "configuration file path",
                .type   = FIO_OPT_STR_STORE,
                .off1   = offsetof(struct kv_fio_engine_options, json_path),
                .help   = "JSON formmatted configuration file path",
                .category = FIO_OPT_C_ENGINE,
        },
        {
                .name   = "ks",
                .lname  = "key size for KV SSD",
                .alias	= "keysize",
                .type   = FIO_OPT_INT,
                .off1   = offsetof(struct kv_fio_engine_options, key_size),
                .def	= DEFAULT_KEY_SIZE,
                .help   = "Key size of KV pairs (valid only for KV SSD)",
                .category = FIO_OPT_C_ENGINE,
        },
        {
                .name   = NULL,
        },
};

static int td_count;
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
static kv_sdk g_sdk_opt = {0, };
static uintptr_t g_page_mask;

int (*kv_fio_write) (uint64_t handle, int qid, kv_pair *kv);
int (*kv_fio_read) (uint64_t handle, int qid, kv_pair *kv);

void increase_iocq_head(struct kv_fio_thread *f)
{
	if (++f->iocq_head > f->iocq_size) {
		f->iocq_head = 0;
	}
}

void increase_iocq_tail(struct kv_fio_thread *f)
{
	if (++f->iocq_tail > f->iocq_size) {
		f->iocq_tail = 0;
	}
}

unsigned int calculate_num_completed_io(struct kv_fio_thread *f)
{
	int ret = f->iocq_head - f->iocq_tail;
	return (ret >= 0) ? (unsigned int)ret : (unsigned int)(ret + (f->iocq_size + 1));
}

static int is_async(struct thread_data *td)
{
	return ((g_sdk_opt.ssd_type == LBA_TYPE_SSD) || (!td->o.sync_io && td->o.iodepth != 1));
}

static int is_sync(struct thread_data *td)
{
	return !is_async(td);
}

static void kv_fio_set_io_function(struct thread_data *td)
{
	if (is_async(td)) {
		kv_fio_write = kv_nvme_write_async;
		kv_fio_read = kv_nvme_read_async;
	} else {
		kv_fio_write = kv_nvme_write;
		kv_fio_read = kv_nvme_read;
	}
}

static void kv_fio_option_check(struct thread_data *td)
{
	if (!td->o.use_thread) {
		log_err("must set thread=1 when using spdk plugin\n");
		exit(1);
	}
}

static int kv_fio_parse_config_file(char *json_path, struct thread_data *td)
{
	int ret = -EINVAL;
	regex_t regex;

	ret = regcomp(&regex, "[a-z,A-Z,0-9,_,-]*.json", 0);
	if (ret) {
		fprintf(stderr, "Could not compile regex\n");
	}

	ret = regexec(&regex, json_path, 0, NULL, 0);
	regfree(&regex);

	if (!ret) { // json config file
		ret = kv_sdk_load_option(&g_sdk_opt, json_path);  // load information from config.json to sdk_opt
		if (!ret) {
			for(int i = 0; i < g_sdk_opt.nr_ssd; i++){
				g_sdk_opt.dd_options[i].queue_depth = td->o.iodepth;
			}
		}
	} else { // not json file
		fprintf(stderr, "Invalid json configuration file\n");
		ret = -EINVAL;
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

static int kv_fio_get_dev_info_on_bdf(char *bdf, struct kv_dev_info *dev_info)
{
	int ret = -1;
	char dev_id[DEV_ID_LEN] = {0, };
	char *c;
	for (int i = 0; i < g_sdk_opt.nr_ssd; i++) {
		memcpy(dev_id, g_sdk_opt.dev_id[i], DEV_ID_LEN);
		c = dev_id;
		do {
			if (*c == ':' ) *c = '.';
			c++;
		} while (*c != 0);
		if (strcmp(dev_id, bdf) == 0) {
			dev_info->dev_handle = g_sdk_opt.dev_handle[i];
			dev_info->core_mask = g_sdk_opt.dd_options[i].core_mask;
			dev_info->cq_thread_mask = g_sdk_opt.dd_options[i].cq_thread_mask;
			dev_info->sector_size = kv_get_sector_size(g_sdk_opt.dev_handle[i]);
			ret = 0;

			break;
		}
	}
	return ret;
}

static int kv_fio_get_core_and_qid_for_io_thread(struct kv_fio_thread *fio_thread, int *arr_qid, int *num_possible_qid)
{
	uint64_t core_mask = UINT64_MAX;
	int core_id = -1;
	struct kv_dev_info *info = fio_thread->dev_info;
	while (info != NULL) {
		core_mask &= info->core_mask;
		info = info->next;
	}

	*num_possible_qid = 0;

	for(int i = 0; i < MAX_CPU_CORES; i ++){
		if (core_mask & (1ULL << i)) {
			arr_qid[(*num_possible_qid)++] = i;
			if (core_id == -1) {
				core_id = i;
			}
		}
	}
	return core_id;
}

static unsigned int kv_fio_max_bs(struct thread_data *td)
{
	unsigned int max_bs;
	max_bs = max(td->o.max_bs[DDIR_READ], td->o.max_bs[DDIR_WRITE]);
	return max(td->o.max_bs[DDIR_TRIM], max_bs);
}

static uint64_t kv_fio_calc_hugemem_size(struct thread_data *td)
{
	uint64_t max_block_size, total_io_block_size;

	max_block_size = kv_fio_max_bs(td);
	total_io_block_size = (max_block_size * td->o.iodepth * td->o.numjobs);
	return total_io_block_size + REDUNDANT_MEM_SIZE; //redundant hugemem for key alloc, etc.
}

static int kv_fio_setup(struct thread_data *td)
{
	int ret;

	struct kv_fio_thread *fio_thread;
	struct fio_file *f;
	struct kv_fio_engine_options *engine_option = td->eo;
	unsigned int i;

	pthread_mutex_lock(&mutex);

	kv_fio_option_check(td);

	if (!kv_nvme_is_dd_initialized()) { // init once
		fprintf(stderr, "unvme2_fio_plugin is built with fio version=%d.%d\n",FIO_MAJOR_VERSION, FIO_MINOR_VERSION);
		kv_sdk_info();

		if (engine_option->json_path == NULL) {
			fprintf(stderr, "Need to specify the path of uNVMe configuration file to option 'json_path'\n");
			goto err;
		}

		if (engine_option->key_size < KV_MIN_KEY_LEN || engine_option->key_size > KV_MAX_KEY_LEN) {
			fprintf(stderr, "keysize(%u) should be between %u and %u.\n", \
					engine_option->key_size, KV_MIN_KEY_LEN, KV_MAX_KEY_LEN);
			goto err;
		}

		ret = kv_fio_parse_config_file(engine_option->json_path, td);
		if (ret != KV_SUCCESS) {
			fprintf(stderr, "Failed to parse configuration file. Please check the configuration is described properly\n");
			goto err;
		}

		g_sdk_opt.app_hugemem_size = kv_fio_calc_hugemem_size(td);
		ret = kv_sdk_init(KV_SDK_INIT_FROM_STR, &g_sdk_opt);
		if (ret != KV_SUCCESS) {
			fprintf(stderr, "Failed to initialize SDK(ret: 0x%x). Please check the configuration(%s)\n", ret, engine_option->json_path);
			goto err;
		}

		kv_fio_set_io_function(td);

		uintptr_t page_size = sysconf(_SC_PAGESIZE);
		assert(page_size > 0);
		g_page_mask = page_size - 1;
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

		fio_thread->iocq_size = td->o.iodepth;
		fio_thread->iocq = calloc(fio_thread->iocq_size+1, sizeof(struct iio_u *));
		assert(fio_thread->iocq != NULL);

		fio_thread->iocq_head = 0;
		fio_thread->iocq_tail = 0;
		fio_thread->head_mutex = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t));
		assert(fio_thread->head_mutex != NULL);
		pthread_mutex_init(fio_thread->head_mutex, NULL);
	}

	struct kv_dev_info **prev_dev_info = &fio_thread->dev_info;
	for_each_file(td, f, i) {
		struct kv_dev_info *dev_info = (struct kv_dev_info *)calloc(1, sizeof(struct kv_dev_info));
	        assert(dev_info != NULL);
		dev_info->f = f;
		dev_info->next = NULL;

		ret = kv_fio_get_dev_info_on_bdf(f->file_name, dev_info);
		if (ret != KV_SUCCESS) {
			fprintf(stderr, "Failed to find the device %s. Please check the option 'filename' is set properly\n", f->file_name);
			goto err;
		}

		f->real_file_size = kv_nvme_get_total_size(dev_info->dev_handle);
		if (f->real_file_size == KV_ERR_INVALID_VALUE) {
			fprintf(stderr, "Failed to get total size of the device %s\n", f->file_name);
			goto err;
		}
			
#if (CHECK_FIO_VERSION(FIO_MAJOR_VERSION, FIO_MINOR_VERSION) >= CHECK_FIO_VERSION(2,18))
		f->filetype = FIO_TYPE_BLOCK;
#else
		f->filetype = FIO_TYPE_BD;
#endif
		fio_file_set_size_known(f);

		*prev_dev_info = dev_info;
		prev_dev_info = &dev_info->next;
	}

	int arr_qid[MAX_CPU_CORES] = {0, };
	int num_possible_qid = 0;
	fio_thread->core_id = kv_fio_get_core_and_qid_for_io_thread(fio_thread, arr_qid, &num_possible_qid);
	if (fio_thread->core_id < 0) {
		fprintf(stderr, "This job can't generate IO workload due to the wrong configuration. \
				Please check the option 'core_mask' of the device(s)\n");
		goto err;
	}
	fio_thread->qid = arr_qid[td->subjob_number % num_possible_qid];
	//fprintf(stderr, "thread #%u: subjob #%u, core #%d, queue #%d\n", td->thread_number, td->subjob_number, fio_thread->core_id, fio_thread->qid);

	fio_thread->ssd_type = g_sdk_opt.ssd_type; // this affects key generation

	if (fio_thread->ssd_type == KV_TYPE_SSD) {
		fio_thread->key_size = engine_option->key_size;
	} else {
		fio_thread->key_size = atoi(DEFAULT_KEY_SIZE);
	}

	// for io job information
	struct kv_dev_info *tmpinfo = fio_thread->dev_info;
	fprintf(stderr, "job %u will generate IO to the devices below(at core %u)\n", td_count, fio_thread->core_id);
	while (tmpinfo != NULL) {
		fprintf(stderr, " %s: handle=%lu, core_mask=0x%lx, cq_mask=0x%lx, sector_size=%u\n", \
			tmpinfo->f->file_name, tmpinfo->dev_handle, tmpinfo->core_mask, tmpinfo->cq_thread_mask, tmpinfo->sector_size);
		tmpinfo = tmpinfo->next;
	}

	td_count++;

	pthread_mutex_unlock(&mutex);

	return 0;
err:
	return 1;
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
	td->orig_buffer = kv_zalloc(aligned_total_mem + 4*1024); // alloc 4KB more for aligning io_u buf
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

	unsigned int aligned_max_bs = MEM_ALIGN(kv_fio_max_bs(td), 4); // dword(4B) aligned
	char *orig_buffer;

	fio_req = calloc(1, sizeof(*fio_req));
	if (fio_req == NULL) {
		return 1;
	}

	if (td->o.odirect || td->o.mem_align || td->o.oatomic ||
#if (CHECK_FIO_VERSION(FIO_MAJOR_VERSION, FIO_MINOR_VERSION) >= CHECK_FIO_VERSION(2,14))
	  td_ioengine_flagged(td, FIO_RAWIO)) {
#else
	  td->io_ops->flags & FIO_RAWIO) {
#endif
		orig_buffer = PTR_ALIGN(td->orig_buffer, g_page_mask) + td->o.mem_align;
	} else {
		orig_buffer = td->orig_buffer;
	}
	io_u->buf = orig_buffer + (io_u->index * aligned_max_bs);
	io_u->engine_data = fio_req;

	fio_req->io = io_u;
	fio_req->fio_thread = fio_thread;
	fio_req->key_size = fio_thread->key_size;
	fio_req->key = kv_zalloc(MEM_ALIGN(fio_req->key_size, 4)); //for long key support

	return fio_req->key == NULL;
}

static void kv_fio_io_u_free(struct thread_data *td, struct io_u *io_u)
{
	struct kv_fio_request *fio_req = io_u->engine_data;

	if (fio_req) {
		assert(fio_req->io == io_u);
		kv_free(fio_req->key);
		free(fio_req);
		io_u->engine_data = NULL;
	}
}

static void kv_fio_completion_cb(kv_pair *kv, unsigned int result, unsigned int status)
{
	assert(kv != NULL);

	struct kv_fio_request		*fio_req = kv->param.private_data;
	struct kv_fio_thread		*fio_thread = fio_req->fio_thread;

	pthread_mutex_lock(fio_thread->head_mutex);
	fio_thread->iocq[fio_thread->iocq_head] = fio_req->io;
	increase_iocq_head(fio_thread);
	pthread_mutex_unlock(fio_thread->head_mutex);
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

	uint64_t		handle = 0;
	uint32_t		sector_size = 0;

	struct kv_dev_info	*dev_info = fio_thread->dev_info;
	while (dev_info != NULL) {
		if (dev_info->f == io_u->file){
			handle = dev_info->dev_handle;
			sector_size = dev_info->sector_size;
			break;
		}
		dev_info = dev_info->next;
	}
	if (dev_info == NULL || handle == 0 ) {
		return FIO_Q_COMPLETED;
	}

	kv_pair* kv = &fio_req->kv;
	kv->key.length = fio_req->key_size;
	kv->keyspace_id = KV_KEYSPACE_IODATA;

	kv->value.value = io_u->buf;
	kv->value.length = io_u->xfer_buflen;
	kv->value.actual_value_size = 0;
	kv->value.offset = 0;

	if(fio_thread->ssd_type == LBA_TYPE_SSD) {
		uint64_t lba = io_u->offset / sector_size; //lba addr
		memcpy(fio_req->key, &lba, sizeof(uint64_t));
	} else { // KV_TYPE_SSD
		uint64_t _key = io_u->offset / io_u->xfer_buflen;
		memcpy(fio_req->key, &_key, MIN(kv->key.length, sizeof(uint64_t)));
		if (io_u->xfer_buflen == ZERO_VALUE_MAGICNUM) {
			kv->value.length = 0;
		}
	}
	kv->key.key = fio_req->key;

	if (is_async(td)) {
		kv->param.async_cb = kv_fio_completion_cb;
		kv->param.private_data = fio_req;
	}

	switch (io_u->ddir) {
	case DDIR_READ:
		kv->param.io_option.retrieve_option = KV_RETRIEVE_DEFAULT;
		ret = kv_fio_read(handle, fio_thread->qid, kv);
		break;
	case DDIR_WRITE:
		kv->param.io_option.store_option = KV_STORE_DEFAULT;
		ret = kv_fio_write(handle, fio_thread->qid, kv);
		break;
	default: // NOT support DDIR_TRIM, DDIR_SYNC, DDIR_DATASYNC
		//break;
		return FIO_Q_COMPLETED;
	}

	return (ret) ? FIO_Q_BUSY : fio_thread->fio_q_finished;
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
	struct io_u *io = fio_thread->iocq[fio_thread->iocq_tail];
	assert(io != NULL);
	increase_iocq_tail(fio_thread);
	return io;
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
	volatile unsigned int iocq_count = 0;
	uint64_t timeout = 0;

	if (t) {
		timeout = t->tv_sec * 1000000000L + t->tv_nsec;
		clock_gettime(CLOCK_MONOTONIC_RAW, &t0);
	}
	assert(min <= max);

	struct kv_dev_info *dev_info;
	for (;;) {
		dev_info = fio_thread->dev_info;
	        while (dev_info != NULL) {
			kv_nvme_process_completion_queue(dev_info->dev_handle, fio_thread->qid);
			dev_info = dev_info->next;
		}
		iocq_count = calculate_num_completed_io(fio_thread);
		if (iocq_count >= min) {
			return iocq_count;
		}

		if (t) {
			clock_gettime(CLOCK_MONOTONIC_RAW, &t1);
			uint64_t elapse = ((t1.tv_sec - t0.tv_sec) * 1000000000L)
					  + t1.tv_nsec - t0.tv_nsec;
			if (elapse > timeout) {
				break;
			}
		}
	}

	return iocq_count;
}

static int kv_fio_invalidate(struct thread_data *td, struct fio_file *f)
{
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
		free(fio_thread->head_mutex);
	}

	struct kv_dev_info *dev_info = fio_thread->dev_info;
	while (dev_info != NULL) {
		struct kv_dev_info *tmp = dev_info->next;
		free(dev_info);
		dev_info = tmp;
	}

	free(fio_thread);
	pthread_mutex_lock(&mutex);
	td_count--;
	if (td_count == 0) {
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

        .options                = options,
        .option_struct_size     = sizeof(struct kv_fio_engine_options),
};
