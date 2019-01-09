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

/*
 * kvslab_core.c
 */

#include <kvslab_core.h>
#include "kvutil.h"
#include "kvnvme.h"
#include "kvcache.h"

#define SAFE_MALLOC(_n, _c, _t)				\
	(_n) = (_t*)malloc((_c) * (sizeof(_t)));	\
	if(!(_n)) {					\
		fprintf(stderr, "%s, line %d: malloc fail\n", __FUNCTION__, __LINE__);	\
		exit(-1);				\
	}						\

struct kvsl_settings kv_settings;

static uint32_t *kv_nfree_msinfoq;		/* # free memory slabinfo q */
static struct kvsl_slabhinfo *kv_free_msinfoq;  /* free memory slabinfo q */
static uint32_t *kv_nfull_msinfoq;		/* # full memory slabinfo q */
static struct kvsl_slabhinfo *kv_full_msinfoq;	/* full memory slabinfo q */

static uint8_t kv_nctable;			/* # class table entry */
struct kvsl_slabclass *kv_ctable;		/* table of slabclass indexed by cid */
static uint32_t *kv_nstable;			/* # slab table entry */
struct kvsl_slabinfo **kv_stable;		/* table of slabinfo indexed by sid */

static uint32_t kv_nmslab;		/* # memory slabs */
static size_t kv_mspace;		/* memory space */

extern kv_cache g_cache;


/*
 * Return the maximum space available for item sized chunks in a given
 * slab. Slab cannot contain more than 2^32 bytes (4G).
 */
static size_t kvsl_slab_data_size(void){
	return kv_settings.slab_size - KVSLAB_SLAB_HDR_SIZE;
}

static void kvsl_generate_profile(void){
	size_t *profile = kv_settings.profile; /* slab profile */
	uint8_t id;                         /* slab class id */
	size_t item_sz, last_item_sz;       /* current and last item chunk size */
	size_t min_item_sz, max_item_sz;    /* min and max item chunk size */

	KVSLAB_ASSERT(kv_settings.chunk_size % KVSLAB_ALIGNMENT == 0);
	KVSLAB_ASSERT(kv_settings.chunk_size <= kvsl_slab_data_size());

	min_item_sz = kv_settings.chunk_size;
	max_item_sz = kvsl_slab_data_size();
	id = KVSLAB_SLABCLASS_MIN_ID;
	item_sz = min_item_sz;

	while (id < MAX_NUM_SLAB_CLASS && item_sz < max_item_sz) {
		/* save the cur item chunk size */
		last_item_sz = item_sz;
		profile[id] = item_sz;
		id++;

		/* get the next item chunk size */
		item_sz *= kv_settings.factor;
		if (item_sz == last_item_sz) {
		    item_sz++;
		}
		item_sz = KV_MEM_ALIGN(item_sz, KVSLAB_ALIGNMENT);
	}
	if (item_sz >= max_item_sz) {
		profile[id] = max_item_sz;
		kv_settings.profile_last_id = id;
		kv_settings.max_chunk_size = max_item_sz;
	} else {
		kv_settings.profile_last_id = MAX_NUM_SLAB_CLASS - 1;
		kv_settings.max_chunk_size = profile[MAX_NUM_SLAB_CLASS - 1];
	}

	log_debug(KV_LOG_INFO, "[DONE]%s\n", __func__);
}


void kvsl_set_options(bool use_default, double factor, size_t max_slab_memory, size_t chunk_size, size_t slab_size, int slab_alloc_policy, int nr_slab){
	if (use_default == true) {
		kv_settings.factor = KVSLAB_FACTOR;			/*default=1.25*/
		kv_settings.max_slab_memory = KVSLAB_SLAB_MEMORY;	/*default=64MB*/
		kv_settings.chunk_size = KVSLAB_CHUNK_SIZE;		/*default=Item header + Item payload (aligned by unsigned long size)*/
		kv_settings.slab_size = KVSLAB_SLAB_SIZE;		/*default=1MB*/
		kv_settings.nr_slab = 1;
	} else {
		kv_settings.factor = factor;
		kv_settings.max_slab_memory = max_slab_memory;
		kv_settings.chunk_size = chunk_size;
		kv_settings.slab_size = slab_size;
		kv_settings.slab_alloc_policy = slab_alloc_policy;
		kv_settings.nr_slab = nr_slab;
	}

	memset(kv_settings.profile, 0, sizeof(kv_settings.profile));

	kvsl_generate_profile();

	log_debug(KV_LOG_INFO, "[DONE]%s\n", __func__);
	log_debug(KV_LOG_INFO, "\n"
				"[SETTINGS OPTIONS]\n"
				"  factor=%f\n"
				"  max_slab_memory=%lu\n"
				"  chunk_size=%lu\n"
				"  slab_size=%lu\n"
				"  profile_last_id=%d\n\n", \
				"  num_slab=%d\n\n", \
				kv_settings.factor, kv_settings.max_slab_memory, kv_settings.chunk_size, kv_settings.slab_size, kv_settings.profile_last_id, kv_settings.nr_slab);
}


static inline uint32_t kvsl_item_ntotal(uint32_t size){
    return KVSLAB_ITEM_HDR_SIZE + size;
}


/*
 * Return the cid of the slab which can store an item of a given size.
 * Return SLABCLASS_INVALID_ID, for large items which cannot be stored in
 * any of the configured slabs.
 */
static uint8_t kvsl_slab_cid(uint32_t size){
	uint8_t cid, imin, imax;

	KVSLAB_ASSERT(size != 0);

	imin = KVSLAB_SLABCLASS_MIN_ID;
	imax = kv_nctable;
	while (imax >= imin) {
		cid = (imin + imax) / 2;
		if (size > kv_ctable[cid].size) {
			imin = cid + 1;
		} else if (cid > KVSLAB_SLABCLASS_MIN_ID && size <= kv_ctable[cid - 1].size) {
			imax = cid - 1;
		} else {
			break;
		}
	}

	if (imin > imax) {
		/* size too big for any slab */
		return KVSLAB_SLABCLASS_INVALID_ID;
	}

	KVSLAB_ASSERT(size <= kv_ctable[cid].size);
	if (cid > 0) {KVSLAB_ASSERT(size > kv_ctable[cid-1].size);};

	/*for debug: print_class_info*/
	/*
	fprintf(stderr, "[SLAB CID RESULT]\n");
	fprintf(stderr, "  total_size_to_store=%u\n", size);
	fprintf(stderr, "  cid=%u\n", cid);
	fprintf(stderr, "  item_size=%lu\n", kv_ctable[cid].size);
	if (cid > 0){
	    fprintf(stderr, "  prev_item_size=%lu\n", kv_ctable[cid-1].size);
	}
	if (cid < kv_settings.profile_last_id){
	    fprintf(stderr, "  next_item_size=%lu\n", kv_ctable[cid+1].size);
	}
	*/

	return cid;
}


static uint8_t kvsl_item_slabcid(uint32_t size){
	uint32_t ntotal;
	uint8_t cid;

	ntotal = kvsl_item_ntotal(size); /*total size including item header*/

	cid = kvsl_slab_cid(ntotal);

	return cid;
}


/*
 * Return and optionally verify the idx^th item with a given size in the
 * in given slab.
 */
static struct kvsl_item * kvsl_slab_to_item(struct kvsl_slab *slab, uint32_t idx, uint8_t did, size_t size, bool verify){
	struct kvsl_item *it;

	KVSLAB_ASSERT(slab->magic == KVSLAB_SLAB_MAGIC);
	KVSLAB_ASSERT(idx <= kv_stable[did][slab->sid].nalloc);
	KVSLAB_ASSERT(idx * size < kv_settings.slab_size);

	it = (struct kvsl_item *)((uint8_t *)slab->data + (idx * size));
	if (verify) {
		KVSLAB_ASSERT(it->magic == KVSLAB_ITEM_MAGIC);
		KVSLAB_ASSERT(it->cid == slab->cid);
		KVSLAB_ASSERT(it->sid == slab->sid);
		KVSLAB_ASSERT(it->did == slab->did);
	}

	return it;
}


/*
 * Return true if all items in the slab have been allocated, else
 * return false.
 */
static bool kvsl_slab_full(struct kvsl_slabinfo *sinfo){
	struct kvsl_slabclass *c;

	KVSLAB_ASSERT(sinfo->cid >= KVSLAB_SLABCLASS_MIN_ID && sinfo->cid < kv_nctable);
	c = &kv_ctable[sinfo->cid];

	return (c->nitem == sinfo->nalloc) ? true : false;
}


static struct kvsl_item * _kvsl_slab_get_item(uint8_t cid, uint8_t did){
	struct kvsl_slabclass *c;
	struct kvsl_slabinfo *sinfo;
	struct kvsl_slab *slab;
	struct kvsl_item *it;

	c = &kv_ctable[cid];

	/* allocate new item from partial slab */
	KVSLAB_ASSERT(!KVSLAB_TAILQ_EMPTY(&c->partial_msinfoq[did]));
	sinfo = KVSLAB_TAILQ_FIRST(&c->partial_msinfoq[did]);
	KVSLAB_ASSERT(!kvsl_slab_full(sinfo));
	slab = (struct kvsl_slab *)sinfo->addr;

	/* consume an item from partial slab */
	it = kvsl_slab_to_item(slab, sinfo->nalloc, did, c->size, false); //get item addr
	sinfo->nalloc++;

	it->offset = (uint32_t)((uint8_t *)it - (uint8_t *)slab); //including slab hdr size
	it->sid = slab->sid;
	it->did = did;
	kvsl_slab_increase_use_cnt(it->did, it->sid);

	if (kvsl_slab_full(sinfo)) {
		/* move memory slab from partial to full q */
		KVSLAB_TAILQ_REMOVE(&c->partial_msinfoq[did], sinfo, tqe);
		kv_nfull_msinfoq[did]++;
		KVSLAB_TAILQ_INSERT_TAIL(&kv_full_msinfoq[did], sinfo, tqe);
	}

	return it;
}


/*
 * evict the first slab(info) which is in full_msinfoq
 */
static int kvsl_cache_delete(kv_pair* kv){
	if(!kv || !kv->key.key ){
		return KV_CACHE_ERR_INVALID_PARAM;
	}
	int ret = KV_CACHE_SUCCESS;
	kv_key* key = &kv->key;

	kv_pair* deleted = art_delete(&g_cache.rtree, key->key, key->length);

	if(deleted){
		//g_cache.mm.free_fn(deleted);
	}
	else{
		ret = KV_CACHE_ERR_NO_CACHED_KEY;
	}
	return ret;
}


#define MAX_RETRY_CNT (10)
static kvsl_rstatus_t kvsl_slab_evict(uint8_t did){
	struct kvsl_slabclass *c;	/* slab class */
	struct kvsl_slabinfo *msinfo;	/* memory slabinfo */

	KVSLAB_ASSERT(!KVSLAB_TAILQ_EMPTY(&kv_full_msinfoq[did]));
	KVSLAB_ASSERT(kv_nfull_msinfoq[did] > 0);

	/* get memory sinfo from full q */
	/* find slab of which sinfo->in_use_cnt == 0 */
	msinfo = NULL;
	struct kvsl_slabinfo *tmp_sinfo;
	while (msinfo == NULL){
	    KVSLAB_TAILQ_FOREACH(tmp_sinfo, &kv_full_msinfoq[did], tqe){
		pthread_mutex_lock(&tmp_sinfo->in_use_cnt_mutex);
		if (tmp_sinfo->in_use_cnt <= 0){
		    msinfo = tmp_sinfo;
		    pthread_mutex_unlock(&tmp_sinfo->in_use_cnt_mutex);
		goto find;
		}
		pthread_mutex_unlock(&tmp_sinfo->in_use_cnt_mutex);
	    }
	}

find:
	kv_nfull_msinfoq[did]--;
	KVSLAB_TAILQ_REMOVE(&kv_full_msinfoq[did], msinfo, tqe);
	KVSLAB_ASSERT(kvsl_slab_full(msinfo));

	c = &kv_ctable[msinfo->cid];
	c->nmslab[did]--;
	c->nevict[did]++;

	/* move msinfo to free q */
	kv_nfree_msinfoq[did]++;
	KVSLAB_TAILQ_INSERT_TAIL(&kv_free_msinfoq[did], msinfo, tqe);

	struct kvsl_slab *slab;	/* read slab */
	uint32_t idx;		/* idx^th item */

	/* initialize (or make invalid) the item */
	slab = (struct kvsl_slab *)msinfo->addr;

	if(g_cache.rtree.size!=0){
		int ret = 0;
		for (idx = 0; idx < c->nitem; idx++) {
			struct kvsl_item *it = kvsl_slab_to_item(slab, idx, did, c->size, false);
			it->evicted = true;

			kv_pair *kv = (kv_pair *)kvsl_item_data(it);

			ret = pthread_rwlock_rdlock(&g_cache.tree_rwlock);
			check_lock(ret);

			kv_pair* cache_kv = art_search(&g_cache.rtree, kv->key.key, kv->key.length);

			ret = pthread_rwlock_unlock(&g_cache.tree_rwlock);
			check_lock(ret);

			if(cache_kv){
				ret = pthread_rwlock_wrlock(&g_cache.tree_rwlock);
				check_lock(ret);

				kvsl_cache_delete(cache_kv);

				ret = pthread_rwlock_unlock(&g_cache.tree_rwlock);
				check_lock(ret);
			}

			/*debug*/
			//printf("    [EVICT] cid=%u, sid=%u, offset=%u mod_offset=%lu\n", \
				it->cid, it->sid, it->offset, (it->offset-KVSLAB_SLAB_HDR_SIZE)/(KVSLAB_ITEM_HDR_SIZE+(it->ndata)));
		}
	}
	msinfo->cid = KVSLAB_SLABCLASS_INVALID_ID;

	return KVSLAB_OK;
}


static struct kvsl_item * kvsl_slab_get_item(uint8_t cid, uint8_t did){
	kvsl_rstatus_t status;
	struct kvsl_slabclass *c;
	struct kvsl_slabinfo *sinfo;
	struct kvsl_slab *slab;

	KVSLAB_ASSERT(cid >= KVSLAB_SLABCLASS_MIN_ID && cid < kv_nctable);
	c = &kv_ctable[cid];

	if (!KVSLAB_TAILQ_EMPTY(&c->partial_msinfoq[did])) { //if partial_msinfoq in the slabclass(ctable[cid]) is not empty
		return _kvsl_slab_get_item(cid, did);
	}

	//(default) 1MB * 64 slabs(info) are inserted to free_msinfoq after slab_init_stable()
	if (!KVSLAB_TAILQ_EMPTY(&kv_free_msinfoq[did])) { //if there's free memory slab
		/* move memory slab from free to partial q */
		sinfo = KVSLAB_TAILQ_FIRST(&kv_free_msinfoq[did]);
		KVSLAB_ASSERT(kv_nfree_msinfoq[did] > 0);
		kv_nfree_msinfoq[did]--;
		c->nmslab[did]++;
		KVSLAB_TAILQ_REMOVE(&kv_free_msinfoq[did], sinfo, tqe);

		/* init partial sinfo */
		KVSLAB_TAILQ_INSERT_HEAD(&c->partial_msinfoq[did], sinfo, tqe);
		/* sid is already initialized by slab_init */
		/* addr is already initialized by slab_init */
		sinfo->nalloc = 0;
		sinfo->cid = cid;
		sinfo->did = did;
		sinfo->in_use_cnt = 0;

		/* init slab of partial sinfo */
		slab = (struct kvsl_slab *)sinfo->addr;
		slab->magic = KVSLAB_SLAB_MAGIC;
		slab->cid = cid;
		slab->sid = sinfo->sid;
		slab->did = did;

		return _kvsl_slab_get_item(cid, did);
	}

	KVSLAB_ASSERT(!KVSLAB_TAILQ_EMPTY(&kv_full_msinfoq[did]));
	KVSLAB_ASSERT(kv_nfull_msinfoq[did] > 0);

	//if there aren't slabs in partial q and free q
	status = kvsl_slab_evict(did);
	if (status != KVSLAB_OK) {
	    return NULL;
	}

	return kvsl_slab_get_item(cid, did);
}


/*
 * Return true if slab class id cid is valid and within bounds, otherwise
 * return false.
 */
static bool kvsl_slab_valid_id(uint8_t cid){
	if (cid >= KVSLAB_SLABCLASS_MIN_ID && cid <= kv_settings.profile_last_id) {
		return true;
	}

	return false;
}


static struct kvsl_item * kvsl_item_get(uint32_t size, uint8_t cid, uint8_t did){
	struct kvsl_item *it;

	KVSLAB_ASSERT(kvsl_slab_valid_id(cid));

	it = kvsl_slab_get_item(cid, did);

	if (it == NULL) {
		return NULL;
	}

	it->magic = KVSLAB_ITEM_MAGIC;
	it->cid = cid;
	it->did = did;
	it->evicted = false;

	/*debug*/
	//printf("    [ALLOC] cid=%u, sid=%u(%u/%u), offset=%u mod_offset=%lu\n", \
		it->cid, it->sid, kv_stable[it->sid].nalloc, kv_ctable[it->cid].nitem, it->offset, (it->offset-KVSLAB_SLAB_HDR_SIZE)/(KVSLAB_ITEM_HDR_SIZE+size));

	return it;
}


struct kvsl_item* kvsl_get_free_item(uint32_t size, uint8_t did){
	uint8_t cid;
	struct kvsl_item *it;

	cid = kvsl_item_slabcid(size);
	if (cid == KVSLAB_SLABCLASS_INVALID_ID) {
		fprintf(stderr, "[ERROR]item_slabcid_fail(item_size \"%lu\" is too big)\n", KVSLAB_ITEM_HDR_SIZE + size);
		return NULL;
	}

	it = kvsl_item_get(size, cid, did);

	return it;
}

void kvsl_slab_increase_use_cnt(uint8_t did, uint32_t sid){
	struct kvsl_slabinfo *sinfo = &kv_stable[did][sid];
	pthread_mutex_lock(&sinfo->in_use_cnt_mutex);
	sinfo->in_use_cnt++;
	pthread_mutex_unlock(&sinfo->in_use_cnt_mutex);
}

void kvsl_slab_decrease_use_cnt(uint8_t did, uint32_t sid){
	struct kvsl_slabinfo *sinfo = &kv_stable[did][sid];
	pthread_mutex_lock(&sinfo->in_use_cnt_mutex);
	sinfo->in_use_cnt--;
	pthread_mutex_unlock(&sinfo->in_use_cnt_mutex);
}

static kvsl_rstatus_t kvsl_slab_init_ctable(void){
	struct kvsl_slabclass *c;
	uint8_t cid;
	size_t *profile;
	int nr_slab = kv_settings.nr_slab;
	KVSLAB_ASSERT(kv_settings.profile_last_id <= KVSLAB_SLABCLASS_MAX_ID);

	profile = kv_settings.profile;
	kv_nctable = kv_settings.profile_last_id + 1;
	kv_ctable = malloc(sizeof(*kv_ctable) * kv_nctable);

	if (kv_ctable == NULL) {
		fprintf(stderr, "ctable_malloc_failed\n");
	        return KVSLAB_ENOMEM;
	}

	for (cid = KVSLAB_SLABCLASS_MIN_ID; cid < kv_nctable; cid++) {
		c = &kv_ctable[cid];
		c->nitem = kvsl_slab_data_size() / profile[cid];
		c->size = profile[cid];
		c->slack = kvsl_slab_data_size() - (c->nitem * c->size);

		SAFE_MALLOC(c->partial_msinfoq, nr_slab, struct kvsl_slabhinfo);
		SAFE_MALLOC(c->nmslab, nr_slab, uint32_t);
		SAFE_MALLOC(c->nevict, nr_slab, uint64_t);
		for (int i=0; i<nr_slab; i++){
			KVSLAB_TAILQ_INIT(&c->partial_msinfoq[i]);
			c->nmslab[i] = 0;
			c->nevict[i] = 0;
		}
	}

	log_debug(KV_LOG_INFO, "[DONE]%s\n", __func__);
	return KVSLAB_OK;
}


static kvsl_rstatus_t kvsl_slab_init_stable(int did){
	struct kvsl_slabinfo *sinfo;
	uint32_t idx;

	kv_nstable[did] = kv_nmslab;
	kv_stable[did] = malloc(sizeof(*kv_stable[did]) * kv_nstable[did]);

	if (kv_stable[did] == NULL) {
		fprintf(stderr, "stable_malloc_failed\n");
		return KVSLAB_ENOMEM;
	}

	/* init memory slabinfo q  */
	for (idx = 0; idx < kv_nmslab; idx++) {
		sinfo = &kv_stable[did][idx];

		switch(kv_settings.slab_alloc_policy){
			case SLAB_MM_ALLOC_HUGE:
				sinfo->addr = kv_zalloc(kv_settings.slab_size);
				break;
			case SLAB_MM_ALLOC_POSIX:
			default:
				sinfo->addr = malloc(kv_settings.slab_size);
				break;
		}
		if (sinfo->addr == NULL) {
			fprintf(stderr, "slab alloc fail(did %d, idx %u)\n", did, idx+1);
			/* if num of allocated slab count is larger(or equal) than nctable, return OK, but notify user of the situation */
			if ((idx) >= kv_nctable) {
				kv_nstable[did] = idx;
				fprintf(stderr, "but minimal slab memory size was satisfied for device#%d", did);
				fprintf(stderr, " (target total size=%luMB, actual allocated total size=%luMB)\n", kv_nmslab * kv_settings.slab_size / MB, idx * kv_settings.slab_size / MB);
				return KVSLAB_OK;
			}
			return KVSLAB_ERROR;
		}

		sinfo->sid = idx;
		sinfo->nalloc = 0;
		sinfo->cid = KVSLAB_SLABCLASS_INVALID_ID;

		sinfo->in_use_cnt = 0;
		sinfo->in_use_cnt_mutex = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;

	        kv_nfree_msinfoq[did]++;
		KVSLAB_TAILQ_INSERT_TAIL(&kv_free_msinfoq[did], sinfo, tqe);
	}

	log_debug(KV_LOG_INFO, "[DONE]%s\n", __func__);
	return KVSLAB_OK;
}


void print_slab_class_info(bool print_detail){
	struct kvsl_slabclass *c;
	uint8_t cid;

	log_debug(KV_LOG_INFO, "[SLAB CLASS INFORMATION]\n");
	log_debug(KV_LOG_INFO, "  - num_of_class=%u\n", kv_nctable);
	log_debug(KV_LOG_INFO, "  - num_of_slab_per_device=%u\n", kv_nmslab);
	log_debug(KV_LOG_INFO, "  - total_size_of_slab_mem_per_deivce=%lu (%luMB)\n", kv_mspace, kv_mspace/MB);
	log_debug(KV_LOG_INFO, "  - total_size_of_slab_mem=%lu (%luMB)\n", kv_mspace * kv_settings.nr_slab, (kv_mspace * kv_settings.nr_slab)/MB);

	log_debug(KV_LOG_INFO, "  - min_item_size=%lu (cid=%u, num_of_item=%u)\n", kv_ctable[KVSLAB_SLABCLASS_MIN_ID].size, KVSLAB_SLABCLASS_MIN_ID, kv_ctable[KVSLAB_SLABCLASS_MIN_ID].nitem);
	log_debug(KV_LOG_INFO, "  - max_item_size=%lu (cid=%u, num_of_item=%u)\n", kv_ctable[kv_nctable-1].size, kv_nctable-1, kv_ctable[kv_nctable-1].nitem);

	if (print_detail) {
		log_debug(KV_LOG_INFO, "[CTABLE INFORMATION]\n");
		log_debug(KV_LOG_INFO, "  %7s %10s %10s %10s %10s   %10s %10s\n", "[class]", "[nitem]", "[size]", "[data]", "[slack]", "[nmslab]", "[nevict]");
		for (cid = KVSLAB_SLABCLASS_MIN_ID; cid < kv_nctable; cid++) {
			c = &kv_ctable[cid];
			log_debug(KV_LOG_INFO, "  %7u %10u %10lu %10lu %10lu\n",	\
					cid, c->nitem, c->size, c->size - KVSLAB_ITEM_HDR_SIZE, c->slack);
			for(int i=0; i<kv_settings.nr_slab; i++){
				log_debug(KV_LOG_INFO, "%55s %10u %10u (did: %d)\n", " ", c->nmslab[i], c->nevict[i], i);
			}
		}
	}
}


void print_slab_info(void){
	int i, j;
	struct kvsl_slabinfo * sinfo;

	printf("[STABLE INFORMATION]\n");
	printf("  - num_of_class=%u\n", kv_nctable);
	printf("  - num_of_slab_per_device=%u\n", kv_nmslab);
	printf("  - total_size_of_slab_mem_per_device=%lu (%luMB)\n", kv_mspace, kv_mspace/MB);

	for (i = 0;  i < kv_settings.nr_slab; i++) {
		printf(" device [%d]\n", i);
		printf("  %7s %10s %10s\n", "[sid]", "[nalloc]", "[cid]");
		for (j = 0; j < kv_nstable[i]; j++) {
			sinfo = &kv_stable[i][j];
			printf("  %7u %10u %10u\n", \
				sinfo->sid, sinfo->nalloc, sinfo->cid);
		}
		printf("\n");
	}
	printf("\n");
}


kvsl_rstatus_t kvsl_slab_init(void){
	kvsl_rstatus_t status;
	int nr_slab = kv_settings.nr_slab; //nr_slab is same with num of devices for now
	int nr_dev = nr_slab;

	/* [multi slab] init global var */
	SAFE_MALLOC(kv_nfree_msinfoq, nr_slab, uint32_t);
	SAFE_MALLOC(kv_free_msinfoq, nr_slab, struct kvsl_slabhinfo);
	SAFE_MALLOC(kv_nfull_msinfoq, nr_slab, uint32_t);
	SAFE_MALLOC(kv_full_msinfoq, nr_slab, struct kvsl_slabhinfo);

	SAFE_MALLOC(kv_nstable, nr_slab, uint32_t);
	SAFE_MALLOC(kv_stable, nr_slab, struct kvsl_slabinfo*);

	kv_nctable = 0;
	kv_ctable = NULL;
	kv_nmslab = 0;
	kv_mspace = 0;

	/* init slab class table */
	status = kvsl_slab_init_ctable();
	if (status != KVSLAB_OK) {
		fprintf(stderr, "[ERROR]slab_init_ctable=%d\n", status);
		return status;
	}

	/* init nmslab and mspace */
	kv_nmslab = MAX(kv_nctable, kv_settings.max_slab_memory / kv_settings.slab_size);
	kv_mspace = kv_nmslab * kv_settings.slab_size;

	for(int did=0; did<nr_slab; did++){
		kv_nfree_msinfoq[did] = 0;
		KVSLAB_TAILQ_INIT(&kv_free_msinfoq[did]);
		kv_nfull_msinfoq[did] = 0;
		KVSLAB_TAILQ_INIT(&kv_full_msinfoq[did]);

		kv_nstable[did] = 0;
		kv_stable[did] = NULL;

		/* init slab info table and slab */
		status = kvsl_slab_init_stable(did);
		if (status != KVSLAB_OK) {
			return status;
		}

		//note: to avoid lazy-allocation, we need to set the whole range of allocated memory
		struct kvsl_slabinfo *sinfo;
		struct kvsl_slab *slab;
		for(uint32_t idx = 0; idx < kv_nstable[did]; idx++){
			slab = (struct kvsl_slab *)kv_stable[did][idx].addr;
			memset(slab, 0, kv_settings.slab_size);
		}
		*(volatile char*)slab = *(volatile char*)slab;
	}
	log_debug(KV_LOG_INFO, "[DONE]memset total slabs with %s allocation for slabs\n", (kv_settings.slab_alloc_policy==SLAB_MM_ALLOC_HUGE)?"HUGE MEMORY":"POSIX MALLOC");
	log_debug(KV_LOG_INFO, "[DONE]%s\n", __func__);
	return KVSLAB_OK;
}


kvsl_rstatus_t kvsl_slab_deinit(void){
	kvsl_rstatus_t result = KVSLAB_OK;

	free(kv_nfree_msinfoq);
	free(kv_free_msinfoq);
	free(kv_nfull_msinfoq);
	free(kv_full_msinfoq);

	for(int did = 0; did < kv_settings.nr_slab; did++){
		for(uint32_t idx = 0; idx < kv_nmslab; idx++){
			switch(kv_settings.slab_alloc_policy){
				case SLAB_MM_ALLOC_HUGE:
					kv_free(kv_stable[did][idx].addr);
					break;
				case SLAB_MM_ALLOC_POSIX:
				default:
					free(kv_stable[did][idx].addr);
					break;
			}
		}
		free(kv_stable[did]);
	}

	free(kv_nstable);
	free(kv_stable);

	for(uint8_t cid = KVSLAB_SLABCLASS_MIN_ID; cid < kv_nctable; cid++){
		struct kvsl_slabclass* c = &kv_ctable[cid];
		free(c->partial_msinfoq);
		free(c->nmslab);
		free(c->nevict);
	}
	free(kv_ctable);

	if (result==KVSLAB_OK){
		log_debug(KV_LOG_INFO, "[DONE]%s\n", __func__);
	}
	return result;
}
