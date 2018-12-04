/* 
 * Author; Trong-Dat Nguyen
 * MySQL REDO log with NVDIMM
 * Using libpmemobj
 * Copyright (c) 2017 VLDB Lab - Sungkyunkwan University
 * */

//Common includes
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>                                                                      
#include <sys/time.h> //for struct timeval, gettimeofday()
#include <string.h>
#include <stdint.h> //for uint64_t
#include <math.h> //for log()
#include <assert.h>
#include <wchar.h>
#include <unistd.h> //for access()

//Global include
#include "wt_internal.h"
//includes from pmem
//#include "my_pmem_common.h"
//#include "my_pmemobj.h"
#include <libpmemobj.h>

//from InnoDB
//#include "os0file.h"
//#include "buf0dblwr.h"

#if defined (UNIV_PMEMOBJ_BUF)
//GLOBAL variables

#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
//256 buckets => 8 bits, max 32 spaces => 5 bits => need 3 = 8 - 5 bits
static uint64_t PMEM_N_BUCKET_BITS = 8;
static uint64_t PMEM_N_SPACE_BITS = 5;
static uint64_t PMEM_PAGE_PER_BUCKET_BITS=10;

static FILE* debug_file = fopen("part_debug.txt","a");

/*
 * This is the "clear" version of hash function, use it for debugging 
 * For the production version, use the macro instead 
 * LESS_BUCKET partition
 * space_no and page_no are 32-bits value
 * the hashed value is B-bits value where B is the number of bits to present the number of buckets 
 * One space_no in a bucket has maximum N pages where log2(N) is page_per_bucket_bits
@hashed		[out]: return hashed value
@space		[in]: space_no
@page		[in]: page_no
@n			[in]: number of buckets 
@B			[in]: number of bits present number of buckets
@S			[in]: number of bits present space_no
@P			[in]: number of bits present max number of pages per space on a bucket, this value is log2(page_per_bucket)
 * 
 * */
ulint 
hash_f1(
		uint64_t&			hashed,
		uint32_t		space_no,
	   	uint32_t		page_no,
	   	uint64_t		n,
		uint64_t		B,	
		uint64_t		S,
		uint64_t		P)
{	

	uint32_t mask1 = 0xffffffff >> (32 - S);
	uint32_t mask2 = 0xffffffff >> 
		(32 - P - (B - S)) ;

	ulint p;
	ulint s;

	s = (space_no & mask1) << (B - S);
	p = (page_no & mask2) >> P;

	hashed = (p + s) % n;

	printf("space %zu (0x%08x) page %zu (0x%08x)  p 0x%016x s 0x%016x hashed %zu (0x%016x) \n",
			space_no, space_no, page_no, page_no, p, s, hashed, hashed);

	return hashed;
}
#endif //UNIV_PMEMOBJ_BUF_PARTITION

/*
  There are two types of structure need to be allocated: structures in NVM that non-volatile after shutdown server or power-off and D-RAM structures that only need when the server is running. 
 * Case 1: First time the server start (fresh server): allocate structures in NVM
 * Case 2: server've had some data, NVM structures already existed. Just get them
 *
 * For both cases, we need to allocate structures in D-RAM
 * */
int
pm_wrapper_buf_alloc_or_open(
		PMEM_WRAPPER*		pmw,
		const size_t		buf_size,
		const size_t		page_size)
{

	uint64_t i;
	char sbuf[256];
	WT_SESSION_IMPL *session = pmw->session;
	int ret;

	/////////////////////////////////////////////////
	// PART 1: NVM structures
	// ///////////////////////////////////////////////
	if (!pmw->pbuf) {
		//Case 1: Alocate new buffer in PMEM
			printf("PMEMOBJ_INFO: allocate %zd MB of buffer in pmem\n", buf_size);

		if ( pm_wrapper_buf_alloc(pmw, buf_size, page_size) == PMEM_ERROR ) {
			printf("PMEMOBJ_ERROR: error when allocate buffer in buf_dblwr_init()\n");
		}
	}
	else {
		//Case 2: Reused a buffer in PMEM
		printf("!!!!!!! [PMEMOBJ_INFO]: the server restart from a crash but the buffer is persist, in pmem: size = %zd free_pool has = %zd free lists\n", 
				pmw->pbuf->size, D_RW(pmw->pbuf->free_pool)->cur_lists);
		//Check the page_size of the previous run with current run
		if (pmw->pbuf->page_size != page_size) {
	//This code is test for recovery, it has lock/unlock mutex. Note that we need the filename in pmem block because at recovery time, the space instance related with pmem_block may not load in the system. In that case, filename is used for ibd_load() func
	//If the performance reduce, then remove it
	//TODO: map to MongoDB
	/*
	fil_node_t*			node;

	node = pm_get_node_from_space(page_id.space());
	if (node == NULL) {
		printf("PMEM_ERROR node from space is NULL\n");
		assert(0);
	}
	strcpy(pfree_block->file_name, node->name);
	//end code
	*/	
	//D_RW(free_block)->bpage = bpage;
			printf("PMEM_ERROR: the pmem buffer size = %zu is different with UNIV_PAGE_SIZE = %zu, you must use the same page_size!!!\n ",
					pmw->pbuf->page_size, page_size);
			assert(0);
		}
			
		//We need to re-align the p_align
		byte* p;
		p = (byte*) (pmemobj_direct(pmw->pbuf->data));
		assert(p);
		//pmw->pbuf->p_align = (byte*) (ut_align(p, page_size));
		pmw->pbuf->p_align = (byte*) (WT_ALIGN(p, page_size));
	}
	////////////////////////////////////////////////////
	// Part 2: D-RAM structures and open file(s)
	// ///////////////////////////////////////////////////
	
#if defined( UNIV_PMEMOBJ_BUF_STAT)
	pm_buf_bucket_stat_init(pmw->pbuf);
#endif	
//#if defined(UNIV_PMEMOBJ_BUF_FLUSHER)
	//init threads for handle flushing, implement in buf0flu.cc
	pm_flusher_init(pmw, pmw->PMEM_N_FLUSH_THREADS);
//#endif 
#if defined (UNIV_PMEMOBJ_BUF_PARTITION_STAT)
	pm_filemap_init(pmw->pbuf);
#endif
	// The AIO	
	uint64_t n_segs = pmw->PMEM_N_FLUSH_THREADS;
	//one AIO segment handle one list, 
	//one AIO slot handle one page in the pmem bucket
	uint64_t n_slots = n_segs * pmw->PMEM_BUCKET_SIZE;
	AIO* aio = AIO_init(pmw, n_slots, n_segs);
	if (aio == NULL){
		printf("PMEM_ERROR, cannot create aio\n");
		exit(0);
	}

	//The bucket locks
	pmw->pbuf->bucket_locks = (WT_SPINLOCK*) calloc(pmw->PMEM_N_BUCKETS, sizeof(WT_SPINLOCK));

	for (i = 0; i < pmw->PMEM_N_BUCKETS; ++i){
		ret = __wt_spin_init(session, &pmw->pbuf->bucket_locks[i], "bucket_locks"); 
	}

	//In any case (new allocation or resued, we should allocate the flush_events for buckets in DRAM
	//pmw->pbuf->flush_events = (os_event_t*) calloc(pmw->PMEM_N_BUCKETS, sizeof(os_event_t));
	pmw->pbuf->flush_conds = (WT_CONDVAR**) calloc(pmw->PMEM_N_BUCKETS, sizeof(WT_CONDVAR*));
	pmw->pbuf->prev_list_flush_conds = (WT_CONDVAR**) calloc(pmw->PMEM_N_BUCKETS, sizeof(WT_CONDVAR*));

	for ( i = 0; i < pmw->PMEM_N_BUCKETS; i++) {
		sprintf(sbuf,"pm_flush_bucket%zu", i);
		ret = __wt_cond_alloc(session,"flush thread cond", &pmw->pbuf->flush_conds[i]);
		ret =__wt_cond_alloc(session,"prev list flush cond", &pmw->pbuf->prev_list_flush_conds[i]);
	}
		ret = __wt_cond_alloc(session,"flush free pool", &pmw->pbuf->free_pool_cond);


	/*Alocate the param array
	 * Size of param array list should at least equal to PMEM_N_BUCKETS
	 * (i.e. one bucket has at least one param array)
	 * In case of one bucket receive heavily write such that the list
	 * is full while the previous list still not finish aio_batch
	 * In this situation, the params of the current list may overwrite the
	 * params of the previous list. We may encounter this situation with SINGLE_BUCKET partition
	 * where one space only map to one bucket, hence all writes are focus on one bucket
	 */
	//PMEM_BUF_BLOCK_LIST* plist;
	ulint arr_size = 2 * pmw->PMEM_N_BUCKETS;

	pmw->pbuf->param_arr_size = arr_size;
	pmw->pbuf->param_arrs = (PMEM_AIO_PARAM_ARRAY*) (
		calloc(arr_size, sizeof(PMEM_AIO_PARAM_ARRAY)));	
	for ( i = 0; i < arr_size; i++) {
		//plist = D_RW(D_RW(pmw->pbuf->buckets)[i]);

		pmw->pbuf->param_arrs[i].params = (PMEM_AIO_PARAM*) (
				calloc(pmw->PMEM_BUCKET_SIZE, sizeof(PMEM_AIO_PARAM)));
		pmw->pbuf->param_arrs[i].is_free = true;
	}
	pmw->pbuf->cur_free_param = 0; //start with the 0
	
	//Open file 
	pmw->pbuf->deb_file = fopen("pmem_debug.txt","a");
	
	return PMEM_SUCCESS;
}

/*
 * CLose/deallocate resource in DRAM
 * */
int pm_wrapper_buf_close(PMEM_WRAPPER* pmw) {
	uint64_t i;
	WT_SESSION_IMPL *session;
	session = pmw->session;

#if defined (UNIV_PMEMOBJ_BUF_PARTITION_STAT)
	//pm_filemap_print(pmw->pbuf, pmw->pbuf->deb_file);
	pm_filemap_print(pmw->pbuf, debug_file);
	pm_filemap_close(pmw->pbuf);
#endif
//TODO: find how os event work in MongoDB

	for ( i = 0; i < pmw->PMEM_N_BUCKETS; i++) {
		__wt_spin_destroy(session, &pmw->pbuf->bucket_locks[i]);

		__wt_cond_destroy(session, &pmw->pbuf->flush_conds[i]);
		__wt_cond_destroy(session, &pmw->pbuf->prev_list_flush_conds[i]);
	}
	//os_event_destroy(pmw->pbuf->free_pool_event);
	__wt_cond_destroy(session, &pmw->pbuf->free_pool_cond);
	free(pmw->pbuf->bucket_locks);
	free(pmw->pbuf->flush_conds);
	free(pmw->pbuf->prev_list_flush_conds);

	//Free the param array
	for ( i = 0; i < pmw->PMEM_N_BUCKETS; i++) {
		free(pmw->pbuf->param_arrs[i].params);
	}
	free(pmw->pbuf->param_arrs);

//#if defined (UNIV_PMEMOBJ_BUF_FLUSHER)
	//Free the flusher
	pm_buf_flusher_close(pmw);
//#endif 

	//The AIO
	AIO_destroy(session, pmw->aio);
	fclose(pmw->pbuf->deb_file);

	return PMEM_SUCCESS;
}

int
pm_wrapper_buf_alloc(
		PMEM_WRAPPER*		pmw,
	    const size_t		size,
		const size_t		page_size)
{

	assert(pmw);

	pmw->pbuf = pm_pop_buf_alloc(pmw, size, page_size);
	if (!pmw->pbuf)
		return PMEM_ERROR;
	else
		return PMEM_SUCCESS;
}
/*
 * Allocate new log buffer in persistent memory and assign to the pointer in the wrapper
 * This function only allocate structures that in NVM
 * Structures in D-RAM are allocated outside in pm_wrapper_buf_alloc_or_open() function
 * */
PMEM_BUF* 
pm_pop_buf_alloc(
		PMEM_WRAPPER*		pmw,
		const size_t		size,
		const size_t		page_size)
{
	PMEMobjpool*		pop = pmw->pop;
	char* p;
	size_t align_size;

	TOID(PMEM_BUF) buf; 

	POBJ_ZNEW(pop, &buf, PMEM_BUF);

	PMEM_BUF *pbuf = D_RW(buf);
	align_size = WT_ALIGN(size, page_size);

	pbuf->size = align_size;
	pbuf->page_size = page_size;
	pbuf->type = BUF_TYPE;
	pbuf->is_new = true;

	pbuf->is_async_only = false;

	pbuf->max_offset = 0;

	pbuf->data = pm_pop_alloc_bytes(pop, align_size);
	//align the pmem address for DIRECT_IO
	p = (char*) (pmemobj_direct(pbuf->data));
	assert(p);
	//pbuf->p_align = static_cast<char*> (ut_align(p, page_size));
	pbuf->p_align = (byte*) (WT_ALIGN(p, page_size));
	pmemobj_persist(pop, pbuf->p_align, sizeof(*pbuf->p_align));

	if (OID_IS_NULL(pbuf->data)){
		//assert(0);
		return NULL;
	}
	
	//This assignment is necessary if the first arg in pm_buf_lists_init() is pmw	
	pmw->pbuf = pbuf;

	pm_buf_lists_init(pmw, align_size, page_size);
	pmemobj_persist(pop, pbuf, sizeof(*pbuf));
	return pbuf;
} 

/*
 * Init in-PMEM lists
 * bucket lists
 * free pool lists
 * and the special list
 * */
void 
pm_buf_lists_init(
		PMEM_WRAPPER*	pmw,
		const size_t	total_size,
	   	const size_t	page_size)
{
	uint64_t i;
	//uint64_t j;
	uint64_t cur_bucket;
	size_t offset;
	PMEMobjpool*	pop;
	PMEM_BUF*		buf;
	PMEM_BUF_FREE_POOL* pfreepool;
	PMEM_BUF_BLOCK_LIST* pfreelist;
	PMEM_BUF_BLOCK_LIST* plist;
	PMEM_BUF_BLOCK_LIST* pspeclist;

	pop = pmw->pop;
	buf = pmw->pbuf;

	size_t n_pages = (total_size / page_size);
	size_t bucket_size = pmw->PMEM_BUCKET_SIZE * page_size;
	size_t n_pages_per_bucket = pmw->PMEM_BUCKET_SIZE;
	size_t n_lists_in_ckpt = (pmw->PMEM_CKPT_BLOCK_SIZE / page_size) / n_pages_per_bucket;
	// # of lists in free pool = total # of lists - lists in bucket - 1 special list - # lists for checkpoint
	size_t n_lists_in_free_pool = n_pages / pmw->PMEM_BUCKET_SIZE - pmw->PMEM_N_BUCKETS - 1 - n_lists_in_ckpt;

	printf("\n\n=======> PMEM_INFO: n_pages = %zu bucket size = %f MB (%zu %zu-KB pages) n_lists in free_pool %zu\n", n_pages, bucket_size*1.0 / (1024*1024), n_pages_per_bucket, (page_size/1024), n_lists_in_free_pool);

	//Don't reset those variables during the init
	offset = 0;
	cur_bucket = 0;

	//init the temp args struct
	struct list_constr_args* args = (struct list_constr_args*) malloc(sizeof(struct list_constr_args)); 
	args->size = page_size;
	args->check = PMEM_AIO_CHECK;
	args->state = PMEM_FREE_BLOCK;
	TOID_ASSIGN(args->list, OID_NULL);

	//(1) Init the buckets
	//The sub-buf lists
	POBJ_ALLOC(pop, &buf->buckets, TOID(PMEM_BUF_BLOCK_LIST),
			sizeof(TOID(PMEM_BUF_BLOCK_LIST)) * pmw->PMEM_N_BUCKETS, NULL, NULL);
	if (TOID_IS_NULL(buf->buckets) ){
		fprintf(stderr, "POBJ_ALLOC\n");
	}

	for(i = 0; i < pmw->PMEM_N_BUCKETS; i++) {
		//init the bucket 
		POBJ_ZNEW(pop, &D_RW(buf->buckets)[i], PMEM_BUF_BLOCK_LIST);	
		if(TOID_IS_NULL(D_RW(buf->buckets)[i])) {
			fprintf(stderr, "POBJ_ZNEW\n");
			assert(0);
		}
		plist = D_RW(D_RW(buf->buckets)[i]);

		//pmemobj_rwlock_wrlock(pop, &plist->lock);

		plist->cur_pages = 0;
		plist->n_aio_pending = 0;
		plist->n_sio_pending = 0;
		plist->max_pages = bucket_size / page_size;
		plist->list_id = cur_bucket;
		plist->hashed_id = cur_bucket;
		//plist->flush_worker_id = PMEM_ID_NONE;
		cur_bucket++;
		plist->hashed_id = i;
		plist->check = PMEM_AIO_CHECK;
		plist->state = PMEM_LIST_FILLING;
		TOID_ASSIGN(plist->next_list, OID_NULL);
		TOID_ASSIGN(plist->prev_list, OID_NULL);
		//plist->pext_list = NULL;
		//TOID_ASSIGN(plist->pext_list, OID_NULL);
	
		//init pages in bucket
		pm_buf_single_list_init(pop, D_RW(buf->buckets)[i], &offset, args, n_pages_per_bucket, page_size);

		//POBJ_ALLOC(pop, &plist->arr, TOID(PMEM_BUF_BLOCK),
		//		sizeof(TOID(PMEM_BUF_BLOCK)) * n_pages_per_bucket, NULL, NULL);

		//for (j = 0; j < n_pages_per_bucket; j++) {
		//	args->pmemaddr = offset;
		//	TOID_ASSIGN(args->list, (D_RW(buf->buckets)[i]).oid);
		//	offset += page_size;	
		//	POBJ_NEW(pop, &D_RW(plist->arr)[j], PMEM_BUF_BLOCK, pm_buf_block_init, args);
		//}

		//pmemobj_persist(pop, &plist->cur_pages, sizeof(plist->cur_pages));
		//pmemobj_persist(pop, &plist->max_pages, sizeof(plist->max_pages));
		//pmemobj_persist(pop, &plist->is_flush, sizeof(plist->is_flush));
		//pmemobj_persist(pop, &plist->next_list, sizeof(plist->next_list));
		//pmemobj_persist(pop, &plist->n_aio_pending, sizeof(plist->n_aio_pending));
		//pmemobj_persist(pop, &plist->n_sio_pending, sizeof(plist->n_sio_pending));
		//pmemobj_persist(pop, &plist->check, sizeof(plist->check));
		//pmemobj_persist(pop, &plist->list_id, sizeof(plist->list_id));
		//pmemobj_persist(pop, &plist->hashed_id, sizeof(plist->hashed_id));
		//pmemobj_persist(pop, &plist->next_free_block, sizeof(plist->next_free_block));
		//pmemobj_persist(pop, plist, sizeof(*plist));

		//pmemobj_rwlock_unlock(pop, &plist->lock);

		// next bucket
	}

	//(2) Init the free pool
	POBJ_ZNEW(pop, &buf->free_pool, PMEM_BUF_FREE_POOL);	
	if(TOID_IS_NULL(buf->free_pool)) {
		fprintf(stderr, "POBJ_ZNEW\n");
		assert(0);
	}
	pfreepool = D_RW(buf->free_pool);
	pfreepool->max_lists = n_lists_in_free_pool;
	pfreepool->cur_lists = 0;
	
	for(i = 0; i < n_lists_in_free_pool; i++) {
		TOID(PMEM_BUF_BLOCK_LIST) freelist;
		POBJ_ZNEW(pop, &freelist, PMEM_BUF_BLOCK_LIST);	
		if(TOID_IS_NULL(freelist)) {
			fprintf(stderr, "POBJ_ZNEW\n");
			assert(0);
		}
		pfreelist = D_RW(freelist);

		pfreelist->cur_pages = 0;
		pfreelist->max_pages = bucket_size / page_size;
		pfreelist->n_aio_pending = 0;
		pfreelist->n_sio_pending = 0;
		pfreelist->list_id = cur_bucket;
		pfreelist->hashed_id = PMEM_ID_NONE;
		//pfreelist->flush_worker_id = PMEM_ID_NONE;
		cur_bucket++;
		pfreelist->check = PMEM_AIO_CHECK;
		pfreelist->state = PMEM_LIST_FREE;
		TOID_ASSIGN(pfreelist->next_list, OID_NULL);
		TOID_ASSIGN(pfreelist->prev_list, OID_NULL);
	
		//init pages in bucket
		pm_buf_single_list_init(pop, freelist, &offset, args, n_pages_per_bucket, page_size);

		//POBJ_ALLOC(pop, &pfreelist->arr, TOID(PMEM_BUF_BLOCK),
		//		sizeof(TOID(PMEM_BUF_BLOCK)) * n_pages_per_bucket, NULL, NULL);

		//for (j = 0; j < n_pages_per_bucket; j++) {
		//	args->pmemaddr = offset;
		//	offset += page_size;	
		//	TOID_ASSIGN(args->list, freelist.oid);

		//	POBJ_NEW(pop, &D_RW(pfreelist->arr)[j], PMEM_BUF_BLOCK, pm_buf_block_init, args);
		//}

		//pmemobj_persist(pop, &pfreelist->cur_pages, sizeof(pfreelist->cur_pages));
		//pmemobj_persist(pop, &pfreelist->max_pages, sizeof(pfreelist->max_pages));
		//pmemobj_persist(pop, &pfreelist->is_flush, sizeof(pfreelist->is_flush));
		//pmemobj_persist(pop, &pfreelist->next_list, sizeof(pfreelist->next_list));
		//pmemobj_persist(pop, &pfreelist->prev_list, sizeof(pfreelist->prev_list));
		//pmemobj_persist(pop, &pfreelist->n_aio_pending, sizeof(pfreelist->n_aio_pending));
		//pmemobj_persist(pop, &pfreelist->n_sio_pending, sizeof(pfreelist->n_sio_pending));
		//pmemobj_persist(pop, &pfreelist->check, sizeof(pfreelist->check));
		//pmemobj_persist(pop, &pfreelist->list_id, sizeof(pfreelist->list_id));
		//pmemobj_persist(pop, &pfreelist->next_free_block, sizeof(pfreelist->next_free_block));
		//pmemobj_persist(pop, pfreelist, sizeof(*plist));

		//pmemobj_rwlock_unlock(pop, &pfreelist->lock);

		//insert this list in the freepool
		POBJ_LIST_INSERT_HEAD(pop, &pfreepool->head, freelist, list_entries);
		pfreepool->cur_lists++;
		//loop: init next buckes
	} //end init the freepool
	pmemobj_persist(pop, &buf->free_pool, sizeof(buf->free_pool));

	// (3) Init the special list used in recovery
	POBJ_ZNEW(pop, &buf->spec_list, PMEM_BUF_BLOCK_LIST);	
	if(TOID_IS_NULL(buf->spec_list)) {
		fprintf(stderr, "POBJ_ZNEW\n");
		assert(0);
	}
	pspeclist = D_RW(buf->spec_list);
	pspeclist->cur_pages = 0;
	pspeclist->max_pages = bucket_size / page_size;
	pspeclist->n_aio_pending = 0;
	pspeclist->n_sio_pending = 0;
	pspeclist->list_id = cur_bucket;
	pspeclist->hashed_id = PMEM_ID_NONE;
	//pfreelist->flush_worker_id = PMEM_ID_NONE;
	cur_bucket++;
	pspeclist->check = PMEM_AIO_CHECK;
	pspeclist->state = PMEM_LIST_FILLING;
	TOID_ASSIGN(pspeclist->next_list, OID_NULL);
	TOID_ASSIGN(pspeclist->prev_list, OID_NULL);
	//init pages in spec list 
	pm_buf_single_list_init(pop, buf->spec_list, &offset, args, n_pages_per_bucket, page_size);

	//(4) init the ckpt block
	buf->is_capture_ckpt = true;

	struct list_constr_args* args2 = (struct list_constr_args*) malloc(sizeof(struct list_constr_args)); 
	args2->check = PMEM_AIO_CHECK;
	args2->state = PMEM_FREE_BLOCK;
	TOID_ASSIGN(args2->list, OID_NULL);

	args2->size = pmw->PMEM_CKPT_BLOCK_SIZE;

	args2->pmemaddr = offset;
	offset += pmw->PMEM_CKPT_BLOCK_SIZE;

	TOID_ASSIGN(args2->list, OID_NULL);

	POBJ_NEW(pop, &buf->ckpt_block, PMEM_BUF_BLOCK, pm_buf_block_init, args2);
	
}

/*
 * Allocate and init blocks in a PMEM_BUF_BLOCK_LIST
 * THis function is called in pm_buf_lists_init()
 * pop [in]: pmemobject pool
 * plist [in/out]: pointer to the list
 * offset [in/out]: current offset, this offset will increase during the function run
 * n [in]: number of pages 
 * args [in]: temp struct to hold info
 * */
void 
pm_buf_single_list_init(
		PMEMobjpool*				pop,
		TOID(PMEM_BUF_BLOCK_LIST)	inlist,
		size_t*						offset,
		struct list_constr_args*	args,
		const size_t				n,
		const size_t				page_size){
		
		ulint i;

		PMEM_BUF_BLOCK_LIST*	plist;
		plist = D_RW(inlist);

		POBJ_ALLOC(pop, &plist->arr, TOID(PMEM_BUF_BLOCK),
				sizeof(TOID(PMEM_BUF_BLOCK)) * n, NULL, NULL);

		for (i = 0; i < n; i++) {
			args->pmemaddr = *offset;
			*offset += page_size;	

			TOID_ASSIGN(args->list, inlist.oid);
			POBJ_NEW(pop, &D_RW(plist->arr)[i], PMEM_BUF_BLOCK, pm_buf_block_init, args);
		}
		// Make properties in the list persist
		pmemobj_persist(pop, &plist->cur_pages, sizeof(plist->cur_pages));
		pmemobj_persist(pop, &plist->max_pages, sizeof(plist->max_pages));
		pmemobj_persist(pop, &plist->state, sizeof(plist->state));
		pmemobj_persist(pop, &plist->next_list, sizeof(plist->next_list));
		pmemobj_persist(pop, &plist->n_aio_pending, sizeof(plist->n_aio_pending));
		pmemobj_persist(pop, &plist->n_sio_pending, sizeof(plist->n_sio_pending));
		pmemobj_persist(pop, &plist->check, sizeof(plist->check));
		pmemobj_persist(pop, &plist->list_id, sizeof(plist->list_id));
		pmemobj_persist(pop, &plist->hashed_id, sizeof(plist->hashed_id));
		pmemobj_persist(pop, &plist->next_free_block, sizeof(plist->next_free_block));

		pmemobj_persist(pop, plist, sizeof(*plist));
}

/*This function is called as the func pointer in POBJ_LIST_INSERT_NEW_HEAD()*/
int
pm_buf_block_init(
		PMEMobjpool*	pop,
	   	void*			ptr,
	   	void*			arg)
{
	struct list_constr_args *args = (struct list_constr_args *) arg;
	PMEM_BUF_BLOCK* block = (PMEM_BUF_BLOCK*) ptr;
	block->max_size = block->size = args->size;
	block->check = args->check;
	block->state = args->state;
	TOID_ASSIGN(block->list, (args->list).oid);
	block->pmemaddr = args->pmemaddr;

	pmemobj_persist(pop, &block->size, sizeof(block->size));
	pmemobj_persist(pop, &block->check, sizeof(block->check));
	pmemobj_persist(pop, &block->state, sizeof(block->state));
	pmemobj_persist(pop, &block->list, sizeof(block->list));
	pmemobj_persist(pop, &block->pmemaddr, sizeof(block->pmemaddr));
	return 0;
}

PMEM_BUF* pm_pop_get_buf(PMEMobjpool* pop) {
	TOID(PMEM_BUF) buf;
	//get the first object in pmem has type PMEM_BUF
	buf = POBJ_FIRST(pop, PMEM_BUF);

	if (TOID_IS_NULL(buf)) {
		return NULL;
	}			
	else {
		PMEM_BUF *pbuf = D_RW(buf);
		if(!pbuf) {
			printf("PMEMOBJ_ERROR: message: %s\n",  pmemobj_errormsg() );
			return NULL;
		}
		return pbuf;
	}
}

/*
 * VERSION 3
 * *Write a page to pmem buffer using "instance swap"
 * This function is called by innodb cleaner thread
 * When a list is full, the cleaner thread does:
 * (1) find the free list from free_pool and swap with current full list
 * (2) add current full list to waiting-list of the flusher and notify the flusher about new added list then return (to reduce latency)
 * (3) The flusher looking for a idle worker to handle full list
@param[in] pop		the PMEMobjpool
@param[in] buf		the pointer to PMEM_BUF_
@param[in] src_data	data contains the bytes to write
@param[in] page_size
 * */
int
pm_buf_write_with_flusher_old2(
		   	PMEM_WRAPPER*	pmw,
			const char*		fname,
		   	uint64_t		name_hash,
		   	off_t			offset,
		   	uint32_t		checksum,
		   	size_t			size,
		   	byte*			src_data)
{
	
	if (strstr (fname, "ycsb") == NULL  
			) {
		return PMEM_ERROR;
	}

	if (pmw->pbuf->page_size < size){
		//printf("XXXXX PMEM_INFO: in pm_buf_write_with_flusher(), write size %zu exceed the max size %zu, file %s offset %zu \n", size, pmw->pbuf->page_size, fname, offset);
		return PMEM_ERROR;
	}	

	if (offset == 0) {
		return pm_buf_write_first_page(pmw, fname, name_hash, offset, checksum, size, src_data);

	}

	WT_SESSION_IMPL *session = pmw->session;
	PMEM_BUF*		buf = pmw->pbuf;
	PMEMobjpool*	pop = pmw->pop;
	//bool is_lock_free_block = false;
	//bool is_lock_free_list = false;
	bool is_safe_check = false;

	ulint hashed;
	ulint i;

	TOID(PMEM_BUF_BLOCK_LIST) hash_list;
	PMEM_BUF_BLOCK_LIST* phashlist;

	//TOID(PMEM_BUF_BLOCK) free_block;
	PMEM_BUF_BLOCK* pfree_block;
	byte* pdata;
	//page_id_t page_id;
	size_t page_size;

	assert(buf);
	assert(src_data);
#if defined (UNIV_PMEMOBJ_BUF_DEBUG)
	//assert (pm_check_io(src_data, page_id) );
#endif 
	//NOTE: this size may not fixed in WT
	page_size = size;

#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	PMEM_LESS_BUCKET_HASH_KEY(hashed, fname, offset);
#else //EVEN_BUCKET
	//TODO: implement fold() in MongoDB
	
	PMEM_HASH_KEY(hashed, offset, name_hash, pmw->PMEM_BUF_MAX_RANGE, pmw->PMEM_N_BUCKETS);
	//hashed = ((offset / 4096) +  (name_hash << 20)) % pmw->PMEM_N_BUCKETS;
#endif

retry:
	//the safe check
	if (is_safe_check){
		if (D_RO(D_RO(buf->buckets)[hashed])->state != PMEM_LIST_FILLING && 
			D_RO(D_RO(buf->buckets)[hashed])->state == PMEM_LIST_FREE ) {
			//os_event_wait(buf->flush_events[hashed]);
			__wt_cond_wait(session, buf->flush_conds[hashed], 0, NULL);
			goto retry;
		}
	}
	
	TOID_ASSIGN(hash_list, (D_RW(buf->buckets)[hashed]).oid);
	phashlist = D_RW(hash_list);
	assert(phashlist);

	pmemobj_rwlock_wrlock(pop, &phashlist->lock);
	//printf("BEGIN pm_write plist %zu with state %d\n", phashlist->list_id, phashlist->state);

	//double check
	//if (phashlist->is_flush) {
	if (phashlist->state != PMEM_LIST_FILLING && 
			phashlist->state != PMEM_LIST_FREE) {
		//When I was blocked (due to mutex) this list is non-flush. When I acquire the lock, it becomes flushing

/* NOTE for recovery 
 *
 * Don't call pm_buf_handle_full_hashed_list before  innodb apply log records that re load spaces (MLOG_FILE_NAME)
 * Hence, we call pm_buf_handle_full_hashed_list only inside pm_buf_write and after the 
 * recv_recovery_from_checkpoint_finish() (through pm_buf_resume_flushing)
 * */
		if (buf->is_recovery	&&
			(phashlist->cur_pages >= phashlist->max_pages * pmw->PMEM_BUF_FLUSH_PCT)) {
			pmemobj_rwlock_unlock(pop, &phashlist->lock);
			//printf("PMEM_INFO: call pm_buf_handle_full_hashed_list during recovering, hashed = %zu list_id = %zu, cur_pages= %zu, is_flush = %d...\n", hashed, phashlist->list_id, phashlist->cur_pages, phashlist->is_flush);
			pm_buf_handle_full_hashed_list(pmw, hashed);
			goto retry;
		}
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
//		printf("\n wait for list %zu hashed_id %zu cur_pages = %zu max_pages= %zu flushing....",
//		phashlist->list_id, phashlist->hashed_id,  phashlist->cur_pages, phashlist->max_pages);
#endif 
		
		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		//os_event_wait(buf->flush_events[hashed]);
		__wt_cond_wait(session, buf->flush_conds[hashed], 0, NULL);

		goto retry;
	}
	//Now the list is non-flush and I have acquired the lock. Let's do my work
	pdata = buf->p_align;

	//DEBUG for checksum error when overwrite the old image on disk
	WT_BLOCK_HEADER *blk, swap;	
	uint32_t page_checksum;
	uint32_t page_checksum_prev;
	uint32_t blk_checksum;
	
	page_checksum_prev = __wt_checksum(src_data, size);
	blk = WT_BLOCK_HEADER_REF(src_data);
	__wt_block_header_byteswap_copy(blk, &swap);

	blk_checksum = swap.checksum;
	page_checksum = __wt_checksum((void*)src_data, size);

	//printf("======== DAT in pmem_buf_write, file %s offset %zu checksum %u page_checksum_prev %u blk_checksum %u page_checksum %u size %zu hash_list id %zu hashed %zu \n ", fname, offset, checksum, page_checksum_prev, blk_checksum, page_checksum, size, phashlist->list_id, hashed);

	//(1) search in the hashed list for a first FREE block to write on 
	for (i = 0; i < phashlist->max_pages; i++) {
		pfree_block = D_RW(D_RW(phashlist->arr)[i]);

		if (pfree_block->state == PMEM_FREE_BLOCK) {
			//found!
			//if(is_lock_free_block)
			//pmemobj_rwlock_wrlock(pop, &pfree_block->lock);
			break;	
		}
		else if(pfree_block->state == PMEM_IN_USED_BLOCK) {
			if (pfree_block->disk_off == offset &&
					pfree_block->name_hash ==  name_hash) {
				//overwrite the old page
				//if(is_lock_free_block)
				//pmemobj_rwlock_wrlock(pop, &pfree_block->lock);
				pmemobj_memset_persist(pop, pdata + pfree_block->pmemaddr, 0, pfree_block->max_size); 
				pmemobj_memcpy_persist(pop, pdata + pfree_block->pmemaddr, src_data, page_size); 
#if defined (UNIV_PMEMOBJ_BUF_STAT)
				++buf->bucket_stats[hashed].n_overwrites;
#endif
				//update size is necessary in WT
				if( size != pfree_block->size){
					printf("PMEM_WARN: overwrite on the same offset %zu but differs size old: %zu new: %zu \n", offset, pfree_block->size, size);
				}
				pfree_block->size = size;

				//if(is_lock_free_block)
				//pmemobj_rwlock_unlock(pop, &pfree_block->lock);
				pmemobj_rwlock_unlock(pop, &phashlist->lock);
				return PMEM_SUCCESS;
			}
		}
		//next block
	}

	if ( i == phashlist->max_pages ) {
		//ALl blocks in this hash_list are either non-fre or locked
		//This is rarely happen but we still deal with it
		//pmemobj_rwlock_unlock(pop, &D_RW(D_RW(buf->buckets)[hashed])->lock);
		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		//os_event_wait(buf->flush_events[hashed]);
		 __wt_cond_wait(session, buf->flush_conds[hashed], 0, NULL);

	printf("\n\n    PMEM_DEBUG: in pmem_buf_write,  hash_list id %zu no free or non-block pages retry \n ", phashlist->list_id);
		goto retry;
	}
#if defined(UNIV_PMEMOBJ_BUF_DEBUG)
	printf("========   PMEM_DEBUG: in pmem_buf_write, the write on file %s offset %zu size %zu hash_list id %zu \n ", fname, offset, size, phashlist->list_id);
#endif	
	// (2) At this point, we get the free and un-blocked block, write data to this block
#if defined (UNIV_PMEMOBJ_BUF_PARTITION_STAT)
	pmemobj_rwlock_wrlock(pop, &buf->filemap->lock);
	pm_filemap_update_items(buf, fname, offset, hashed, PMEM_BUCKET_SIZE);
	pmemobj_rwlock_unlock(pop, &buf->filemap->lock);
#endif 
	
	pfree_block->disk_off = offset;
	strcpy(pfree_block->file_name, fname);
	pfree_block->name_hash = name_hash;
	//update size is necessary in WT
	pfree_block->size = size;

	//In WT, the on-disk page sizes are various	
	//In current version, we just allocate the maximum size (take the simple apporach for the trade-off of waste PM)
	//assert(pfree_block->size == size);
	assert(pfree_block->state == PMEM_FREE_BLOCK);
	pfree_block->state = PMEM_IN_USED_BLOCK;

	//this set is useful for the first write (PMEM_LIST_FREE -> PMEM_LIST_FILLING)
	phashlist->state = PMEM_LIST_FILLING;

	pmemobj_memset_persist(pop, pdata + pfree_block->pmemaddr, 0, pfree_block->max_size); 
	pmemobj_memcpy_persist(pop, pdata + pfree_block->pmemaddr, src_data, size); 

	//if(is_lock_free_block)
	//pmemobj_rwlock_unlock(pop, &pfree_block->lock);

#if defined (UNIV_PMEMOBJ_BUF_STAT)
	++buf->bucket_stats[hashed].n_writes;
#endif 
	//handle hash_list, 

	++(phashlist->cur_pages);

// HANDLE FULL LIST ////////////////////////////////////////////////////////////
	if (phashlist->cur_pages >= phashlist->max_pages * pmw->PMEM_BUF_FLUSH_PCT) {
		//(3) The hashlist is (nearly) full, flush it and assign a free list 
		phashlist->hashed_id = hashed;
		phashlist->state = PMEM_LIST_PREP_PROP;
		//block upcomming writes into this bucket
		//os_event_reset(buf->flush_events[hashed]);
		
#if defined (UNIV_PMEMOBJ_BUF_STAT)
	++buf->bucket_stats[hashed].n_flushed_lists;
#endif 

		pmemobj_rwlock_unlock(pop, &phashlist->lock);
#if defined(UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("\n[1] BEGIN pm_buf_handle_full list_id %zu, hashed_id %zu\n", phashlist->list_id, hashed);
#endif 
		////////// THIS FUNCTION IS IMPORTANT //////////////
		pm_buf_handle_full_hashed_list(pmw, hashed);
		////////////////////////////////////////////////////
#if defined(UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("\n[1] END pm_buf_handle_full list_id %zu, hashed_id %zu\n", phashlist->list_id, hashed);
#endif 

		//unblock the upcomming writes on this bucket
		//os_event_set(buf->flush_events[hashed]);
		__wt_cond_signal(session,buf->flush_conds[hashed]);
	}
	else {
		//unlock the hashed list
		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		//pmemobj_rwlock_unlock(pop, &D_RW(D_RW(buf->buckets)[hashed])->lock);
	}

	return PMEM_SUCCESS;
}

/*
 * write page flush from DRAM to NVM
 * similar with pm_buf_write_with_flusher but invalid overlap range
 * new write (offset1, size1)
 * OVERLAP condition: If exist (offset2, size2) in the current list that:
 * (offset1 <= offset2 && offset2 < offset1 + size1) OR
 * (offset1 >= offset2 && offset1 < offset2 + size2)
 * */
int
pm_buf_write_with_flusher(
		   	PMEM_WRAPPER*	pmw,
			const char*		fname,
		   	uint64_t		name_hash,
		   	off_t			offset,
		   	uint32_t		checksum,
		   	size_t			size,
		   	byte*			src_data)
{
	
	if (strstr (fname, "ycsb") == NULL  
			) {
		return PMEM_ERROR;
	}


	if (offset == 0) {
		return pm_buf_write_first_page(pmw, fname, name_hash, offset, checksum, size, src_data);

	}

	WT_SESSION_IMPL *session = pmw->session;
	PMEM_BUF*		buf = pmw->pbuf;
	PMEMobjpool*	pop = pmw->pop;

	ulint hashed;
	ulint i;

	TOID(PMEM_BUF_BLOCK_LIST) hash_list;
	PMEM_BUF_BLOCK_LIST* phashlist;

	PMEM_BUF_BLOCK* pfree_block;
	byte* pdata;
	//page_id_t page_id;
	size_t page_size;

	assert(buf);
	assert(src_data);
	//NOTE: this size may not fixed in WT
	page_size = size;

	if (pmw->pbuf->page_size < size){
		//We only capture the first ckpt
		if(pmw->pbuf->is_capture_ckpt){
			printf("PMEM_WARN 1: WRITE ON NVM OVERSIZE write, offset %zu checksum %u size %zu \n", offset, checksum, size);
			pmw->pbuf->is_capture_ckpt = false;

			pdata = buf->p_align;
			PMEM_BUF_BLOCK* pckpt_block;
			pckpt_block = D_RW(pmw->pbuf->ckpt_block);

			pckpt_block->checksum = checksum;

			pckpt_block->disk_off = offset;
			strcpy(pckpt_block->file_name, fname);
			pckpt_block->name_hash = name_hash;
			pckpt_block->size = size;
			pckpt_block->state = PMEM_IN_USED_BLOCK;

			pmemobj_memset_persist(pop, pdata + pckpt_block->pmemaddr, 0, pckpt_block->max_size); 
			pmemobj_memcpy_persist(pop, pdata + pckpt_block->pmemaddr, src_data, size); 

			return PMEM_SUCCESS;
		}
		else {
			printf("PMEM_WARN 2: WRITE ON DISK OVERSIZE write, offset %zu checksum %u size %zu \n", offset, checksum, size);
			return PMEM_ERROR;
		}
	}	

#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	PMEM_LESS_BUCKET_HASH_KEY(hashed, fname, offset);
#else //EVEN_BUCKET
	
	PMEM_HASH_KEY(hashed, offset, name_hash, pmw->PMEM_BUF_MAX_RANGE, pmw->PMEM_N_BUCKETS);
#endif

	//Why new implement: 
	//Old implement problems: 
	//+ lock on phashlist not the bucket
	//+ Complicated check multi writer on the same bucket
	//New implement:
	//+ Use the bucket's locks
	//+ Once a write acquire a bucket's lock, it handle the write as the old method
	//+ When the list is full, replace free list first
	//+ Then release the lock
	//+ Then handle full list without affect to the new free list
	
	//(1) Acquire the bucket's lock
	//IMPORTANT: ensure only this thread can access the phashlist
	__wt_spin_lock(session, &buf->bucket_locks[hashed]);
	//After acquire the lock, the hashlist only has two state 	
	TOID_ASSIGN(hash_list, (D_RW(buf->buckets)[hashed]).oid);
	phashlist = D_RW(hash_list);
	assert(phashlist);	

	if (phashlist->state != PMEM_LIST_FILLING && 
			phashlist->state != PMEM_LIST_FREE) {
		printf("PMEM_ERROR, state ERROR in pm_buf_write_with_flusher() \n");
		assert(0);
	}

	pdata = buf->p_align;

#if defined (CHECKSUM_DEBUG)
	//DEBUG for checksum error when overwrite the old image on disk
	WT_BLOCK_HEADER *blk, swap;	
	uint32_t page_checksum;
	uint32_t page_checksum_prev;
	uint32_t blk_checksum;
	
	page_checksum_prev = __wt_checksum(src_data, size);
	blk = WT_BLOCK_HEADER_REF(src_data);
	__wt_block_header_byteswap_copy(blk, &swap);

	blk_checksum = swap.checksum;
	page_checksum = __wt_checksum((void*)src_data, size);

	printf("======== DAT in pmem_buf_write, file %s offset %zu checksum %u page_checksum_prev %u blk_checksum %u page_checksum %u size %zu hash_list id %zu hashed %zu \n ", fname, offset, checksum, page_checksum_prev, blk_checksum, page_checksum, size, phashlist->list_id, hashed);
#endif
	// (2) search for the FREE block to write on, if overlap occur, invalid previous  write 
	ulint i_free = 0;
	bool is_first_free = true;
	PMEM_BUF_BLOCK* pcurblock;

	for (i = 0; i < phashlist->max_pages; i++) {
		pcurblock = D_RW(D_RW(phashlist->arr)[i]);
		if(pcurblock->state == PMEM_IN_USED_BLOCK){
			//Check for the overlap
			if  ( (name_hash == pcurblock->name_hash) &&
				  ((offset <= pcurblock->disk_off &&
				  pcurblock->disk_off < offset + (off_t) size) ||
				   (offset >= pcurblock->disk_off && 
				  offset < pcurblock->disk_off + (off_t) pcurblock->size))
				  ){
#if defined (CHECKSUM_DEBUG)
				printf("=== OVERLAP at index %zu of list %zu hash_id %zu file %s offset %zu size %zu checksum %u pre_off %zu pre_size %zu pre_checksum %u \n",
						i, phashlist->list_id, hashed, fname, offset, size, checksum, pcurblock->disk_off, pcurblock->size, pcurblock->checksum);
#endif
				//overlapse, invalid the old write
				pcurblock->state = PMEM_FREE_BLOCK;
				pmemobj_memset_persist(pop, pdata + pcurblock->pmemaddr, 0, pcurblock->size); 
				pcurblock->size = 0;
				pcurblock->checksum = 0;

				--(phashlist->cur_pages);
			} 
			// no overlapse, just skip
		}
		else if (pcurblock->state == PMEM_FREE_BLOCK &&
				is_first_free){
			i_free = i; // save the index of the first free block
			is_first_free = false;
		}
	} //end for loop of merge range
	
	// (3) At this point, we get the free block, write data to this block
	pfree_block = D_RW(D_RW(phashlist->arr)[i_free]);

	assert(pfree_block);
	assert(pfree_block->state == PMEM_FREE_BLOCK);

	pfree_block->checksum = checksum;

	pfree_block->disk_off = offset;
	strcpy(pfree_block->file_name, fname);
	pfree_block->name_hash = name_hash;
	pfree_block->size = size;
	pfree_block->state = PMEM_IN_USED_BLOCK;

	phashlist->state = PMEM_LIST_FILLING;
	
#if defined (CHECKSUM_DEBUG)
	//update the max_offset
	if (buf->max_offset < offset)
		buf->max_offset = offset;
#endif

	pmemobj_memset_persist(pop, pdata + pfree_block->pmemaddr, 0, pfree_block->max_size); 
	pmemobj_memcpy_persist(pop, pdata + pfree_block->pmemaddr, src_data, size); 

#if defined (UNIV_PMEMOBJ_BUF_STAT)
	++buf->bucket_stats[hashed].n_writes;
#endif 
	//handle hash_list, 

	++(phashlist->cur_pages);

// HANDLE FULL LIST ////////////////////////////////////////////////////////////
	if (phashlist->cur_pages >= phashlist->max_pages * pmw->PMEM_BUF_FLUSH_PCT) {
		/*    (4) Handle free list*/
get_free_list:
		//Get a free list from the free pool
		pmemobj_rwlock_wrlock(pop, &(D_RW(buf->free_pool)->lock));

		TOID(PMEM_BUF_BLOCK_LIST) first_list = POBJ_LIST_FIRST (&(D_RW(buf->free_pool)->head));
		if (D_RW(buf->free_pool)->cur_lists == 0 ||
				TOID_IS_NULL(first_list) ) {
			pthread_t tid;
			tid = pthread_self();
			printf("PMEM_INFO: thread %zu free_pool->cur_lists = %zu, the first list is NULL? %d the free list is empty, sleep then retry..\n", tid, D_RO(buf->free_pool)->cur_lists, TOID_IS_NULL(first_list));
			pmemobj_rwlock_unlock(pop, &(D_RW(buf->free_pool)->lock));
			__wt_cond_wait(session, buf->free_pool_cond, 0, NULL);

			goto get_free_list;
		}
		//Get the free list at HEAD of the free pool
		POBJ_LIST_REMOVE(pop, &D_RW(buf->free_pool)->head, first_list, list_entries);
		D_RW(buf->free_pool)->cur_lists--;
		pmemobj_rwlock_unlock(pop, &(D_RW(buf->free_pool)->lock));

		assert(!TOID_IS_NULL(first_list));

		D_RW(first_list)->hashed_id = hashed;

		//Asign linked-list refs, swap the first_list with the hash_list
		TOID_ASSIGN(D_RW(buf->buckets)[hashed], first_list.oid);
		TOID_ASSIGN( D_RW(first_list)->next_list, hash_list.oid);
		TOID_ASSIGN( D_RW(hash_list)->prev_list, first_list.oid);
		//Right after the swaping finish, we can release the bucket's lock
		__wt_spin_unlock(session, &pmw->pbuf->bucket_locks[hashed]);

		/*    (5) assign the phashlist to the Flusher*/
		pm_buf_assign_flusher(pmw, phashlist);
	}//end if handle full list
	else {
		__wt_spin_unlock(session, &pmw->pbuf->bucket_locks[hashed]);
	}

	return PMEM_SUCCESS;
}

/*
 * This version is for MongoDB
 * hash by range
 * overlap range bug remain
 * */
int
pm_buf_write_with_flusher_old1(
		   	PMEM_WRAPPER*	pmw,
			const char*		fname,
		   	uint64_t		name_hash,
		   	off_t			offset,
		   	uint32_t		checksum,
		   	size_t			size,
		   	byte*			src_data)
{
	
	if (strstr (fname, "ycsb") == NULL  
			) {
		return PMEM_ERROR;
	}

	if (pmw->pbuf->page_size < size){
		printf("PMEM_WARN: DAT OVERSIZE write, offset %zu checksum %u size %zu \n", offset, checksum, size);
		return PMEM_ERROR;
	}	

	if (offset == 0) {
		return pm_buf_write_first_page(pmw, fname, name_hash, offset, checksum, size, src_data);

	}

	WT_SESSION_IMPL *session = pmw->session;
	PMEM_BUF*		buf = pmw->pbuf;
	PMEMobjpool*	pop = pmw->pop;

	ulint hashed;
	ulint i;

	TOID(PMEM_BUF_BLOCK_LIST) hash_list;
	PMEM_BUF_BLOCK_LIST* phashlist;

	PMEM_BUF_BLOCK* pfree_block;
	byte* pdata;
	//page_id_t page_id;
	size_t page_size;

	assert(buf);
	assert(src_data);
	//NOTE: this size may not fixed in WT
	page_size = size;

#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	PMEM_LESS_BUCKET_HASH_KEY(hashed, fname, offset);
#else //EVEN_BUCKET
	
	PMEM_HASH_KEY(hashed, offset, name_hash, pmw->PMEM_BUF_MAX_RANGE, pmw->PMEM_N_BUCKETS);
#endif

	//Why new implement: 
	//Old implement problems: 
	//+ lock on phashlist not the bucket
	//+ Complicated check multi writer on the same bucket
	//New implement:
	//+ Use the bucket's locks
	//+ Once a write acquire a bucket's lock, it handle the write as the old method
	//+ When the list is full, replace free list first
	//+ Then release the lock
	//+ Then handle full list without affect to the new free list
	
	//(1) Acquire the bucket's lock
	//IMPORTANT: ensure only this thread can access the phashlist
	__wt_spin_lock(session, &buf->bucket_locks[hashed]);
	//After acquire the lock, the hashlist only has two state 	
	TOID_ASSIGN(hash_list, (D_RW(buf->buckets)[hashed]).oid);
	phashlist = D_RW(hash_list);
	assert(phashlist);	

	if (phashlist->state != PMEM_LIST_FILLING && 
			phashlist->state != PMEM_LIST_FREE) {
		printf("PMEM_ERROR, state ERROR in pm_buf_write_with_flusher() \n");
		assert(0);
	}

	pdata = buf->p_align;

#if defined (CHECKSUM_DEBUG)
	//DEBUG for checksum error when overwrite the old image on disk
	WT_BLOCK_HEADER *blk, swap;	
	uint32_t page_checksum;
	uint32_t page_checksum_prev;
	uint32_t blk_checksum;
	
	page_checksum_prev = __wt_checksum(src_data, size);
	blk = WT_BLOCK_HEADER_REF(src_data);
	__wt_block_header_byteswap_copy(blk, &swap);

	blk_checksum = swap.checksum;
	page_checksum = __wt_checksum((void*)src_data, size);

	printf("======== DAT in pmem_buf_write, file %s offset %zu checksum %u page_checksum_prev %u blk_checksum %u page_checksum %u size %zu hash_list id %zu hashed %zu \n ", fname, offset, checksum, page_checksum_prev, blk_checksum, page_checksum, size, phashlist->list_id, hashed);
#endif

	//(2) search in the hashed list for a first FREE block to write on 
	pfree_block = NULL;
	for (i = 0; i < phashlist->max_pages; i++) {

		pfree_block = D_RW(D_RW(phashlist->arr)[i]);
		if (pfree_block->state == PMEM_FREE_BLOCK) {
			//found!
			break;	
		}
		else if(pfree_block->state == PMEM_IN_USED_BLOCK) {
			if (pfree_block->disk_off == offset &&
					pfree_block->name_hash ==  name_hash) {
				//overwrite the old page
				pmemobj_memset_persist(pop, pdata + pfree_block->pmemaddr, 0, pfree_block->max_size); 
				pmemobj_memcpy_persist(pop, pdata + pfree_block->pmemaddr, src_data, page_size); 
#if defined (UNIV_PMEMOBJ_BUF_STAT)
				++buf->bucket_stats[hashed].n_overwrites;
#endif
				//update size is necessary in WT
				if( size != pfree_block->size){
					printf("PMEM_WARN: overwrite on the same offset %zu but differs size old: %zu new: %zu \n", offset, pfree_block->size, size);
				}
				pfree_block->size = size;
				pfree_block->checksum = checksum;

				__wt_spin_unlock(session, &buf->bucket_locks[hashed]);
				return PMEM_SUCCESS;
			}
		}
		//next block
	}//end for search for the first FREE block

	if ( i == phashlist->max_pages ) {
		//the current list is full
		printf("PMEM_ERROR: logical error in pm_buf_write()\n");
		assert(0);
	}

	// (3) At this point, we get the free block, write data to this block
	assert(pfree_block);
	assert(pfree_block->state == PMEM_FREE_BLOCK);
	
	pfree_block->checksum = checksum;

	pfree_block->disk_off = offset;
	strcpy(pfree_block->file_name, fname);
	pfree_block->name_hash = name_hash;
	pfree_block->size = size;
	pfree_block->state = PMEM_IN_USED_BLOCK;

	//this set is useful for the first write (PMEM_LIST_FREE -> PMEM_LIST_FILLING)
	phashlist->state = PMEM_LIST_FILLING;
	
#if defined (CHECKSUM_DEBUG)
	//update the max_offset
	if (buf->max_offset < offset)
		buf->max_offset = offset;
#endif

	pmemobj_memset_persist(pop, pdata + pfree_block->pmemaddr, 0, pfree_block->max_size); 
	pmemobj_memcpy_persist(pop, pdata + pfree_block->pmemaddr, src_data, size); 

#if defined (UNIV_PMEMOBJ_BUF_STAT)
	++buf->bucket_stats[hashed].n_writes;
#endif 
	//handle hash_list, 

	++(phashlist->cur_pages);

// HANDLE FULL LIST ////////////////////////////////////////////////////////////
	if (phashlist->cur_pages >= phashlist->max_pages * pmw->PMEM_BUF_FLUSH_PCT) {
		/*    (4) Handle free list*/
get_free_list:
		//Get a free list from the free pool
		pmemobj_rwlock_wrlock(pop, &(D_RW(buf->free_pool)->lock));

		TOID(PMEM_BUF_BLOCK_LIST) first_list = POBJ_LIST_FIRST (&(D_RW(buf->free_pool)->head));
		if (D_RW(buf->free_pool)->cur_lists == 0 ||
				TOID_IS_NULL(first_list) ) {
			pthread_t tid;
			tid = pthread_self();
			printf("PMEM_INFO: thread %zu free_pool->cur_lists = %zu, the first list is NULL? %d the free list is empty, sleep then retry..\n", tid, D_RO(buf->free_pool)->cur_lists, TOID_IS_NULL(first_list));
			pmemobj_rwlock_unlock(pop, &(D_RW(buf->free_pool)->lock));
			__wt_cond_wait(session, buf->free_pool_cond, 0, NULL);

			goto get_free_list;
		}
		//Get the free list at HEAD of the free pool
		POBJ_LIST_REMOVE(pop, &D_RW(buf->free_pool)->head, first_list, list_entries);
		D_RW(buf->free_pool)->cur_lists--;
		pmemobj_rwlock_unlock(pop, &(D_RW(buf->free_pool)->lock));

		assert(!TOID_IS_NULL(first_list));

		D_RW(first_list)->hashed_id = hashed;

		//Asign linked-list refs, swap the first_list with the hash_list
		TOID_ASSIGN(D_RW(buf->buckets)[hashed], first_list.oid);
		TOID_ASSIGN( D_RW(first_list)->next_list, hash_list.oid);
		TOID_ASSIGN( D_RW(hash_list)->prev_list, first_list.oid);
		//Right after the swaping finish, we can release the bucket's lock
		__wt_spin_unlock(session, &pmw->pbuf->bucket_locks[hashed]);

		/*    (5) assign the phashlist to the Flusher*/
		pm_buf_assign_flusher(pmw, phashlist);
	}//end if handle full list
	else {
		__wt_spin_unlock(session, &pmw->pbuf->bucket_locks[hashed]);
	}

	return PMEM_SUCCESS;
}

int
pm_buf_write_first_page(
		PMEM_WRAPPER*	pmw,
		const char*		fname,
		uint64_t		name_hash,
		off_t			offset,
		uint32_t		checksum,
		size_t			size,
		byte*			src_data){

	printf("pm_buf_write offset 0 in file %s size %zu\n", fname, size);
	ulint i;
	PMEMobjpool*	pop = pmw->pop;
	PMEM_BUF_BLOCK_LIST* pspec_list;
	PMEM_BUF_BLOCK*		pspec_block;

	byte* pdata;
	size_t page_size = size;

	PMEM_BUF*		buf = pmw->pbuf;
	pspec_list = D_RW(buf->spec_list); 

	pmemobj_rwlock_wrlock(pop, &pspec_list->lock);

	pdata = buf->p_align;
	//scan in the special list
	for (i = 0; i < pspec_list->cur_pages; i++){
		pspec_block = D_RW(D_RW(pspec_list->arr)[i]);

		if (pspec_block->state == PMEM_FREE_BLOCK){
			//write the new metadata block in the special list
			break;
		}	
		else if (pspec_block->state == PMEM_IN_USED_BLOCK) {
			if (pspec_block->disk_off == offset && 
					pspec_block->name_hash == name_hash) {
				//overwrite this spec block
				pmemobj_memset_persist(pop, pdata + pspec_block->pmemaddr, 0, pspec_block->max_size); 
				pmemobj_memcpy_persist(pop, pdata + pspec_block->pmemaddr, src_data, page_size); 
				//update the file_name, page_id in case of tmp space
				strcpy(pspec_block->file_name, fname);
				pspec_block->disk_off = offset;
				//update size is necessary in WT
				if( size != pspec_block->size){
					printf("PMEM_WARN: write on the same offset %zu but differs size old: %zu new: %zu \n", offset, pspec_block->size, size);
				}
				pspec_block->size = size;

				pmemobj_rwlock_unlock(pop, &pspec_list->lock);

				return PMEM_SUCCESS;
			}
			//else: just skip this block
		}
		//next block
	}//end for

	if (i < pspec_list->cur_pages) {
		printf("PMEM_BUF Logical error when handle the special list\n");
		assert(0);
	}
	//write the new metadata block in the special list
	if (i == pspec_list->cur_pages) {
		pspec_block = D_RW(D_RW(pspec_list->arr)[i]);
		//add new block to the spec list
		pspec_block->disk_off = offset;
		//update size is necessary in WT
		if( size != pspec_block->size){
			printf("PMEM_WARN: write on the same offset %zu but differs size old: %zu new: %zu \n", offset, pspec_block->size, size);
		}
		pspec_block->size = size;

		pspec_block->state = PMEM_IN_USED_BLOCK;
		strcpy(pspec_block->file_name, fname);
		pspec_block->name_hash = name_hash;
		pmemobj_memset_persist(pop, pdata + pspec_block->pmemaddr, 0, pspec_block->max_size); 
		pmemobj_memcpy_persist(pop, pdata + pspec_block->pmemaddr, src_data, page_size); 
		++(pspec_list->cur_pages);

		printf("Add new block to the spec list, file %s cur_pages %zu \n", fname,  pspec_list->cur_pages);

		//We do not handle flushing the spec list here
		if (pspec_list->cur_pages >= pspec_list->max_pages * pmw->PMEM_BUF_FLUSH_PCT) {
			printf("We do not handle flushing spec list in this version, adjust the input params to get larger size of spec list\n");
			assert(0);
		}
	}
	pmemobj_rwlock_unlock(pop, &pspec_list->lock);
	return PMEM_SUCCESS;
}

/////////////////////////// pm_buf_flush_list versions///
/*  VERSION 0 (BATCH)
 * Async write pages from the list to datafile
 * The caller thread need to lock/unlock the plist
 * See buf_dblwr_write_block_to_datafile
 *	@param[in] pop		PMEMobjpool pointer
 *	@param[in] buf		PMEM_BUF pointer
 *	@param[in] plist	pointer to the list that will be flushed 
 * */
void
pm_buf_flush_list(
		PMEM_WRAPPER*				pmw,
	   	PMEM_BUF_BLOCK_LIST*	plist) {


// (A) ASYNC write version
	int ret;
	ret = pm_process_batch(pmw, plist);
	if (ret != PMEM_SUCCESS){
		printf("pm_process_batch() return errors\n");
		assert(0);
	}

/*
// (B) SYNC write version
	//We need to repeat this write
//__wt_write(session, fh, offset, align_size, buf->mem)
// use __handle_search() to get the WT_FH from the file name
	
	uint32_t flags, i, fh_i, fh_cnt;
	int ret;
	byte* pdata;
	void*			buf;

	WT_FH ** fh_arr;
	WT_FH* fh_cur;

	PMEM_BUF_BLOCK* pblock;
	PMEM_BUF*		pmem_buf = pmw->pbuf;
	WT_SESSION_IMPL *session = pmw->session;

	assert(pmem_buf);
	
	//the root address 
	pdata = pmem_buf->p_align;
	flags = 0;
	fh_arr = (WT_FH**) calloc(plist->max_pages, sizeof(WT_FH*));
	//for (fh_i = 0; fh_i < plist->max_pages; fh_i++) {
	//	fh_arr[fh_i] = (WT_FH*) malloc(sizeof(WT_FH));
	//}

	fh_cnt = 0;

	// write each block in the plist to disk using __wt_write()
	for (i = 0; i < plist->max_pages; ++i) {
		pblock = D_RW(D_RW(plist->arr)[i]);

		buf = pdata + pblock->pmemaddr;

		if (pblock->state == PMEM_FREE_BLOCK) {
			continue;
		}
		assert( pblock->pmemaddr < pmem_buf->size);
		//Change the state of the block
		pblock->state = PMEM_IN_FLUSH_BLOCK;
		//(1) Open the file to get the fh
		ret =__wt_open(session, pblock->file_name, WT_FS_OPEN_FILE_TYPE_DATA, flags, &fh_cur);
		assert(!ret);
		ret = __wt_write(session, fh_cur, pblock->disk_off, pblock->size, buf);
		if (ret != 0){
			printf("Inside pm_buf_flush_list(), __wt_write() has errror\n Detail: file %s offset %zu size %zu\n", fh_cur->name, pblock->disk_off, pblock->size);
			exit(0);
		}

		//Add the file handler to the list for flushing 
		for (fh_i = 0; fh_i < fh_cnt; ++fh_i) {
			if (fh_arr[fh_i]->name_hash == fh_cur->name_hash){
				//this file is existed in the fh_arr list, do not add it	
				break;
			}
		}
		if (fh_i == fh_cnt){
			//add new entry in the fh_arr list
			fh_arr[fh_i] = fh_cur;
			fh_cnt++;
		}
	}//end for
	//(2) Since the write is sync, at this point the write is finish
	pm_handle_finished_list_with_flusher(pmw, plist);
	//(3) flush
	for (fh_i = 0; fh_i < fh_cnt; ++fh_i) {
		//printf("PMEM_DEBUG: propagate and fsync file %s\n", fh_arr[fh_i]->name);
		__wt_fsync(session, fh_arr[fh_i], true);
		if (fh_arr[fh_i] != NULL){
			fh_arr[fh_i] = NULL;
		}
	}

	//(4) Free the memory
	free (fh_arr);
	fh_arr = NULL;
*/ // end (B) SYNC write version	
}


/*
 *This function is called from aio complete (fil_aio_wait)
 (1) Reset the list
 (2) Flush spaces in this list
 * */
void
pm_handle_finished_block(
	   	PMEM_WRAPPER*			pmw,
	   	PMEM_BUF_BLOCK*		pblock)
{

	WT_SESSION_IMPL *session = pmw->session;
	PMEMobjpool*		pop = pmw->pop;
	PMEM_BUF*			buf = pmw->pbuf;
	//bool is_lock_prev_list = false;

	//(1) handle the flush_list
	TOID(PMEM_BUF_BLOCK_LIST) flush_list;

	TOID_ASSIGN(flush_list, pblock->list.oid);
	PMEM_BUF_BLOCK_LIST* pflush_list = D_RW(flush_list);

	assert(pflush_list);
	
	//possible owners: producer (pm_buf_write), consummer (this function) threads
	pmemobj_rwlock_wrlock(pop, &pflush_list->lock);
	
	if(pblock->sync)
		pflush_list->n_sio_pending--;
	else
		pflush_list->n_aio_pending--;

	//printf("PMEM_DEBUG aio FINISHED slot_id = %zu n_page_requested = %zu flush_list id = %zu n_pending = %zu/%zu page_id %zu space_id %zu \n",
	//if (pflush_list->n_aio_pending == 0) {
	if (pflush_list->n_aio_pending + pflush_list->n_sio_pending == 0) {
		//Now all pages in this list are persistent in disk
		//(0) flush spaces
		//TODO: Research flush in MongoDB
		//pm_buf_flush_spaces_in_list(pop, buf, pflush_list);

		//(1) Reset blocks in the list
		ulint i;
		for (i = 0; i < pflush_list->max_pages; i++) {
			D_RW(D_RW(pflush_list->arr)[i])->state = PMEM_FREE_BLOCK;
			D_RW(D_RW(pflush_list->arr)[i])->sync = false;
		}

		pflush_list->cur_pages = 0;
		pflush_list->state = PMEM_LIST_FREE;
		pflush_list->hashed_id = PMEM_ID_NONE;
		
		// (2) Remove this list from the doubled-linked list
		
		//assert( !TOID_IS_NULL(pflush_list->prev_list) );
		if( !TOID_IS_NULL(pflush_list->prev_list) ) {

			//if (is_lock_prev_list)
			//pmemobj_rwlock_wrlock(pop, &D_RW(pflush_list->prev_list)->lock);
			TOID_ASSIGN( D_RW(pflush_list->prev_list)->next_list, pflush_list->next_list.oid);
			//if (is_lock_prev_list)
			//pmemobj_rwlock_unlock(pop, &D_RW(pflush_list->prev_list)->lock);
		}

		if (!TOID_IS_NULL(pflush_list->next_list) ) {
			//pmemobj_rwlock_wrlock(pop, &D_RW(pflush_list->next_list)->lock);

			TOID_ASSIGN(D_RW(pflush_list->next_list)->prev_list, pflush_list->prev_list.oid);

			//pmemobj_rwlock_unlock(pop, &D_RW(pflush_list->next_list)->lock);
		}
		
		TOID_ASSIGN(pflush_list->next_list, OID_NULL);
		TOID_ASSIGN(pflush_list->prev_list, OID_NULL);

		// (3) we return this list to the free_pool
		PMEM_BUF_FREE_POOL* pfree_pool;
		pfree_pool = D_RW(buf->free_pool);

		//printf("PMEM_DEBUG: in fil_aio_wait(), try to lock free_pool list id: %zd, cur_lists in free_pool= %zd \n", pflush_list->list_id, pfree_pool->cur_lists);
		pmemobj_rwlock_wrlock(pop, &pfree_pool->lock);

		POBJ_LIST_INSERT_TAIL(pop, &pfree_pool->head, flush_list, list_entries);
		pfree_pool->cur_lists++;
		//wakeup who is waitting for free_pool available
		//os_event_set(buf->free_pool_event);
		__wt_cond_signal(session,buf->free_pool_cond);

		pmemobj_rwlock_unlock(pop, &pfree_pool->lock);

	}
	//the list has some unfinished aio	
	pmemobj_rwlock_unlock(pop, &pflush_list->lock);
}

/*
 *This function is called from aio complete (fil_aio_wait)
 (1) Reset the list
 (2) Flush spaces in this list
 * */
void
pm_handle_finished_block_no_free_pool(
	   	PMEM_WRAPPER*			pmw,
	   	PMEM_BUF_BLOCK*		pblock)
{

	WT_SESSION_IMPL *session = pmw->session;
	PMEMobjpool*		pop = pmw->pop;
	PMEM_BUF*			buf = pmw->pbuf;
	//bool is_lock_prev_list = false;

	//(1) handle the flush_list
	TOID(PMEM_BUF_BLOCK_LIST) flush_list;

	TOID_ASSIGN(flush_list, pblock->list.oid);
	PMEM_BUF_BLOCK_LIST* pflush_list = D_RW(flush_list);

	assert(pflush_list);
	
	//possible owners: producer (pm_buf_write), consummer (this function) threads
	pmemobj_rwlock_wrlock(pop, &pflush_list->lock);
	
	if(pblock->sync)
		pflush_list->n_sio_pending--;
	else
		pflush_list->n_aio_pending--;

	//printf("PMEM_DEBUG aio FINISHED slot_id = %zu n_page_requested = %zu flush_list id = %zu n_pending = %zu/%zu page_id %zu space_id %zu \n",
	//if (pflush_list->n_aio_pending == 0) {
	if (pflush_list->n_aio_pending + pflush_list->n_sio_pending == 0) {
		//Now all pages in this list are persistent in disk
		//(0) flush spaces
#if defined(UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("finish list %zu hash_id %d \n",
				pflush_list->list_id, pflush_list->hashed_id);
#endif
		//TODO: Research flush in MongoDB
		//pm_buf_flush_spaces_in_list(pop, buf, pflush_list);

		//(1) Reset blocks in the list
		ulint i;
		for (i = 0; i < pflush_list->max_pages; i++) {
			D_RW(D_RW(pflush_list->arr)[i])->state = PMEM_FREE_BLOCK;
			D_RW(D_RW(pflush_list->arr)[i])->sync = false;
		}

		pflush_list->cur_pages = 0;
		pflush_list->state = PMEM_LIST_FREE;

		//os_event_set(buf->flush_events[pflush_list->hashed_id]);
		__wt_cond_signal(session, buf->flush_conds[pflush_list->hashed_id]);

		//pflush_list->hashed_id = PMEM_ID_NONE;

	}
	//the list has some unfinished aio	
	pmemobj_rwlock_unlock(pop, &pflush_list->lock);
}

/*
 * Handle a AIO finished block
 * */
void
pm_handle_asyn_finished_block(
	   	PMEM_WRAPPER*			pmw,
	   	PMEM_BUF_BLOCK*		pblock)
{
	WT_SESSION_IMPL *session = pmw->session;
	PMEMobjpool*		pop = pmw->pop;
	PMEM_BUF*			buf = pmw->pbuf;

	//(1) handle the flush_list
	PMEM_BUF_BLOCK_LIST* pnext_list;
	PMEM_BUF_BLOCK_LIST* pprev_list;
	TOID(PMEM_BUF_BLOCK_LIST) flush_list;

	TOID_ASSIGN(flush_list, pblock->list.oid);
	PMEM_BUF_BLOCK_LIST* pflush_list = D_RW(flush_list);

	assert(pflush_list);

	//possible owners: producer (pm_buf_write), consummer (this function) threads
	pmemobj_rwlock_wrlock(pop, &pflush_list->lock);

	pflush_list->n_aio_pending--;
	//(2) If this is the last block in the list, handle the list also
	if (pflush_list->n_aio_pending + pflush_list->n_sio_pending == 0) {

		//(2.1) Reset blocks in the list
		ulint i;
		for (i = 0; i < pflush_list->max_pages; i++) {
			D_RW(D_RW(pflush_list->arr)[i])->state = PMEM_FREE_BLOCK;
			D_RW(D_RW(pflush_list->arr)[i])->sync = false;
		}

		pflush_list->cur_pages = 0;
		pflush_list->state = PMEM_LIST_FREE;
		pflush_list->hashed_id = PMEM_ID_NONE;

		// (2.2) Remove this list from the doubled-linked list
		pnext_list = D_RW(pflush_list->next_list);
		pprev_list = D_RW(pflush_list->prev_list);

		if (pprev_list != NULL &&
				D_RW(pprev_list->next_list) != NULL &&
				D_RW(pprev_list->next_list)->list_id == pflush_list->list_id){

			if (pnext_list == NULL) {
				TOID_ASSIGN(pprev_list->next_list, OID_NULL);
			}
			else {
				TOID_ASSIGN(pprev_list->next_list, (pflush_list->next_list).oid);
			}
		}

		if (pnext_list != NULL &&
				D_RW(pnext_list->prev_list) != NULL &&
				D_RW(pnext_list->prev_list)->list_id == pflush_list->list_id) {
			if (pprev_list == NULL) {
				TOID_ASSIGN(pnext_list->prev_list, OID_NULL);
			}
			else {
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
				printf("[4] !!!!! handle finish, cur_list_id %zu ",
						pflush_list->list_id);
				printf ("[4] !!!!  has next_list_id %zu ", pnext_list->list_id);
				printf ("[4] !!!! has prev_list_id %zu \n", pprev_list->list_id);
#endif
				TOID_ASSIGN(pnext_list->prev_list, (pflush_list->prev_list).oid);
			}
		}

		TOID_ASSIGN(pflush_list->next_list, OID_NULL);
		TOID_ASSIGN(pflush_list->prev_list, OID_NULL);

		// (2.3) we return this list to the free_pool
		PMEM_BUF_FREE_POOL* pfree_pool;
		pfree_pool = D_RW(buf->free_pool);

		pmemobj_rwlock_wrlock(pop, &pfree_pool->lock);

		POBJ_LIST_INSERT_TAIL(pop, &pfree_pool->head, flush_list, list_entries);
		pfree_pool->cur_lists++;
		//wakeup who is waitting for free_pool available
		//os_event_set(buf->free_pool_event);
		__wt_cond_signal(session, buf->free_pool_cond);

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("\n *****[4] END finish AIO List %zu, cur free pool size %zu]\n", pflush_list->list_id, pfree_pool->cur_lists);
#endif
		pmemobj_rwlock_unlock(pop, &pfree_pool->lock);
	}
	pmemobj_rwlock_unlock(pop, &pflush_list->lock);
}

/*
 * Handle a flush list after all its blocks are finished flushing on disk
 * Use this function for sync progagation
 (1) Reset the list
 (2) Put bach the list to the Free Block Pool
 (3) Remove the related linked lists
 We handle fsync() for blocks in the list in handle_AIO_seg_complete() 
 * */
	void
pm_handle_finished_list_with_flusher(
		PMEM_WRAPPER*		pmw,
		PMEM_BUF_BLOCK_LIST*		pflush_list)
{

	WT_SESSION_IMPL *session = pmw->session;
	PMEMobjpool*		pop = pmw->pop;
	PMEM_BUF*			buf = pmw->pbuf;
	//PMEM_FLUSHER* flusher;

	//(1) handle the flush_list
	PMEM_BUF_BLOCK_LIST* pnext_list;
	PMEM_BUF_BLOCK_LIST* pprev_list;
	TOID(PMEM_BUF_BLOCK_LIST) flush_list;
	PMEM_BUF_BLOCK* pfirst_block = D_RW(D_RW(pflush_list->arr)[0]);

	TOID_ASSIGN(flush_list, pfirst_block->list.oid);
	assert(pflush_list);

	pmemobj_rwlock_wrlock(pop, &pflush_list->lock);

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	printf("\n [*****[6]  BEGIN pm_handle_finished_list_with_flusher list %zu hashed_id %d\n",
			pflush_list->list_id, pflush_list->hashed_id);
#endif
	//Now all pages in this list are persistent in disk

	//(1) Reset blocks in the list
	ulint i;
	for (i = 0; i < pflush_list->max_pages; i++) {
		D_RW(D_RW(pflush_list->arr)[i])->state = PMEM_FREE_BLOCK;
		D_RW(D_RW(pflush_list->arr)[i])->sync = false;
	}

	pflush_list->cur_pages = 0;
	pflush_list->state = PMEM_LIST_FREE;

	// (2) Remove this list from the doubled-linked list
	//assert( !TOID_IS_NULL(pflush_list->prev_list) );

	pnext_list = D_RW(pflush_list->next_list);
	pprev_list = D_RW(pflush_list->prev_list);

	if (pprev_list != NULL &&
			D_RW(pprev_list->next_list) != NULL &&
			D_RW(pprev_list->next_list)->list_id == pflush_list->list_id){

		if (pnext_list == NULL) {
			TOID_ASSIGN(pprev_list->next_list, OID_NULL);
		}
		else {
			TOID_ASSIGN(pprev_list->next_list, (pflush_list->next_list).oid);
		}
		//signal the waiting list
		if (pprev_list->state == PMEM_LIST_WAIT_PROP){
			int hashed = pprev_list->hashed_id;

			assert(hashed == pflush_list->hashed_id);
#if defined (UNIV_PMEMOBJ_BUF)
			printf("NNNN [6] This list %zu with hashed_id %d signal for its prev list %zu wakeup\n",
					pflush_list->list_id, hashed, pprev_list->list_id);
#endif //UNIV_PMEMOBJ_BUF
			__wt_cond_signal(session, pmw->pbuf->prev_list_flush_conds[hashed]);
		}
	}

	if (pnext_list != NULL &&
			D_RW(pnext_list->prev_list) != NULL &&
			D_RW(pnext_list->prev_list)->list_id == pflush_list->list_id) {
		if (pprev_list == NULL) {
			TOID_ASSIGN(pnext_list->prev_list, OID_NULL);
		}
		else {
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
			printf("[6] !!!!! handle finish, cur_list_id %zu ",
					pflush_list->list_id);
			printf ("[6] !!!!  has next_list_id %zu ", pnext_list->list_id);
			printf ("[6] !!!! has prev_list_id %zu \n", pprev_list->list_id);
#endif
			TOID_ASSIGN(pnext_list->prev_list, (pflush_list->prev_list).oid);
		}
	}

	pflush_list->hashed_id = PMEM_ID_NONE;

	TOID_ASSIGN(pflush_list->next_list, OID_NULL);
	TOID_ASSIGN(pflush_list->prev_list, OID_NULL);

	// (3) we return this list to the free_pool
	PMEM_BUF_FREE_POOL* pfree_pool;
	pfree_pool = D_RW(buf->free_pool);

	pmemobj_rwlock_wrlock(pop, &pfree_pool->lock);

	POBJ_LIST_INSERT_TAIL(pop, &pfree_pool->head, flush_list, list_entries);
	pfree_pool->cur_lists++;
	//wakeup who is waitting for free_pool available
	//os_event_set(buf->free_pool_event);
	__wt_cond_signal(session, buf->free_pool_cond);

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	printf("\n *****[6] END finish AIO List %zu, cur free pool size %zu]\n", pflush_list->list_id, pfree_pool->cur_lists);
#endif
	pmemobj_rwlock_unlock(pop, &pfree_pool->lock);
	//the list has some unfinished aio	
	pmemobj_rwlock_unlock(pop, &pflush_list->lock);
}

/*
 *This function is called from aio complete (fil_aio_wait) 
 (1) Reset the list
 (2) Flush spaces in this list
 * */
void
pm_handle_finished_block_with_flusher(
	   	PMEM_WRAPPER*		pmw,
	   	PMEM_BUF_BLOCK*		pblock)
{
	
	WT_SESSION_IMPL *session = pmw->session;
	PMEMobjpool*		pop = pmw->pop;
	PMEM_BUF*			buf = pmw->pbuf;
	//PMEM_FLUSHER* flusher;

	//bool is_lock_prev_list = false;

	//(1) handle the flush_list
	TOID(PMEM_BUF_BLOCK_LIST) flush_list;

	TOID_ASSIGN(flush_list, pblock->list.oid);
	PMEM_BUF_BLOCK_LIST* pflush_list = D_RW(flush_list);

	PMEM_BUF_BLOCK_LIST* pnext_list;
	PMEM_BUF_BLOCK_LIST* pprev_list;


	assert(pflush_list);
		
	pmemobj_rwlock_wrlock(pop, &pflush_list->lock);
	
	if(pblock->sync)
		pflush_list->n_sio_pending--;
	else
		pflush_list->n_aio_pending--;

	if (pflush_list->n_aio_pending + pflush_list->n_sio_pending == 0) {
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("\n [*****[4]  BEGIN finish AIO list %zu hashed_id %d\n",
		   	pflush_list->list_id, pflush_list->hashed_id);
#endif
		//Now all pages in this list are persistent in disk
		//(0) flush spaces
		//TODO: Research flush in MongoDB
		//pm_buf_flush_spaces_in_list(pop, buf, pflush_list);
		// Reset the param_array
		ulint arr_idx;
		arr_idx = pflush_list->param_arr_index;
		assert(arr_idx >= 0);

		pmemobj_rwlock_wrlock(pop, &buf->param_lock);
		//TODO: Research AIO in MongoDB
		//buf->param_arrs[arr_idx].is_free = true;
		pmemobj_rwlock_unlock(pop, &buf->param_lock);

		//(1) Reset blocks in the list
		ulint i;
		for (i = 0; i < pflush_list->max_pages; i++) {
			D_RW(D_RW(pflush_list->arr)[i])->state = PMEM_FREE_BLOCK;
			D_RW(D_RW(pflush_list->arr)[i])->sync = false;
		}

		pflush_list->cur_pages = 0;
		pflush_list->state = PMEM_LIST_FREE;
		pflush_list->hashed_id = PMEM_ID_NONE;
		
		// (2) Remove this list from the doubled-linked list
		//assert( !TOID_IS_NULL(pflush_list->prev_list) );
		
		pnext_list = D_RW(pflush_list->next_list);
		pprev_list = D_RW(pflush_list->prev_list);

		if (pprev_list != NULL &&
			D_RW(pprev_list->next_list) != NULL &&
			D_RW(pprev_list->next_list)->list_id == pflush_list->list_id){

			if (pnext_list == NULL) {
				TOID_ASSIGN(pprev_list->next_list, OID_NULL);
			}
			else {
				TOID_ASSIGN(pprev_list->next_list, (pflush_list->next_list).oid);
			}
		}

		if (pnext_list != NULL &&
				D_RW(pnext_list->prev_list) != NULL &&
				D_RW(pnext_list->prev_list)->list_id == pflush_list->list_id) {
			if (pprev_list == NULL) {
				TOID_ASSIGN(pnext_list->prev_list, OID_NULL);
			}
			else {
			#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
			printf("[4] !!!!! handle finish, cur_list_id %zu ",
					pflush_list->list_id);
			printf ("[4] !!!!  has next_list_id %zu ", pnext_list->list_id);
			printf ("[4] !!!! has prev_list_id %zu \n", pprev_list->list_id);
			#endif
				TOID_ASSIGN(pnext_list->prev_list, (pflush_list->prev_list).oid);
			}
		}
		
		TOID_ASSIGN(pflush_list->next_list, OID_NULL);
		TOID_ASSIGN(pflush_list->prev_list, OID_NULL);

		// (3) we return this list to the free_pool
		PMEM_BUF_FREE_POOL* pfree_pool;
		pfree_pool = D_RW(buf->free_pool);

		pmemobj_rwlock_wrlock(pop, &pfree_pool->lock);

		POBJ_LIST_INSERT_TAIL(pop, &pfree_pool->head, flush_list, list_entries);
		pfree_pool->cur_lists++;
		//wakeup who is waitting for free_pool available
		//os_event_set(buf->free_pool_event);
		__wt_cond_signal(session, buf->free_pool_cond);
		
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("\n *****[4] END finish AIO List %zu]\n", pflush_list->list_id);
#endif
		pmemobj_rwlock_unlock(pop, &pfree_pool->lock);
	}
	//the list has some unfinished aio	
	pmemobj_rwlock_unlock(pop, &pflush_list->lock);
}
/*
 *	Read a page from pmem buffer using the page_id as key
 *	@param[in] pop		PMEMobjpool pointer
 *	@param[in] buf		PMEM_BUF pointer
 *  @param[in] page_id	read key
 *  @param[out] data	read data if the page_id exist in the buffer
 *  @return:  size of read page
 * */
const PMEM_BUF_BLOCK* 
pm_buf_read(
		   	PMEM_WRAPPER*	pmw,
			const char*		fname,
			uint64_t		name_hash,
		   	const off_t		offset,
		   	const size_t	size,
		   	byte*			data
		   )
{
	
	PMEMobjpool*		pop = pmw->pop;
	PMEM_BUF*			buf = pmw->pbuf;
	//bool is_lock_on_read = true;	
	ulint hashed;
	//int i;
	size_t i;
	size_t count;

	//currently, we only read blocks from ycsb data
	if ( strstr(fname, "ycsb") == NULL){
		return NULL;
	}
#if defined(UNIV_PMEMOBJ_BUF_STAT)
	ulint cur_level = 0;
#endif
	//int found;

	TOID(PMEM_BUF_BLOCK_LIST) cur_list;
	//PMEM_BUF_BLOCK_LIST* plist;
	//TOID(PMEM_BUF_BLOCK) free_block;
	PMEM_BUF_BLOCK* pblock;
	//char* pdata;
	byte* pdata;
	//size_t bytes_read;
	
	assert(buf);
	assert(data);	

	if ( pmw->pbuf->page_size < size){
		//we search in the ckpt block
		PMEM_BUF_BLOCK* pckpt_block = D_RW(buf->ckpt_block);
		if( pckpt_block->state != PMEM_FREE_BLOCK && 
				pckpt_block->disk_off == offset &&
				pckpt_block->name_hash == name_hash){
		pdata = buf->p_align;
		if (pckpt_block->size != size){
					printf("PMEM_ERROR, request size %zu differs with the block size %zu, file %s offset %zu\n",
							size, pckpt_block->size, fname, offset);
					assert(0);
		}
				memcpy(data, pdata + pckpt_block->pmemaddr, pckpt_block->size); 

		printf("PMEM_WARN 1: pm read from nvm size %zu exceed block size %zu offset %zu file %s \n",
				size, pmw->pbuf->page_size, offset, fname);
			return pckpt_block;
		}
		else {
			printf("PMEM_WARN 2: pm read from disk size %zu exceed block size %zu offset %zu file %s \n",
				size, pmw->pbuf->page_size, offset, fname);
			return NULL;
		}
	}
/*handle page 0
// Note that there are two case for reading a page 0
// Case 1: read through buffer pool (handle in this function, during the server working time)
// Case 2: read without buffer pool during the sever stat/stop 
*/
	//TODO: Handle page_id 0 in MongoDB
	if (offset == 0) {
		//PMEM_BUF_BLOCK_LIST* pspec_list;
		PMEM_BUF_BLOCK*		pspec_block;

		const PMEM_BUF_BLOCK_LIST* pspec_list = D_RO(buf->spec_list); 
		//scan in the special list
		for (i = 0; i < pspec_list->cur_pages; i++){
			//const PMEM_BUF_BLOCK* pspec_block = D_RO(D_RO(pspec_list->arr)[i]);
			if (	D_RO(D_RO(D_RO(buf->spec_list)->arr)[i]) != NULL && 
					D_RO(D_RO(D_RO(buf->spec_list)->arr)[i])->state != PMEM_FREE_BLOCK &&
					D_RO(D_RO(D_RO(buf->spec_list)->arr)[i])->name_hash ==  name_hash ) {
				pspec_block = D_RW(D_RW(D_RW(buf->spec_list)->arr)[i]);
				pmemobj_rwlock_rdlock(pop, &pspec_block->lock);
				//found
				pdata = buf->p_align;
				if (pspec_block-> size != size){
					printf("PMEM_ERROR, request size %zu differs with the block size %zu, file %s offset %zu\n",
							size, pspec_block->size, fname, offset);
					assert(0);
				}
				memcpy(data, pdata + pspec_block->pmemaddr, pspec_block->size); 
					
				printf("==> PMEM_DEBUG read page 0 (case 1) of file %s\n",
				pspec_block->file_name);
				if (pspec_block->size != size) {
					printf("PMEM_ERROR: read size %zu differs to block size %zu\n",
							size, pspec_block->size);
					assert(0);
				}

				pmemobj_rwlock_unlock(pop, &pspec_block->lock);
				return pspec_block;
			}
			//else: just skip this block
			//next block
		}//end for

		//this page 0 is not in PMEM, return NULL to read it from disk
		return NULL;
	} //end if page_no == 0

#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	PMEM_LESS_BUCKET_HASH_KEY(hashed, fname, offset);
#else //EVEN_BUCKET
	//PMEM_HASH_KEY(hashed, page_id.fold(), pmw->PMEM_N_BUCKETS);
	PMEM_HASH_KEY(hashed, offset, name_hash, pmw->PMEM_BUF_MAX_RANGE, pmw->PMEM_N_BUCKETS);
#endif

	TOID_ASSIGN(cur_list, (D_RO(buf->buckets)[hashed]).oid);

#if defined(UNIV_PMEMOBJ_BUF_STAT)
	++buf->bucket_stats[hashed].n_reads;
#endif

	if ( TOID_IS_NULL(cur_list)) {
		//assert(!TOID_IS_NULL(cur_list));
		printf("PMEM_ERROR error in get hashded list, but return NULL, check again! \n");
		return NULL;
	}
	//plist = D_RO(D_RO(buf->buckets)[hashed]);
	//assert(plist);

	//pblock = NULL;
	//found = -1;
	
	while ( !TOID_IS_NULL(cur_list) && (D_RO(cur_list) != NULL) ) {
		//plist = D_RW(cur_list);
		//pmemobj_rwlock_rdlock(pop, &plist->lock);
		//Scan in this list
		//for (i = 0; i < D_RO(cur_list)->max_pages; i++) {
		if (D_RO(cur_list) == NULL) {
			printf("===> ERROR read NULL list \n");
			assert(0);
		}
		count = 0;
		//for (i = 0; i < D_RO(cur_list)->cur_pages; i++) {
		for (i = 0; i < D_RO(cur_list)->max_pages; i++) {
			//accepted states: PMEM_IN_USED_BLOCK, PMEM_IN_FLUSH_BLOCK
			//if ( D_RO(D_RO(plist->arr)[i])->state != PMEM_FREE_BLOCK &&
			if (D_RO(D_RO(D_RO(cur_list)->arr)[i])->state == PMEM_FREE_BLOCK){
				continue;
			}
			if (	D_RO(D_RO(D_RO(cur_list)->arr)[i]) != NULL && 
					D_RO(D_RO(D_RO(cur_list)->arr)[i])->state != PMEM_FREE_BLOCK &&
					D_RO(D_RO(D_RO(cur_list)->arr)[i])->disk_off == offset &&
					D_RO(D_RO(D_RO(cur_list)->arr)[i])->name_hash == name_hash
					) {
					//printf("pm read hit, file %s offset %zu size %zu\n", fname, offset, size);
				pblock = D_RW(D_RW(D_RW(cur_list)->arr)[i]);
				//if(is_lock_on_read)
				pmemobj_rwlock_rdlock(pop, &pblock->lock);
				
				pdata = buf->p_align;

				if (pblock-> size != size){
					printf("PMEM_ERROR, request size %zu differs with the block size %zu, file %s offset %zu\n",
							size, pblock->size, fname, offset);
					assert(0);
				}
				memcpy(data, pdata + pblock->pmemaddr, pblock->size); 
				//bytes_read = pblock->size.physical();
				if (pblock->size != size) {
					printf("PMEM_ERROR: read size %zu differs to block size %zu\n",
							size, pblock->size);
					assert(0);
				}
#if defined (UNIV_PMEMOBJ_DEBUG)
				//assert( pm_check_io(pdata + pblock->pmemaddr, pblock->id) ) ;
#endif
#if defined(UNIV_PMEMOBJ_BUF_STAT)
				++buf->bucket_stats[hashed].n_reads_hit;
				if (D_RO(cur_list)->state == PMEM_LIST_PREP_PROP)
					++buf->bucket_stats[hashed].n_reads_flushing;
#endif
				//if(is_lock_on_read)
				pmemobj_rwlock_unlock(pop, &pblock->lock);

				//return pblock;
				return D_RO(D_RO(D_RO(cur_list)->arr)[i]);
			}
			++count;
			if (count == D_RO(cur_list)->cur_pages){
				//we end the loop early
				break;
			}
		}//end for

		//next list
		if ( TOID_IS_NULL(D_RO(cur_list)->next_list))
			break;
		TOID_ASSIGN(cur_list, (D_RO(cur_list)->next_list).oid);
		if (TOID_IS_NULL(cur_list) || D_RO(cur_list) == NULL)
			break;

#if defined(UNIV_PMEMOBJ_BUF_STAT)
		cur_level++;
		if (buf->bucket_stats[hashed].max_linked_lists < cur_level)
			buf->bucket_stats[hashed].max_linked_lists = cur_level;
#endif
		
	} //end while
	
		return NULL;
}


/*handle page 0
// Note that there are two case for reading a page 0
// Case 1: read through buffer pool (handle in pm_buf_read  during the server working time)
// Case 2: read without buffer pool during the sever stat/stop  (this function)
*/
const PMEM_BUF_BLOCK*
pm_buf_read_page_zero(
		PMEM_WRAPPER*			pmw,
		char*				file_name,
		byte*				data) {

	PMEMobjpool*		pop = pmw->pop;
	PMEM_BUF*			buf = pmw->pbuf;
	ulint i;
	byte* pdata;

	PMEM_BUF_BLOCK* pspec_block;

	PMEM_BUF_BLOCK_LIST* pspec_list = D_RW(buf->spec_list); 

	//scan in the special list with the scan key is file handle
	for (i = 0; i < pspec_list->cur_pages; i++){
		if (	D_RO(D_RO(D_RO(buf->spec_list)->arr)[i]) != NULL && 
				D_RO(D_RO(D_RO(buf->spec_list)->arr)[i])->state != PMEM_FREE_BLOCK &&
				strstr(D_RO(D_RO(D_RO(buf->spec_list)->arr)[i])->file_name, file_name) != 0) {
			//if (pspec_block != NULL &&
			//	pspec_block->state != PMEM_FREE_BLOCK &&
			//	strstr(pspec_block->file_name,file_name)!= 0)  {
			pspec_block = D_RW(D_RW(D_RW(buf->spec_list)->arr)[i]);
			pmemobj_rwlock_rdlock(pop, &pspec_block->lock);
			//found
			printf("!!!!!!!! PMEM_DEBUG read_page_zero file= %s \n", pspec_block->file_name);
			pdata = buf->p_align;
			memcpy(data, pdata + pspec_block->pmemaddr, pspec_block->size); 

			//pmemobj_rwlock_unlock(pop, &pspec_list->lock);
			pmemobj_rwlock_unlock(pop, &pspec_block->lock);
			return pspec_block;
		}
		//else: just skip this block
		//next block
		}//end for

	//pmemobj_rwlock_unlock(pop, &pspec_list->lock);
	//this page 0 is not in PMEM, return NULL to read it from disk
	return NULL;
}

/*
 * Check full lists in the buckets and linked-list 
 * Resume flushing them 
   Logical of this function is similar with pm_buf_write
   in case the list is full

  This function should be called by only recovery thread. We don't handle thread lock here 
 * */
void
pm_buf_resume_flushing(
		PMEM_WRAPPER*			pmw){
	
	PMEM_BUF*				buf = pmw->pbuf;

	ulint i;
	TOID(PMEM_BUF_BLOCK_LIST) cur_list;
	PMEM_BUF_BLOCK_LIST* plist;
	PMEM_BUF_BLOCK_LIST* phashlist;

	for (i = 0; i < pmw->PMEM_N_BUCKETS; i++) {
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		//printf ("\n====>resuming flush hash %zu\n", i);
#endif
		TOID_ASSIGN(cur_list, (D_RW(buf->buckets)[i]).oid);
		phashlist = D_RW(cur_list);
		if (phashlist->cur_pages >= phashlist->max_pages * pmw->PMEM_BUF_FLUSH_PCT) {
			assert(phashlist->state == PMEM_LIST_PREP_PROP ||
					phashlist->state == PMEM_LIST_PROPAGATING);
			assert((uint64_t) phashlist->hashed_id == i);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
			printf("\ncase 1 PMEM_RECOVERY ==> resume flushing for hashed_id %zu list_id %zu\n", i, phashlist->list_id);
#endif
			pm_buf_handle_full_hashed_list(pmw, i);
		}

		//(2) Check the linked-list of current list	
		TOID_ASSIGN(cur_list, (D_RW(cur_list)->next_list).oid);
		while( !TOID_IS_NULL(cur_list)) {
			plist = D_RW(cur_list);
			if (plist->cur_pages >= plist->max_pages * pmw->PMEM_BUF_FLUSH_PCT) {
				//for the list in flusher, we only assign the flusher thread, no handle free list replace
			assert(phashlist->state == PMEM_LIST_PREP_PROP ||
					phashlist->state == PMEM_LIST_PROPAGATING);
				assert((uint64_t)plist->hashed_id == i);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
				printf("\n\t\t case 2 PMEM_RECOVERY ==> resume flushing for linked list %zu of hashlist %zu hashed_id %zu\n", plist->list_id, phashlist->list_id, i);
#endif
				pm_buf_assign_flusher(pmw, plist);
			}

			TOID_ASSIGN(cur_list, (D_RW(cur_list)->next_list).oid);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
			printf("pm_buf_resume_flushing get next linked_list of hash_id %zu list_id %zu\n", i, plist->list_id);
#endif
			//next linked-list
		} //end while
		//next hashed list	
	} //end for
}

/*
 *Handle flushing a bucket list when it is full
 (1) Assign a pointer in worker thread to the full list
 (2) Swap the full list with the first free list from the free pool 
 * */
void
pm_buf_handle_full_hashed_list(
		PMEM_WRAPPER*		pmw,
		ulint			hashed) {

	WT_SESSION_IMPL *session = pmw->session;
	PMEMobjpool*	pop = pmw->pop;
	PMEM_BUF*		buf = pmw->pbuf;
	TOID(PMEM_BUF_BLOCK_LIST) hash_list;
	PMEM_BUF_BLOCK_LIST* phashlist;

	TOID_ASSIGN(hash_list, (D_RW(buf->buckets)[hashed]).oid);
	phashlist = D_RW(hash_list);

	/*(1) Handle flusher
	 *+ assign the pointer from the Flusher to the full list 
	 + trigger the propagation process if the number of full list a threashold
	 * */
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	printf("\t[[1.1] BEGIN handle assign flusher list %zu hashed %zu ===> \n", phashlist->list_id, hashed);
#endif
	pm_buf_assign_flusher(pmw, phashlist);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	printf("\n\t[1.1] END handle assign flusher list %zu]\n", phashlist->list_id);
#endif

	/*    (2) Handle free list*/
get_free_list:
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	printf("\n [1.2] BEGIN get_free_list to replace full list %zu ==>\n", phashlist->list_id);
#endif
	//Get a free list from the free pool
	pmemobj_rwlock_wrlock(pop, &(D_RW(buf->free_pool)->lock));

	TOID(PMEM_BUF_BLOCK_LIST) first_list = POBJ_LIST_FIRST (&(D_RW(buf->free_pool)->head));
	if (D_RW(buf->free_pool)->cur_lists == 0 ||
			TOID_IS_NULL(first_list) ) {
		pthread_t tid;
		tid = pthread_self();
		printf("PMEM_INFO: thread %zu free_pool->cur_lists = %zu, the first list is NULL? %d the free list is empty, sleep then retry..\n", tid, D_RO(buf->free_pool)->cur_lists, TOID_IS_NULL(first_list));
		pmemobj_rwlock_unlock(pop, &(D_RW(buf->free_pool)->lock));
		//os_event_wait(buf->free_pool_event);
		__wt_cond_wait(session, buf->free_pool_cond, 0, NULL);

		goto get_free_list;
	}

	POBJ_LIST_REMOVE(pop, &D_RW(buf->free_pool)->head, first_list, list_entries);
	D_RW(buf->free_pool)->cur_lists--;
	//The free_pool may empty now, wait in necessary
	//os_event_reset(buf->free_pool_event);
	pmemobj_rwlock_unlock(pop, &(D_RW(buf->free_pool)->lock));

	assert(!TOID_IS_NULL(first_list));
	//This ref hashed_id used for batch AIO
	//Hashed id of old list is kept until its batch AIO is completed
	D_RW(first_list)->hashed_id = hashed;

	//Asign linked-list refs 
	TOID_ASSIGN(D_RW(buf->buckets)[hashed], first_list.oid);
	TOID_ASSIGN( D_RW(first_list)->next_list, hash_list.oid);
	TOID_ASSIGN( D_RW(hash_list)->prev_list, first_list.oid);

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	printf("\n [1.2] END get_free_list %zu to replace full list %zu, hashed %zu ==>\n", D_RW(first_list)->list_id, phashlist->list_id, hashed);
#endif

#if defined (UNIV_PMEMOBJ_BUF_STAT)
	if ( !TOID_IS_NULL( D_RW(hash_list)->next_list ))
		++buf->bucket_stats[hashed].max_linked_lists;
#endif 
	// Now the handle finish here. The FLUSHER is reponsible for propagation
}

//////////////////// The FLUSHER /////////////////////
/////////////////////////////////////////////////////
// Assign an idle worker to handle the pointer to the hashlist
void
pm_buf_assign_flusher(
		PMEM_WRAPPER*				pmw,
		PMEM_BUF_BLOCK_LIST*	phashlist) {

	WT_SESSION_IMPL *session = pmw->session;
	PMEM_BUF*				buf = pmw->pbuf;
	//int lock_ret;

	PMEM_FLUSHER* flusher = buf->flusher;
assign_worker:
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	printf("[pm_buf_assign_flusher try to acquire flusher_lock ... hashed_id %d phashlist %zu flusher size = %zu cur request = %zu \n", phashlist->hashed_id, phashlist->list_id, flusher->size, flusher->n_requested);
#endif
	//lock_ret = __wt_spin_trylock(session, &flusher->flusher_lock);
	__wt_spin_lock(session, &flusher->flusher_lock);
	//if (lock_ret != 0){
	//	printf("PMEM WARN: in pm_buf_assign_flusher(), wt_spin_trylock() busy \n");
	//}
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	printf("[pm_buf_assign_flusher OK! acquired flusher_lock ,hashed_id %d phashlist %zu flusher size = %zu cur request = %zu \n", phashlist->hashed_id, phashlist->list_id, flusher->size, flusher->n_requested);
#endif

	if (flusher->n_requested == flusher->size) {
		//all requested slot is full, wait until there is an available one
		printf("PMEM_INFO: pm_buf_assign_flusher() for plist %zu, all reqs are booked, sleep and wait \n", phashlist->list_id);
		__wt_spin_unlock(session, &flusher->flusher_lock);
		__wt_cond_wait(session, flusher->is_req_full_cond, 0, NULL);
		goto assign_worker;	
	}

	//find an idle thread to assign flushing task
	int64_t n_try = flusher->size;
	while (n_try > 0) {
		if (flusher->flush_list_arr[flusher->tail] == NULL) {
			//found
			//Asign the pointer to the hashlist
			flusher->flush_list_arr[flusher->tail] = phashlist;
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
			printf("pm_buf_assign_flusher pointer id = %zu, list_id = %zu\n", flusher->tail, phashlist->list_id);
#endif
			++flusher->n_requested;
			//delay calling flush up to a threshold
			if (flusher->n_requested >= pmw->PMEM_FLUSHER_WAKE_THRESHOLD) {
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
			printf("\n\tin [1.1] try to trigger flusher list_id = %zu flusher->n_requested %zu \n", phashlist->list_id, flusher->n_requested);
#endif
				//Triger the propagation process, see pm_flusher_worker()
				//__wt_cond_signal(flusher->worker_sessions[flusher->tail], flusher->is_req_not_empty_cond);
				//__wt_cond_signal(session, flusher->is_req_not_empty_cond); // see pm_flusher_worker()
				//Only wake up the right flusher thread
				__wt_cond_signal(session, flusher->flusher_conds[flusher->tail]); // see pm_flusher_worker()
			}

			if (flusher->n_requested >= flusher->size) {
				//os_event_reset(flusher->is_req_full);
			}
			//for the next 
			flusher->tail = (flusher->tail + 1) % flusher->size;
			break;
		}
		//circled increase
		flusher->tail = (flusher->tail + 1) % flusher->size;
		n_try--;
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("pm_buf_assign_flusher n_try = %zu\n", n_try);
#endif
	} //end while 
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	//printf("pass while phashlist %zu flusher_tail = %zu flusher size = %zu n_requested %zu]\n", phashlist->list_id, flusher->tail, flusher->size, flusher->n_requested);
#endif
	//check
	if (n_try == 0) {
		/*This imply an logical error 
		 * */
		printf("=======> PMEM_ERROR requested/size = %zu /%zu \n", flusher->n_requested, flusher->size);
		//mutex_exit(&flusher->mutex);
		//if (lock_ret == 0)
			__wt_spin_unlock(session, &flusher->flusher_lock);
		goto assign_worker;
		//exit(0);
		//assert (n_try);
	}

	//mutex_exit(&flusher->mutex);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	printf("[pm_buf_assign_flusher UNLOCK ,hashed_id %d phashlist %zu flusher size = %zu cur request = %zu \n", phashlist->hashed_id, phashlist->list_id, flusher->size, flusher->n_requested);
#endif
	//if (lock_ret == 0)
		__wt_spin_unlock(session, &flusher->flusher_lock);
	//end FLUSHER handling
}

//Init the FLUSHER 
//pmw: PMEM wrapper
//size: number of workers in the Flusher
int
pm_flusher_init(
				PMEM_WRAPPER*		pmw, 
				const size_t	size) {
	WT_SESSION_IMPL *session;
	WT_CONNECTION_IMPL *conn;
	PMEM_BUF* buf = pmw->pbuf;
	PMEM_FLUSHER* flusher;
	
	uint32_t i, session_flags;

	session = pmw->session;
	conn = S2C(session);

	flusher = (PMEM_FLUSHER*) (
			malloc(sizeof(PMEM_FLUSHER)));
	//locks
	__wt_spin_init(session, &flusher->flusher_lock, "flusher");
	flusher->flusher_arr_locks = (WT_SPINLOCK*) calloc(size, sizeof(WT_SPINLOCK));
	for (i = 0; i < size; i++) {
		__wt_spin_init(session, &flusher->flusher_arr_locks[i], "flusher_arr_locks");
	}	
	//conds	
	
	WT_RET(__wt_cond_alloc(session, "flusher_is_req_not_empty", &flusher->is_req_not_empty_cond));

	flusher->flusher_conds = (WT_CONDVAR**) calloc(size, sizeof(WT_CONDVAR*));
	for (i = 0; i < size; i++){
		WT_RET(__wt_cond_alloc(session, "fluser conds", &flusher->flusher_conds[i]));
	}

	//flusher->is_req_full = os_event_create("flusher_is_req_full");
WT_RET(__wt_cond_alloc(session, "flusher_is_req_full", &flusher->is_req_full_cond));

//	flusher->is_all_finished = os_event_create("flusher_is_all_finished");
WT_RET(__wt_cond_alloc(session, "flusher_is_all_finished", &flusher->is_all_finished_cond));
	//flusher->is_all_closed = os_event_create("flusher_is_all_closed");
WT_RET(__wt_cond_alloc(session, "flusher_is_all_closed", &flusher->is_all_closed_cond));
	flusher->size = size;
	flusher->n_workers = 0;
	flusher->tail = 0;
	flusher->n_requested = 0;
	flusher->is_running = false;

	//allocate the block list array
	flusher->flush_list_arr = (PMEM_BUF_BLOCK_LIST**) (	calloc(size, sizeof(PMEM_BUF_BLOCK_LIST*)));
	flusher->flush_list_flag_arr = (bool*) calloc(size, sizeof(bool));

	for (i = 0; i < size; i++) {
		//flusher->flush_list_arr[i] = static_cast <PMEM_BUF_BLOCK_LIST*> (
		//		malloc(sizeof(PMEM_BUF_BLOCK_LIST)));
		flusher->flush_list_arr[i] = NULL;
		flusher->flush_list_flag_arr[i] = false;
	}	
	buf->flusher = flusher;

	//Threads allocate
	flusher->worker_sessions = (WT_SESSION_IMPL**) calloc(size, sizeof(WT_SESSION_IMPL*));
	flusher->worker_tids  = (wt_thread_t*)  calloc(size, sizeof(wt_thread_t));

	for (i = 0; i < size; i++) {
		//Each flusher-worker has its own session
		session_flags = WT_SESSION_SERVER_ASYNC;
		WT_RET(__wt_open_internal_session(conn, "pm-flusher-worker",
		    true, session_flags, &flusher->worker_sessions[i]));
	}

	flusher->is_running = true;

	printf("======>   PMEM_INFO: start %zu flusher threads\n", size);
	for (i = 0; i < size; ++i) {
		//start the worker threads

		//FLUSHER_THREAD_PARAM param;
		//param.flusher_idx = i;
		//param.pmw = pmw;

		//WT_RET(__wt_thread_create(session, &flusher->worker_tids[i],
		//    pm_flusher_worker, &param));
		WT_RET(__wt_thread_create(session, &flusher->worker_tids[i],
		    pm_flusher_worker, pmw));
	}
	

	return PMEM_SUCCESS;
}
////////////////////////////////////
// Close the flusher
// ////////////////////////////////////
int
pm_buf_flusher_close(
		PMEM_WRAPPER*	pmw) {

	int ret = 0;

	WT_SESSION_IMPL *session;
	//WT_SESSION *wt_session;
	PMEM_BUF*	buf;
	PMEM_FLUSHER* flusher;
	ulint i;
	
	session = pmw->session;
	buf = pmw->pbuf;
	flusher = buf->flusher;
	
	 __wt_spin_lock(session, &flusher->flusher_lock);
	flusher->is_running = false;

	//wait for all workers finish their work
	if (flusher->n_workers > 0){
		__wt_spin_unlock(session, &flusher->flusher_lock);
		//The last calls
		for (i = 0; i < flusher->size; ++i){
			__wt_cond_signal(session, flusher->flusher_conds[i]);
		}
		__wt_cond_wait(session, flusher->is_all_closed_cond, 0, NULL);
		printf("All flusher workers are closed, release resources\n");	
	}
	else {
		__wt_spin_unlock(session, &flusher->flusher_lock);
	}

	//flush array	
	for (i = 0; i < flusher->size; i++) {
		if (flusher->flush_list_arr[i]){
			//free(buf->flusher->flush_list_arr[i]);
			flusher->flush_list_arr[i] = NULL;
		}
	}	

	if (flusher->flush_list_arr){
		free(flusher->flush_list_arr);
		flusher->flush_list_arr = NULL;
	}	

    free (flusher->flush_list_flag_arr);

	for (i = 0; i < buf->flusher->size; i++) {
		WT_TRET(__wt_thread_join(session, flusher->worker_tids[i]));
	}
	//close the worker thread sessions
	//for (i = 0; i < buf->flusher->size; i++) {
	//	if (flusher->worker_sessions[i] != NULL) {
	//		wt_session = &flusher->worker_sessions[i]->iface;
	//		WT_TRET(wt_session->close(wt_session, NULL));
	//		flusher->worker_sessions[i] = NULL;
	//	}
	//}

	free(flusher->worker_tids);
	free(flusher->worker_sessions);

	__wt_spin_destroy(session, &flusher->flusher_lock);
	for (i = 0; i < buf->flusher->size; i++){
		__wt_spin_destroy(session, &flusher->flusher_arr_locks[i]);
	}
	free(flusher->flusher_arr_locks);

	__wt_cond_destroy(session, &flusher->is_req_not_empty_cond);
	__wt_cond_destroy(session, &flusher->is_req_full_cond);
	__wt_cond_destroy(session, &flusher->is_all_finished_cond);
	__wt_cond_destroy(session, &flusher->is_all_closed_cond);
	for (i = 0; i < buf->flusher->size; i++){
		__wt_cond_destroy(session, &flusher->flusher_conds[i]);
	}
	free(flusher->flusher_conds);

	if(buf->flusher){
		buf->flusher = NULL;
		free(buf->flusher);
	}
	//printf("free flusher ok\n");
	return PMEM_SUCCESS;
}
////////////////////////////////////////////////
//		The master flusher thread //////////////
/////////////////////////////////////////////////

//		Follow the fashion from __ckpt_server()
//		arg: the pointer to the session of the caller thread
static void* pm_flusher_worker (void* arg) {
	WT_SESSION_IMPL *session;
	size_t select_i;
	ulint flusher_idx;
	//int lock_ret;
	PMEM_WRAPPER* pmw = (PMEM_WRAPPER*) arg;
	assert(pmw);

	PMEM_FLUSHER* flusher = pmw->pbuf->flusher;
	PMEM_BUF_BLOCK_LIST* plist;

	session = pmw->session;

	//mutex_enter(&flusher->mutex);
	__wt_spin_lock(session, &flusher->flusher_lock);
	//lock_ret = __wt_spin_trylock(session, &flusher->flusher_lock);
	flusher_idx = flusher->n_workers;	
	flusher->n_workers++;
	//mutex_exit(&flusher->mutex);
	//if(lock_ret == 0)
		__wt_spin_unlock(session, &flusher->flusher_lock);

	for (;;) {
		//wait for the flusher is trigger in pm_buf_assign_flusher()
		//__wt_cond_wait(session,
		//    flusher->is_req_not_empty_cond, 0, NULL);
		__wt_cond_wait(session,
		    flusher->flusher_conds[flusher_idx], 0, NULL);
#if defined(UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("[2] wakeup flusher worker %zu, try to acquire the flusher_arr_locks[]...\n", flusher_idx);	
#endif
		//looking for a full list in wait-list and flush it
		__wt_spin_lock(session, &flusher->flusher_arr_locks[flusher_idx]);
		
#if defined(UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("[2] OK! acquired the flusher_arr_locks %zu...\n", flusher_idx);	
#endif
		select_i = flusher_idx;	
		//Get the plist based on the flusher_idx
		plist = flusher->flush_list_arr[flusher_idx];

		if (plist == NULL){
			if (!flusher->is_running){
				//Shutdowning
				break;
			}
			printf("pm_flusher_worker() plist is NULL, logical error!!! Exit\n");
			exit(0);
		}

		if (flusher->flush_list_flag_arr[select_i] == true){
			printf("pm_flusher_worker() logical error!!! Exit\n");
			exit(0);
		}

		flusher->flush_list_flag_arr[select_i] = true;

		//***this call aio_batch ***
#if defined(UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("\n [2] BEGIN (in flusher thread), pointer id=%zu, list_id =%zu\n", select_i, plist->list_id);
#endif
		//////////////////////////////////////////////
		//          PROPAGATION   /////////////
		pm_buf_flush_list(pmw, plist);
		/////////////////////////////////////////////
		
		//we can set the pointer to null after the pm_buf_flush_list finished
		flusher->flush_list_arr[select_i] = NULL;
		flusher->flush_list_flag_arr[select_i] = false;
		__wt_spin_unlock(session, &flusher->flusher_arr_locks[flusher_idx]);

#if defined(UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("\n [2] END (in flusher thread), pointer id=%zu, list_id =%zu\n", select_i, plist->list_id);
		//#endif

		//#if defined(UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("[2.1] After pm_buf_flush_list() list_id %zu, try to acquire the flusher_lock...\n", plist->list_id);	
#endif
		__wt_spin_lock(session, &flusher->flusher_lock);

#if defined(UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("[2.1] After pm_buf_flush_list() list_id %zu, OK! acquired the flusher_lock\n", plist->list_id);	
#endif
		flusher->n_requested--;
		//os_event_set(flusher->is_req_full);
		__wt_cond_signal(session, flusher->is_req_full_cond);

		if (flusher->n_requested == 0) {
			//if the server is running, sleep (wait) again
			//otherwise, end the thread  
			if (flusher->is_running == true) {
				//server is running, start waiting
				//os_event_reset(flusher->is_req_not_empty);
			}
			else {
				//mutex_exit(&flusher->mutex);
				//if (lock_ret != 0)
					__wt_spin_unlock(session, &flusher->flusher_lock);
				//We end the for loop for this thread
				break;
			}
		}
		__wt_spin_unlock(session, &flusher->flusher_lock);

#if defined(UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("\n [2.1] UNLOCK pointer id=%zu, list_id =%zu\n", select_i, plist->list_id);
#endif

		//Repeat waiting...
	} //end for loop

	//lock_ret = __wt_spin_trylock(session, &flusher->flusher_lock);
	 __wt_spin_lock(session, &flusher->flusher_lock);
	flusher->n_workers--;
	if (flusher->n_workers == 0) {
		__wt_cond_signal(session, flusher->is_all_closed_cond);
	}
	//if (lock_ret != 0)
		__wt_spin_unlock(session, &flusher->flusher_lock);
	
	return 0;
}


/////////// End FLUSHER implementation

///////////////////////// PARTITION ///////////////////////////
#if defined (UNIV_PMEMOBJ_BUF_PARTITION_STAT)
void 
pm_filemap_init(
		PMEM_BUF*		buf){
	
	ulint i;
	PMEM_FILE_MAP* fm;

	fm = static_cast<PMEM_FILE_MAP*> (malloc(sizeof(PMEM_FILE_MAP)));
	fm->max_size = 1024*1024;
	fm->items = static_cast<PMEM_FILE_MAP_ITEM**> (
		calloc(fm->max_size, sizeof(PMEM_FILE_MAP_ITEM*)));	
	//for (i = 0; i < fm->max_size; i++) {
	//	fm->items[i].count = 0
	//	fm->item[i].hashed_ids = static_cast<int*> (
	//			calloc(PMEM_BUCKET_SIZE, sizeof(int)));
	//}

	fm->size = 0;

	buf->filemap = fm;
}


#endif //UNIV_PMEMOBJ_PARTITION

//						END OF PARTITION//////////////////////


//////////////////////// STATISTICS FUNCTS/////////////////////

#if defined (UNIV_PMEMOBJ_BUF_STAT)

#define PMEM_BUF_BUCKET_STAT_PRINT(pb, index) do {\
	assert (0 <= index && index <= PMEM_N_BUCKETS);\
	PMEM_BUCKET_STAT* p = &pb->bucket_stats[index];\
	printf("bucket %zu [n_writes %zu,\t n_overwrites %zu,\t n_reads %zu, n_reads_hit %zu, n_reads_flushing %zu \tmax_linked_lists %zu, \tn_flushed_lists %zu] \n ",index,  p->n_writes, p->n_overwrites, p->n_reads, p->n_reads_hit, p->n_reads_flushing, p->max_linked_lists, p->n_flushed_lists); \
}while (0)


void pm_buf_bucket_stat_init(PMEM_WRAPPER* pmw) {

	PMEM_BUF* pbuf = pmw->pbuf;
	int i;

	PMEM_BUCKET_STAT* arr = 
		(PMEM_BUCKET_STAT*) calloc(pmw->PMEM_N_BUCKETS, sizeof(PMEM_BUCKET_STAT));
	
	for (i = 0; i < pmw->PMEM_N_BUCKETS; i++) {
		arr[i].n_writes = arr[i].n_overwrites = 
			arr[i].n_reads = arr[i].n_reads_hit = arr[i].n_reads_flushing = arr[i].max_linked_lists =
			arr[i].n_flushed_lists = 0;
	}
	pbuf->bucket_stats = arr;
}

/*
 *Print statistic infomation for all hashed lists
 * */
void pm_buf_stat_print_all(PMEM_WRAPPER* pmw) {

	PMEM_BUF* pbuf = pmw->pbuf;
	ulint i;
	PMEM_BUCKET_STAT* arr = pbuf->bucket_stats;
	PMEM_BUCKET_STAT sumstat;

	sumstat.n_writes = sumstat.n_overwrites =
		sumstat.n_reads = sumstat.n_reads_hit = sumstat.n_reads_flushing = sumstat.max_linked_lists = sumstat.n_flushed_lists = 0;


	for (i = 0; i < pmw->PMEM_N_BUCKETS; i++) {
		sumstat.n_writes += arr[i].n_writes;
		sumstat.n_overwrites += arr[i].n_overwrites;
		sumstat.n_reads += arr[i].n_reads;
		sumstat.n_reads_hit += arr[i].n_reads_hit;
		sumstat.n_reads_flushing += arr[i].n_reads_flushing;
		sumstat.n_flushed_lists += arr[i].n_flushed_lists;

		if (sumstat.max_linked_lists < arr[i].max_linked_lists) {
			sumstat.max_linked_lists = arr[i].max_linked_lists;
		}

		PMEM_BUF_BUCKET_STAT_PRINT(pbuf, i);
	}

	printf("\n==========\n Statistic info:\n n_writes\t n_overwrites \t n_reads \t n_reads_hit \t n_reads_flushing \t max_linked_lists \t n_flushed_lists \n %zu \t %zu \t %zu \t %zu \t %zu \t %zu \t %zu \n",
			sumstat.n_writes, sumstat.n_overwrites, sumstat.n_reads, sumstat.n_reads_hit, sumstat.n_reads_flushing, sumstat.max_linked_lists, sumstat.n_flushed_lists);
	printf("\n==========\n");

	fprintf(pbuf->deb_file, "\n==========\n Statistic info:\n n_writes\t n_overwrites \t n_reads \t n_reads_hit \t n_reads_flushing \t max_linked_lists \t n_flushed_lists \n %zu \t %zu \t %zu \t %zu \t %zu \t %zu \t %zu \n",
			sumstat.n_writes, sumstat.n_overwrites, sumstat.n_reads, sumstat.n_reads_hit, sumstat.n_reads_flushing, sumstat.max_linked_lists, sumstat.n_flushed_lists);
	fprintf(pbuf->deb_file, "\n==========\n");

	fprintf(debug_file, "\n==========\n Statistic info:\n n_writes n_overwrites n_reads n_reads_hit n_reads_flushing max_linked_lists n_flushed_lists \n %zu %zu %zu %zu %zu %zu %zu \n",
			sumstat.n_writes, sumstat.n_overwrites, sumstat.n_reads, sumstat.n_reads_hit, sumstat.n_reads_flushing, sumstat.max_linked_lists, sumstat.n_flushed_lists);
	fprintf(pbuf->deb_file, "\n==========\n");

}

#endif //UNIV_PMEMOBJ_STAT
///////////////////// DEBUG Functions /////////////////////////

#if defined (UNIV_PMEMOBJ_BUF_DEBUG)
/*
 *Read the header from frame that contains page_no and space. Then check whether they matched with the page_id.page_no() and page_id.space()
 * */
bool pm_check_io(byte* frame, page_id_t page_id) {
	//Does some checking
	ulint   read_page_no;
	ulint   read_space_id;

	read_page_no = mach_read_from_4(frame + FIL_PAGE_OFFSET);
	read_space_id = mach_read_from_4(frame + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
	if ((page_id.space() != 0
				&& page_id.space() != read_space_id)
			|| page_id.page_no() != read_page_no) {                                      
		/* We did not compare space_id to read_space_id
		 *             if bpage->space == 0, because the field on the
		 *                         page may contain garbage in MySQL < 4.1.1,                                        
		 *                                     which only supported bpage->space == 0. */

		ib::error() << "PMEM_ERROR: Space id and page no stored in "
			"the page, read in are "
			<< page_id_t(read_space_id, read_page_no)
			<< ", should be " << page_id;
		return 0;
	}   
	return PMEM_SUCCSESS;
}
#endif //defined (UNIV_PMEMOBJ_BUF_DEBUG)

void pm_buf_print_lists_info(PMEM_WRAPPER* pmw){
	PMEM_BUF_FREE_POOL* pfreepool;
	PMEM_BUF_BLOCK_LIST* plist;
	uint64_t i;
	
	PMEM_BUF* buf = pmw->pbuf;
	printf("PMEM_DEBUG ==================\n");

	pfreepool = D_RW(buf->free_pool);
	printf("The free pool: curlists=%zd \n", pfreepool->cur_lists);

	printf("The buckets: \n");
	
	for (i = 0; i < pmw->PMEM_N_BUCKETS; i++){

		plist = D_RW( D_RW(buf->buckets)[i] );
		printf("\tBucket %zu: list_id=%zu cur_pages=%zd ", i, plist->list_id, plist->cur_pages);
		printf("\n");
	}		
}
//////////////////////// THREAD HANDLER /////////////
#endif //UNIV_PMEMOBJ_BUF
