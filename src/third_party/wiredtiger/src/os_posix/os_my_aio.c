/*
 *Implement AIO in WiredTiger
Note: build with -laio option
Declaration is in src/third_party/wiredtiger/src/include/os0file.h
 * */

#include <libaio.h>
#include <lz4.h>
#include <zlib.h>

#include <sys/time.h>
#include <assert.h>
#include "wt_internal.h"

#include <libpmemobj.h>
#if defined (UNIV_PMEMOBJ_BUF)
static const int	OS_AIO_IO_SETUP_RETRY_ATTEMPTS = 5;
static const int	OS_AIO_IO_SETUP_RETRY_SLEEP = 5000;
static const ulint	OS_AIO_REAP_TIMEOUT = 500000000UL; //500ms
/////////////////// AIO functions /////////////////////
// Init the AIO
// pmw [in]: The pmem wrapper
// n_slots: the total number of slots, each slot for one AIO
// n_segments: the number of segment, n_slots must be a multiplier of n_segments
AIO* AIO_init(PMEM_WRAPPER* pmw,
	   	ulint n_slots,
		ulint n_segments){

	WT_SESSION_IMPL *session = pmw->session;
	WT_CONNECTION_IMPL *conn;
	AIO* aio;
	ulint i;
	uint32_t session_flags;
	int ret;

	conn = S2C(session);

	aio = (AIO*) malloc(sizeof(AIO));

	if (aio == NULL)
		goto err;

    pmw->aio = aio;

	// (1) Events, locks
	aio->n_events = n_slots;
	aio->events = (struct io_event*) calloc(aio->n_events, sizeof(struct io_event));
	memset(&aio->events[0], 0x0, sizeof(aio->events[0]) * n_slots);

	ret = __wt_spin_init(session, &aio->aio_lock, "aio");

	ret = __wt_cond_alloc(session,"aio not full", &aio->not_full_cond);
	ret = __wt_cond_alloc(session,"is aio req", &aio->is_aio_req_cond);
	ret = __wt_cond_alloc(session,"all aio closed", &aio->all_aio_closed_cond);
    

	aio->n_seg_reserved = 0;
	aio->n_slot_reserved = 0;
    aio->next_free_seg = 0;

	//(2) Segments
	ulint slots_per_seg = n_slots / n_segments;

	aio->n_segs = n_segments;
    aio->n_slots_per_seg = slots_per_seg;

	PMEM_SEG_WRAPPER* seg_arr = (PMEM_SEG_WRAPPER*) calloc(n_segments, sizeof(PMEM_SEG_WRAPPER));
	aio->seg_wrapper_arr = seg_arr;

    WT_SPINLOCK* seg_locks = (WT_SPINLOCK*) calloc(n_segments, sizeof(WT_SPINLOCK));
    aio->seg_locks = seg_locks;
	
	for (i = 0; i < n_segments; ++i) {
		seg_arr[i].local_index = i;
		seg_arr[i].io_finished = 0;
		seg_arr[i].io_pending = 0;
		seg_arr[i].is_reserved = false;
		
		seg_arr[i].ppiocb = (struct iocb**) 
				calloc(slots_per_seg, sizeof(struct iocb*));	

	    ret = __wt_spin_init(session, &seg_locks[i], "seg_locks");
	}

	//(3) aio context
	aio->aio_ctx = (io_context_t*) calloc(n_segments, sizeof(*aio->aio_ctx));
	if (aio->aio_ctx == NULL){
		printf("cannot allocate aio_ctx\n");
		exit(0);
	}
	io_context_t*	ctx_arr = aio->aio_ctx;
	for (i = 0; i < n_segments; ++i) {
		if (!linux_create_io_ctx(slots_per_seg, &ctx_arr[i])) {
			printf("linux_create_io_ctx() has error\n");
			goto err;	
		}
	}

	//(4) Slots	
	aio->n_slots = n_slots;
	Slot* slots = (Slot*) calloc(aio->n_slots, sizeof(Slot));
	aio->slots = slots;
	//Init each slot in the slot array
	for (i = 0; i < n_slots; ++i) {
		slots[i].pos = (uint32_t) i;
		slots[i].is_reserved = false;
		slots[i].is_aio_completed = false;
		slots[i].ret = 0;
		slots[i].n_bytes = 0;
		memset(&slots[i].control, 0x0, sizeof(slots[i].control));
	}	

	//(5) Thread Handler
	
	aio->n_workers = 0;
	aio->is_running = false;

	aio->worker_sessions = (WT_SESSION_IMPL**) calloc(n_segments, sizeof(WT_SESSION_IMPL*));
	aio->worker_tids  = (wt_thread_t*)  calloc(n_segments, sizeof(wt_thread_t));

    aio->aio_req_conds = (WT_CONDVAR**) calloc(n_segments, sizeof(WT_CONDVAR*));

	session_flags = WT_SESSION_SERVER_ASYNC;
	for (i = 0; i < n_segments; ++i) {
		//Each flusher-worker has its own session
		ret = __wt_open_internal_session(conn, "pm-aio-worker",
		    true, session_flags, &aio->worker_sessions[i]);

	    ret = __wt_cond_alloc(aio->worker_sessions[i],"aio req", &aio->aio_req_conds[i]);
	}
	
	aio->is_running = true;
	printf("======>   PMEM_INFO: start %zu AIO threads\n", n_segments);

	// Start threads
	for (i = 0; i < n_segments; ++i) {

		//AIO_THREAD_PARAM param;
		//param.pmw = pmw;
		//param.seg_idx = i;

		//start the worker threads
		//ret = __wt_thread_create(session, &aio->worker_tids[i],
		//    pm_aio_worker, &param);
		ret = __wt_thread_create(session, &aio->worker_tids[i],
		    pm_aio_worker, pmw);
	}

	printf("======>   END start %zu AIO threads\n", n_segments);
	return aio;	
err:
	return NULL;
}

void AIO_destroy(WT_SESSION_IMPL* session,
		AIO* aio) {

	ulint i;
    int ret;
	
    __wt_spin_lock(session, &aio->aio_lock);
	aio->is_running = false;
	if (aio->n_workers > 0) {
		//wait until all AIO threads close
		__wt_spin_unlock(session, &aio->aio_lock);
		//Last calls
		for (i = 0; i < aio->n_segs; ++i) {
			__wt_cond_signal(aio->worker_sessions[i], aio->aio_req_conds[i]);
		}
		__wt_cond_wait(session, aio->all_aio_closed_cond, 0, NULL);
		printf("All AIO threads are closed, release resources\n");
	}
	else
		__wt_spin_unlock(session, &aio->aio_lock);
	// (1) Events, locks
	 free(aio->events);

	 __wt_spin_destroy(session, &aio->aio_lock);
	 __wt_cond_destroy(session, &aio->not_full_cond);
	 __wt_cond_destroy(session, &aio->is_aio_req_cond);
	 __wt_cond_destroy(session, &aio->all_aio_closed_cond);

	//(2) Segments
	for (i = 0; i < aio->n_segs; ++i) {
		free(aio->seg_wrapper_arr[i].ppiocb);
	    __wt_spin_destroy(session, &aio->seg_locks[i]);
	}
	free(aio->seg_wrapper_arr);
    free(aio->seg_locks);

	//(3) aio context
	free(aio->aio_ctx);

	//(4) Slots	
	free(aio->slots);

	//(5) Thread handler
	for (i = 0; i < aio->n_segs; i++) {
		ret = __wt_thread_join(session, aio->worker_tids[i]);
	    __wt_cond_destroy(aio->worker_sessions[i], &aio->aio_req_conds[i]);
	}
	free (aio->worker_tids);
	free (aio->worker_sessions);	
    free (aio->aio_req_conds);
	free (aio);
}

/** Creates an io_context for native linux AIO.
@param[in]	max_events	number of events
@param[out]	io_ctx		io_ctx to initialize.
@return true on success. */
bool
linux_create_io_ctx(
	ulint		max_events,
	io_context_t*	io_ctx)
{
	ssize_t		n_retries = 0;

	for (;;) {

		memset(io_ctx, 0x0, sizeof(*io_ctx));

		/* Initialize the io_ctx. Tell it how many pending
		IO requests this context will handle. */

		int	ret = io_setup(max_events, io_ctx);

		if (ret == 0) {
			/* Success. Return now. */
			return(true);
		}

		/* If we hit EAGAIN we'll make a few attempts before failing. */

		switch (ret) {
		case -EAGAIN:
			if (n_retries == 0) {
				/* First time around. */
				printf("io_setup() failed with EAGAIN. Will make attempts before giving up.\n");
			}

			if (n_retries < OS_AIO_IO_SETUP_RETRY_ATTEMPTS) {

				++n_retries;
				printf("io_setup() attempt %zu\n", n_retries);

				__wt_sleep(0, OS_AIO_IO_SETUP_RETRY_SLEEP);
				continue;
			}
			
			/* Have tried enough. Better call it a day. */
			printf("io_setup() failed with EAGAIN after %d attemps\n", OS_AIO_IO_SETUP_RETRY_ATTEMPTS);
			break;

		case -ENOSYS:
		printf("Linux Native AIO interface is not supported on this platform. Please check your OS documentation and install appropriate binary of InnoDB.\n");
			break;

		default:
		printf("Linux Native AIO setup returned following error[%d]\n", ret);
			break;
		}

		break;
	}
	return(false);
}
/* Process AIO in batch of slots
 * One batch include a n slots with n is the size of the block from pmem
 * */
int 
pm_process_batch(
		PMEM_WRAPPER* pmw,
		PMEM_BUF_BLOCK_LIST* plist
		) {
	
	int ret;	
    //int lock_ret;
	//ulint		slots_per_seg;
	ulint		n_seg_need;

	AIO* aio = (AIO*) pmw->aio;
	WT_SESSION_IMPL* session = pmw->session;
	PMEM_BUF_BLOCK_LIST* pnext_list;
	//WT_SESSION_IMPL* worker_session;

	ulint		local_seg;
	ulint		io_ctx_index;
	
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
    printf("[3] BEGIN pm_process_batch() for plish %zu\n", plist->list_id);
#endif
	/*
	 * if n_params < slots_per_seg: just need 1 seg to handle (waste, because write thread can handle more params)
	 * if n_params == slots_per_seg: just need 1 seg to handle (OK)
	 * if n_params > slots_per_seg: need more segs to handle one batch_aio
	 * */	
	n_seg_need = (plist->cur_pages + aio->n_slots_per_seg - 1) / aio->n_slots_per_seg;
	//Wait in case current array is full
check_seg:
	for (;;) {

		//IMPORTANT NOTE: waiting until there is no older list in the linked-list
		pnext_list = D_RW(plist->next_list);
		//if (pnext_list != NULL && 
		//	pnext_list->state == PMEM_LIST_PROPAGATING){
		if (pnext_list != NULL){
			int hashed = plist->hashed_id;

			assert(hashed == pnext_list->hashed_id);
#if defined (UNIV_PMEMOBJ_BUF_DEBUG)
			printf("NNNN [3] this list %zu with hashed_id %d is waiting for the older list %zu with state %d finish propagating ... \n", plist->list_id, hashed, pnext_list->list_id, pnext_list->state);
#endif //UNIV_PMEMOBJ_BUF_DEBUG
			plist->state = PMEM_LIST_WAIT_PROP;

			__wt_cond_wait(session, pmw->pbuf->prev_list_flush_conds[hashed], 0, NULL); //see pm_handle_finished_list_with_flusher 
			//wait....
			
			printf("this list %zu wakeup and check again\n", plist->list_id);
			plist->state = PMEM_LIST_PREP_PROP;
			goto check_seg;
		}

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
    printf("[3] plist %zu try to acquire aio_lock, reserved_segs %zu / total segs %zu... \n", plist->list_id, aio->n_seg_reserved, aio->n_segs);
#endif
		//lock_ret = __wt_spin_trylock(session, &aio->aio_lock);
		__wt_spin_lock(session, &aio->aio_lock);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
    printf("[3] OK! plist %zu acquired aio_lock! \n", plist->list_id);
#endif

		if (aio->n_seg_reserved + n_seg_need < aio->n_segs) {
            //Still hold the lock
			break;
		}
        
       // if (lock_ret == 0)
	   //     __wt_spin_unlock(session, &aio->aio_lock);
		__wt_spin_unlock(session, &aio->aio_lock);

        printf("\t[3] plist %zu wait in pm_process_batch().... Detail: reserved segs %zu + n_segs need %zu >= the total segs %zu\n", plist->list_id, aio->n_seg_reserved, n_seg_need, aio->n_segs);
		__wt_cond_wait(session, aio->not_full_cond, 0, NULL);
	} //end for

    //Still hold the aio_lock
	
    ulint		count, i, slot_i;
	Slot*	slot = NULL;
	PMEM_SEG_WRAPPER* wrapper;
	PMEM_BUF_BLOCK* pblock;
	PMEM_BUF*       pmem_buf;
	byte* pdata;
	WT_FH* fh_cur;
	WT_FILE_HANDLE_POSIX *pfh;
	int fd;


	pmem_buf = pmw->pbuf;
	pdata = pmem_buf->p_align;
	assert(plist!=NULL);

	//(1) Find the next free segment
	//local_seg = aio->n_segs + 1;
	local_seg = aio->next_free_seg;

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
    printf("\t[3.1] BEGIN find the next free segemnt for plist %zu current next_free_seg %zu\n", plist->list_id, local_seg);
#endif
	for (i = 0; i < aio->n_segs; ++i) {
		//if (aio->seg_wrapper_arr[i].is_reserved == false) {
		if (aio->seg_wrapper_arr[local_seg].is_reserved == false) {
            
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
    printf("\t[3.1] try to acquire lock of seg %zd, plist %zu... \n", local_seg, plist->list_id);
#endif
		     //__wt_spin_lock(aio->worker_sessions[local_seg], &aio->seg_locks[local_seg]);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
    printf("\t[3.1] OK! acquired lock of seg %zd plist %zu\n", local_seg, plist->list_id);
#endif
			aio->seg_wrapper_arr[local_seg].is_reserved = true;
			++aio->n_seg_reserved;
            //Update the next_free_seg value
            aio->next_free_seg = (local_seg + 1) % aio->n_segs;

		     //__wt_spin_unlock(aio->worker_sessions[local_seg], &aio->seg_locks[local_seg]);
            //release the outer lock also
		    __wt_spin_unlock(session, &aio->aio_lock);
			break;
		} //end if
        //The next segment
        local_seg = (local_seg + 1) % aio->n_segs;
	} // end for 

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
    printf("\t[3.1] END find the next free segemnt %zu plist %zu \n", local_seg, plist->list_id);
#endif
	//If there is a free lot in this array than there is at least one free segment 	
	if (i >= aio->n_segs){
		//there is 
		printf("*** All segs are busy, waiting..., i %zu > n_segs %zu, aio->n_seg_reserved = %zu\n", i, aio->n_segs, aio->n_seg_reserved);

		__wt_spin_unlock(session, &aio->aio_lock);
        goto check_seg;
	}

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
    printf("\n\t[3.2] BEGIN Transfers params from plist %zu to slots at seg %zu\n", plist->list_id, local_seg);
#endif
    //worker_session = aio->worker_sessions[local_seg];

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
    printf("\n\t[3.2] try acquire lock from plist %zu to slots at seg %zu...\n", plist->list_id, local_seg);
#endif
//	__wt_spin_lock(aio->worker_sessions[local_seg], &aio->seg_locks[local_seg]);

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
    printf("\n\t[3.2] OK! acquired lock from plist %zu to slots at seg %zu!\n", plist->list_id, local_seg);
#endif
	wrapper = &aio->seg_wrapper_arr[local_seg];
	assert (wrapper->local_index == local_seg);
	wrapper->io_pending = 0;
	wrapper->io_finished = 0;
	wrapper->list_id = plist->list_id;

	//(2) transfers param to each slot in the segment
	count = 0;
	for (i = 0, slot_i = local_seg * aio->n_slots_per_seg;
		   	i < plist->cur_pages && slot_i < aio->n_slots;
		   	++slot_i, ++i) {
		
		if (wrapper->io_pending == aio->n_slots_per_seg){
			break;
		}	
		slot = &aio->slots[slot_i];
		assert(slot->is_reserved == false);	
		//Get the block i in the list
		pblock = D_RW(D_RW(plist->arr)[i]);	
		//Found a free slot
		++aio->n_slot_reserved;
		
		// (2.1) The block
		if (pblock->state == PMEM_FREE_BLOCK) {
			printf("Check this case!!!\n");
			assert(0);
		}
		pblock->state = PMEM_IN_FLUSH_BLOCK;
		ret =__wt_open(session, pblock->file_name, WT_FS_OPEN_FILE_TYPE_DATA, 0, &fh_cur);
		pfh = (WT_FILE_HANDLE_POSIX *) (fh_cur->handle);
		fd = pfh->fd;
        
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
//        printf("[3.2] pfh name %s pfh->direct_io = %d\n",fh_cur->name,  pfh->direct_io);
#endif
		
		//(2.2) Assign data in slot
		
		off_t offset = (off_t) pblock->disk_off;
#if defined (CHECKSUM_DEBUG)
		uint32_t checksum_tem, checksum2;
		uint32_t checksum1 = pblock->checksum;
		//Recompute the checksum of the data block in pmem
		WT_BLOCK_HEADER *blk;

		blk = WT_BLOCK_HEADER_REF(pdata + pblock->pmemaddr);
		checksum_tem = blk->checksum;
		blk->checksum = 0;
		checksum2 = __wt_checksum(pdata + pblock->pmemaddr, pblock->size);
		blk->checksum = checksum_tem;
		//Check the checksum right before io_submit()
		if (checksum1 != checksum2){
			printf("CHECKSUM ERROR #3 (io_submit): offset %zu size %zu flush checksum1 %u differs to propagating checksum2 %u, blk->checksum %u\n", offset, pblock->size, checksum1, checksum2, checksum_tem);
		}
#endif //CHECKSUM_DEBUG
		slot->is_reserved = true;
		slot->is_aio_completed = false;
		//slot->reservation_time = time(NULL);
		gettimeofday(&slot->reservation_time,NULL);
		slot->m1       = pblock;
		slot->file     = fh_cur;
		slot->len      = pblock->size;
		slot->buf      = pdata + pblock->pmemaddr;

		slot->ptr      = slot->buf;
		slot->offset   = offset;
		slot->err      = PMEM_SUCCESS;

		//(2.3) Prepare pwrite
		off_t		aio_offset = offset;
		struct iocb*	iocb = &slot->control;

		wrapper->ppiocb[wrapper->io_pending] = iocb;
		++wrapper->io_pending;
		//io_prep_pwrite() is defined from libaio.h, call this function to prepare pwrite in iocb struct
		io_prep_pwrite(iocb, fd, slot->ptr, slot->len, aio_offset);

		iocb->data = slot;
		slot->n_bytes = 0;
		slot->ret = 0;

	} //end for

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
    printf("\n\t[3.2] END Transfers params from plist %zu to slots at seg %zu ios to be submited %zd\n", plist->list_id, local_seg, wrapper->io_pending);
#endif
	//(3) AIO Submit in batch 
	io_ctx_index = local_seg;
	//io_submit() is defined in libaio.h
#if defined(UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
    printf("\n\t[3.3] BEGIN io_submit for list %zu signal worker %zu, #iocbs %zu\n", plist->list_id, local_seg, wrapper->io_pending);
    struct timeval t1, t2;

    gettimeofday(&t1, NULL);
#endif
	
	//set state before io_submit()
	plist->state = PMEM_LIST_PROPAGATING;

	ret = io_submit(
			aio->aio_ctx[io_ctx_index],
			wrapper->io_pending,
			wrapper->ppiocb);
	
	//(4) Check the ret
	if (ret != (int) wrapper->io_pending){
		printf("PMEM_AIO ERROR: # of submitted IO %d differs from # of expected %zu\n", ret, wrapper->io_pending);
		assert(0);
	}

	//(5) Trigger the waiting AIO write 
    WT_CONDVAR* req_cond = aio->aio_req_conds[local_seg];
	//__wt_cond_signal(session, aio->is_aio_req_cond);
#if defined(UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
    //time_t end_t = time(NULL);
    gettimeofday(&t2, NULL);
    double diff_usec = (t2.tv_sec - t1.tv_sec)*1000000 + (t2.tv_usec - t1.tv_usec);

    printf("\n\t[3.3] END io_submit for list %zu, it takes %f micro seconds, signal worker %zu ret %d\n", plist->list_id, diff_usec, local_seg, ret);
#endif
	__wt_cond_signal(aio->worker_sessions[local_seg], req_cond);
	//__wt_cond_signal(session, req_cond);

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
    printf("[3] END pm_process_batch() for plish %zu\n", plist->list_id);
#endif

	//__wt_spin_unlock(aio->worker_sessions[local_seg], &aio->seg_locks[local_seg]);
	return PMEM_SUCCESS;
}
/////////////////////////////////////////
//// The AIO thread
//The caller should send the arg as AIO_THREAD_PARAM
/////////////////////////////////////////

//This implement is for aio complete by a slot
/*
static void* pm_aio_worker (void* arg) {
    //int ret;
    int lock_ret;
	WT_SESSION_IMPL *session;
	WT_SESSION_IMPL *worker_session;
	ulint seg_idx;
	
	AIO_THREAD_PARAM* param = (AIO_THREAD_PARAM*) arg;
	assert(param);

	PMEM_WRAPPER* pmw = param->pmw;

	AIO* aio = pmw->aio;
	PMEM_SEG_WRAPPER* seg_wrapper;
    PMEM_BUF_BLOCK* pblock_out = NULL;
    PMEM_BUF_BLOCK_LIST* plist_out = NULL;

	session = pmw->session;	
	seg_idx = param->seg_idx;
	seg_wrapper = &aio->seg_wrapper_arr[seg_idx];

    worker_session = aio->worker_sessions[seg_idx];

	lock_ret = __wt_spin_trylock(worker_session, &aio->aio_lock);
	//__wt_spin_lock(worker_session, &aio->aio_lock);
	aio->n_workers++;
    if (lock_ret == 0)
	    __wt_spin_unlock(worker_session, &aio->aio_lock);

	//The thread for loop
	for (;;) {
		//wait for a AIO request issue
		//__wt_cond_wait(session, aio->is_aio_req_cond, 0, NULL);
		__wt_cond_wait(aio->worker_sessions[seg_idx],
                aio->aio_req_conds[seg_idx], 0, NULL);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
        printf("[4] BEGIN aio_worker_thread wakeup,seg_idx = %zu\n", seg_idx);
#endif        
			//This is trigger by AIO producer. Repeat poll() to collect any complete AIO slot until all slots are completed
		while (seg_wrapper->io_finished < seg_wrapper->io_pending){
            //Get one aio completed page (block)
			pblock_out = poll(pmw, aio, seg_idx);
            //plist_out = collect(pmw, aio, seg_idx);
            //printf("after collect() plist_out->id=%zu\n", plist_out->list_id);

            if (pblock_out == NULL){
                printf("**** poll() return pblock_out == NULL, ERROR!\n");
                assert(0); 
            }

	        TOID(PMEM_BUF_BLOCK_LIST) flush_list;
	        TOID_ASSIGN(flush_list, pblock_out->list.oid);
	        plist_out = D_RW(flush_list);

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
        printf("\t[4.1] After poll  pblock_out offset %zu plist_out %zu seg_idx = %zu\n", 
                pblock_out->disk_off, plist_out->list_id, seg_idx);
#endif        
			if (seg_wrapper->io_finished == seg_wrapper->io_pending){
				handle_AIO_seg_complete(pmw, aio, seg_idx, plist_out);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
                printf("after handle_AIO_seg_complete() plist_out->id=%zu \n", plist_out->list_id);
#endif        
				break;
			}
            //continue get next block
		}//end while

		if (!aio->is_running){
			break;
		}
	}// end the thread for loop

	lock_ret = __wt_spin_trylock(worker_session, &aio->aio_lock);
	//__wt_spin_lock(worker_session, &aio->aio_lock);
	aio->n_workers--;
	if (aio->n_workers == 0){
		printf("PMEM_INFO: close the last aio worker thread \n");
	}
    if (lock_ret == 0)
	    __wt_spin_unlock(worker_session, &aio->aio_lock);

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
        printf("[4] END aio_worker_thread wakeup,seg_idx = %zu\n", seg_idx);
#endif        
	return 0;
}
*/

//This implement for aio complete a whole segment
static void* pm_aio_worker (void* arg) {
    //int ret;
    //int lock_ret;
	WT_SESSION_IMPL *session;
	//WT_SESSION_IMPL *worker_session;
	ulint seg_idx;
	
	//AIO_THREAD_PARAM* param = (AIO_THREAD_PARAM*) arg;
	//assert(param);

	//PMEM_WRAPPER* pmw = param->pmw;
	PMEM_WRAPPER* pmw = (PMEM_WRAPPER*) arg;
    assert(pmw);

	AIO* aio = pmw->aio;
	PMEM_SEG_WRAPPER* seg_wrapper;
    //PMEM_BUF_BLOCK* pblock_out = NULL;
    PMEM_BUF_BLOCK_LIST* plist_out = NULL;

	session = pmw->session;	

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
        //printf("[4] aio_worker_thread seg_idx = %zu, try to acquire aio_lock...\n", seg_idx);
#endif        

	__wt_spin_lock(session, &aio->aio_lock);

	//seg_idx = param->seg_idx;
	seg_idx = aio->n_workers;
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
        printf("[4] OK! aio_worker_thread seg_idx = %zu, acquired aio_lock! aio->n_workers %zu\n", seg_idx, aio->n_workers);
#endif        

    //worker_session = aio->worker_sessions[seg_idx];

	aio->n_workers++;
    //if (lock_ret == 0)
	__wt_spin_unlock(session, &aio->aio_lock);

	//The thread for loop
        for (;;) {
            //wait for a AIO request issue
            //__wt_cond_wait(session, aio->is_aio_req_cond, 0, NULL);
            __wt_cond_wait(aio->worker_sessions[seg_idx],
                    aio->aio_req_conds[seg_idx], 0, NULL);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
            printf("[4] BEGIN aio_worker_thread wakeup,seg_idx = %zu\n", seg_idx);
            printf("[4.1] try acquire seg lock of ,seg_idx = %zu...\n", seg_idx);
#endif        

 //           __wt_spin_lock(aio->worker_sessions[seg_idx], &aio->seg_locks[seg_idx]);

            seg_wrapper = &aio->seg_wrapper_arr[seg_idx];
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
            printf("[4.1] OK! acquired seg lock of ,seg_idx = %zu, seg_wrapper->io_finished %zu io_pending %zu...\n", seg_idx, seg_wrapper->io_finished, seg_wrapper->io_pending);
#endif        
            //This is trigger by AIO producer. Repeat poll() to collect any complete AIO slot until all slots are completed
            while (seg_wrapper->io_finished < seg_wrapper->io_pending){
                plist_out = collect_list(pmw, aio, seg_idx);
                if (plist_out == NULL) {
                    //The server is shutdown or there is an error
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
                printf("\t[4.2] After collect  plist_out is NULL !!!  seg_idx = %zu\n", seg_idx);
#endif        
                    break;
                }

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
                printf("\t[4.2] After collect  plist_out %zu seg_idx = %zu\n", plist_out->list_id, seg_idx);
#endif        
                if (seg_wrapper->io_finished == seg_wrapper->io_pending){
                    handle_AIO_seg_complete(pmw, aio, seg_idx, plist_out);


#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
                printf("\t[4.3] Try to acquire aio_lock  plist_out %zu seg_idx = %zu...\n", plist_out->list_id, seg_idx);
#endif        
                    __wt_spin_lock(session, &aio->aio_lock);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
                printf("\t[4.3] OK! acquired aio_lock  plist_out %zu seg_idx = %zu\n", plist_out->list_id, seg_idx);
#endif        
                    --aio->n_seg_reserved;

                    if (aio->n_seg_reserved <= aio->n_segs - 1){
                        __wt_cond_signal(session, aio->not_full_cond);
                    }
                    __wt_spin_unlock(session, &aio->aio_lock);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
                    printf("after handle_AIO_seg_complete() plist_out->id=%zu seg_idx %zu \n", plist_out->list_id, seg_idx);
#endif        
                    break;
                }

            }//end while

//            __wt_spin_unlock(aio->worker_sessions[seg_idx], &aio->seg_locks[seg_idx]);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
                    printf("[4.3] aio worker %zu goto wait again.\n", seg_idx);
#endif        
            if (!aio->is_running){
                break;
            }
        }// end the thread for loop

        //lock_ret = __wt_spin_trylock(worker_session, &aio->aio_lock);
        //__wt_spin_lock(worker_session, &aio->aio_lock);
        __wt_spin_lock(session, &aio->aio_lock);
        aio->n_workers--;
        if (aio->n_workers == 0){
            printf("PMEM_INFO: close the last aio worker thread \n");
			__wt_cond_signal(session, aio->all_aio_closed_cond);
        }
        //if (lock_ret == 0)
        //__wt_spin_unlock(worker_session, &aio->aio_lock);
        __wt_spin_unlock(session, &aio->aio_lock);

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
        printf("[4] END aio_worker_thread wakeup,seg_idx = %zu\n", seg_idx);
#endif        
        return 0;
}

/*
 *Handle a completed segment
 The caller thread has owned the segment seg_idx  lock
 The caller thread must not own the aio_lock
 Do not handle any lock in this function
 * */

void handle_AIO_seg_complete(
		PMEM_WRAPPER *pmw, 
				AIO* aio,
				ulint seg_idx,
				PMEM_BUF_BLOCK_LIST* plist
				){
	
	WT_FH ** fh_arr;
	WT_FH* fh_cur;
	uint32_t i, fh_i, fh_cnt;
	int ret;
	ulint slot_i;
	ulint slots_per_seg;
	Slot* slot;
    //int lock_ret;

	WT_SESSION_IMPL *session = pmw->session;
    //WT_SESSION_IMPL *worker_session = aio->worker_sessions[seg_idx];
	PMEM_SEG_WRAPPER* seg_wrapper;
	seg_wrapper = &aio->seg_wrapper_arr[seg_idx];
    
    slots_per_seg = aio->n_slots / aio->n_segs;

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
        printf("[5] BEGIN handle_AIO_seg_complete() seg_idx = %zu, plist %zu\n", seg_idx, plist->list_id);
#endif        

	//pm_handle_finished_list_with_flusher(pmw, plist);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
        printf("\t[5.2] BEGIN reset slots seg_idx = %zu, plist %zu\n", seg_idx, plist->list_id);
#endif        
	//lock_ret = __wt_spin_trylock(session, &aio->seg_locks[seg_idx]);
	fh_arr = (WT_FH**) calloc(plist->max_pages, sizeof(WT_FH*));
	
	fh_cnt = 0;
	slot_i = seg_idx * slots_per_seg;
	
	//(2) Reset slots and Collect fhs for fsync()
	for (i = 0; i < plist->max_pages; ++i, ++slot_i) {
		//Add the file handler to the list for flushing 
		slot = &aio->slots[slot_i];

		fh_cur = slot->file;

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
#if defined (CHECKSUM_DEBUG)
		PMEM_BUF_BLOCK* pblock;
		uint32_t checksum_tem, checksum2;
		WT_BLOCK_HEADER *blk;
	
		//Check the checksum after io_submit()
		pblock = (PMEM_BUF_BLOCK*) slot->m1;
		uint32_t checksum1 = pblock->checksum;
		blk = WT_BLOCK_HEADER_REF(slot->buf);
		checksum_tem = blk->checksum;
		blk->checksum = 0;
		checksum2 = __wt_checksum(slot->buf, pblock->size);
		blk->checksum = checksum_tem;
		if (checksum1 != checksum2){
			printf("CHECKSUM ERROR #4 (AIO finished): offset %zu size %zu flush checksum1 %u differs to propagating checksum2 %u, blk->checksum %u\n", slot->offset, pblock->size, checksum1, checksum2, checksum_tem);
		}

#endif
		slot->is_reserved = false;
		slot->is_aio_completed = false;
		slot->ret = 0;
		slot->n_bytes = 0;
		memset(&slot->control, 0x0, sizeof(slot->control));
	}

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
        printf("\t[5.2] END reset slots seg_idx = %zu, plist %zu\n", seg_idx, plist->list_id);
        printf("\t[5.3] BEGIN call fsync  seg_idx = %zu, plist %zu # files to fsyncs %u\n", seg_idx, plist->list_id, fh_cnt);
#endif        
	//(3) Call fsyncs
	for (fh_i = 0; fh_i < fh_cnt; ++fh_i) {
        //Check this fsync
		//__wt_fsync(session, fh_arr[fh_i], true);
		__wt_fsync(session, fh_arr[fh_i], false);
	}

	//This loop just decrease the fh->ref to balance __wt_open() / __wt_close()
	slot_i = seg_idx * slots_per_seg;
	for (i = 0; i < plist->max_pages; ++i, ++slot_i) {
		slot = &aio->slots[slot_i];
		ret = __wt_close(session, &slot->file);
		slot->file = NULL;
	}

	//(4) free allocate 
	free (fh_arr);
	fh_arr = NULL;

	//(5) Handle the seg wrapper
	seg_wrapper->io_finished = seg_wrapper->io_pending = 0;
	seg_wrapper->is_reserved = false;

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
        printf("\t[5.3] END call fsync  seg_idx = %zu, plist %zu # files to fsyncs %u\n", seg_idx, plist->list_id, fh_cnt);
        printf("\t[5.1] BEGIN pm_handle_finished_list seg_idx = %zu, plist %zu\n", seg_idx, plist->list_id);
#endif        
	//(1) Handle the finish list in pm
	//////////////////////////////////////////////////
	pm_handle_finished_list_with_flusher(pmw, plist);
	/////////////////////////////////////////////////
	
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
        printf("\t[5.1] END pm_handle_finished_list seg_idx = %zu, plist %zu\n", seg_idx, plist->list_id);
        printf("[5] END handle_AIO_seg_complete() seg_idx = %zu, plist %zu\n", seg_idx, plist->list_id);
#endif        
}

// Call by a aio worker thread
// Use find_completed_slot() to get a completed aio slot
// If there is no completed aio slot, use poll_slot() to call getevents() 
PMEM_BUF_BLOCK*
 poll(PMEM_WRAPPER *pmw, 
				AIO* aio,
			   	ulint seg_idx
				){
    //
//This method use getevents() for whole slots ///////
    //PMEM_BUF_BLOCK_LIST* plist_out = NULL;
    //plist_out = collect(pmw, aio, seg_idx);
    //return PMEM_SUCCESS;
/////////////////////////////////////////////////////

// This method use getevents() for one slot /////////////////
    PMEM_BUF_BLOCK* pblock_out;
    int ret;
	Slot*		slot;
	ulint n_pending;
	WT_SESSION_IMPL *session = pmw->session;
	PMEM_FLUSHER* flusher = pmw->pbuf->flusher;

	n_pending = 0;

	/* Loop until we have found a completed request. */
	for (;;) {

		slot = find_completed_slot(session, aio, seg_idx, &n_pending);

		if (slot != NULL) {
			break;
		}
		else if (flusher->is_running == false && n_pending == 0){
			//If the server is shutdown and there is no IO pending 
			pblock_out = NULL;
			return PMEM_SUCCESS;
		}
		else {
			//There is no io completed
			//This can happend when the server start or when the system idle
			//In collect(), call io_getevents()
			ret = collect_slot(pmw, aio, seg_idx);
            if (ret != PMEM_SUCCESS)
                return NULL; // ERROR
		}
	} // end for
	//Note that find_completed_slot set the lock, so we should release the lock here
	//	__wt_spin_unlock(session, &aio->aio_lock);
	//At this point, there is an AIO slot complete 
	pblock_out = (PMEM_BUF_BLOCK*) slot->m1;

	return pblock_out;
} //end poll

/////////////////////////
// Find the completed slot in AIO
// This function is called in the loop of AIO thread
// aio [in]: The AIO
// seg_idx [in]: segment index
// n_pending [out]: number of pending slots in the segment
Slot* find_completed_slot(WT_SESSION_IMPL *session, 
				AIO* aio,
			   	ulint seg_idx,
			   	ulint* n_pending){
		ulint slot_i;

		slot_i = aio->n_slots_per_seg * seg_idx; 

		//__wt_spin_lock(session, &aio->aio_lock);
        //Start with the first slot of the seg
		Slot*	slot = &aio->slots[slot_i];

		for (ulint i = 0; i < aio->n_slots_per_seg; ++i, ++slot) {
			if (slot->is_reserved && slot->is_aio_completed) {
				++*n_pending;
					return(slot);
			}
		}
		//__wt_spin_unlock(session, &aio->aio_lock);
		return NULL;
}

// Collection AIO complete for a slot
// Work base on the value of ret = io_getevents()
// If ret == 0, try again as the loop
// If ret > 0, get data from the complete AIO and return
// Get the completed slot and set the flags for that slot
int
 collect_slot(PMEM_WRAPPER *pmw, 
				AIO* aio,
			   	ulint seg_idx
			   	){
    //int lock_ret;
    //PMEM_BUF_BLOCK_LIST* plist_out;

	ulint slots_per_seg = aio->n_slots / aio->n_segs;
    
	//WT_SESSION_IMPL *session = pmw->session;
	PMEM_FLUSHER* flusher = pmw->pbuf->flusher;
	PMEM_SEG_WRAPPER* seg_wrapper = &aio->seg_wrapper_arr[seg_idx];
	WT_SESSION_IMPL *worker_session;

    //Output
    //PMEM_BUF_BLOCK* pblock_out;
	//TOID(PMEM_BUF_BLOCK_LIST) flush_list;

    worker_session = aio->worker_sessions[seg_idx];

	//Start at the first slot of the segment
	//ulint	start_pos = seg_idx * aio->n_slots;
	ulint	start_pos = seg_idx * slots_per_seg;
	//ulint	end_pos = start_pos + aio->n_slots;
	for (;;) {
		//struct io_event from libaio.h
		struct io_event*	events;
		events = &aio->events[start_pos];
		/* (1) Reset the events. */
		memset(events, 0, sizeof(*events) * slots_per_seg);
		//How long we can wait for an AIO
		struct timespec		timeout;
		timeout.tv_sec = 0;
		timeout.tv_nsec = OS_AIO_REAP_TIMEOUT;

		int	ret;
		// (2) call io_getevents() from libaio.h
		//ret = io_getevents(aio->aio_ctx[seg_idx], 1, aio->n_slots, pevent, &timeout);
		ret = io_getevents(aio->aio_ctx[seg_idx], 1, slots_per_seg, events, &timeout);

        //Check data persistent 
		for (int i = 0; i < ret; ++i) {
			struct iocb*	iocb;
			iocb = (struct iocb*)(events[i].obj);
			assert(iocb != NULL);

			Slot*	out_slot = (Slot*)(iocb->data);
			assert (out_slot != NULL);
			assert (out_slot->is_reserved);
			assert (out_slot->pos >= start_pos);
			//Get the aio slot using the out_slot->pos
            Slot* in_slot = &aio->slots[out_slot->pos];
            //Check
            if (in_slot->offset != out_slot->offset){
                printf("AIO error, the input Slot offset %zu differs to the output Slot offset %zu \n", in_slot->offset, out_slot->offset);
                assert(0);
            }
            out_slot = in_slot;

			//__wt_spin_lock(worker_session, &aio->aio_lock);
			//lock_ret = __wt_spin_trylock(worker_session, &aio->aio_lock);
	        //__wt_spin_lock(session, &aio->seg_locks[seg_idx]);

			seg_wrapper->io_finished++;

            in_slot->is_aio_completed = true;
			in_slot->err = PMEM_SUCCESS;
			in_slot->ret = events[i].res2;
			in_slot->n_bytes = events[i].res;
            //pblock_out = (PMEM_BUF_BLOCK*) in_slot->m1;
            
        //   if (lock_ret == 0) 
		//	    __wt_spin_unlock(worker_session, &aio->aio_lock);
	        //__wt_spin_unlock(session, &aio->seg_locks[seg_idx]);
		} //end inner for
		
		if (flusher->is_running == false || ret > 0){
			//break if the server is shutdown or there is some complete AIO
			break;
		}
		// There is no IO complete
		switch (ret) {
		case -EAGAIN:
			/* Not enough resources! Try again. */
		case -EINTR:
		case 0:
			/* No pending request! Go back and check again. */
			continue;
		}
		//If the code reach here, there is some errors
		printf("in collect(), io_getevents() return error %d", ret);
		return PMEM_ERROR;
		//return NULL;
	}//end outer for

	//TOID_ASSIGN(flush_list, pblock_out->list.oid);
	//plist_out = D_RW(flush_list);
    return PMEM_SUCCESS;
    //return plist_out;
}

/*
 *Collect the AIO complete for a list of the input segment
 The caller must own the segment lock
 * */
PMEM_BUF_BLOCK_LIST*
 collect_list(PMEM_WRAPPER *pmw, 
				AIO* aio,
			   	ulint seg_idx
			   	){
    //int lock_ret;
    PMEM_BUF_BLOCK_LIST* plist_out = NULL;

	ulint slots_per_seg = aio->n_slots / aio->n_segs;
    
	//WT_SESSION_IMPL *session = pmw->session;
	PMEM_FLUSHER* flusher = pmw->pbuf->flusher;
	PMEM_SEG_WRAPPER* seg_wrapper = &aio->seg_wrapper_arr[seg_idx];
	WT_SESSION_IMPL *worker_session;

    //Output
    PMEM_BUF_BLOCK* pblock_out;
	TOID(PMEM_BUF_BLOCK_LIST) flush_list;
	TOID(PMEM_BUF_BLOCK_LIST) flush_list_tem;

    worker_session = aio->worker_sessions[seg_idx];

	//Start at the first slot of the segment
	//ulint	start_pos = seg_idx * aio->n_slots;
	ulint	start_pos = seg_idx * slots_per_seg;
	//ulint	end_pos = start_pos + aio->n_slots;
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
    printf("\t[4.2] BEGIN collect_list for  seg_idx = %zu\n", seg_idx);
#endif        
	for (;;) {
		//struct io_event from libaio.h
		struct io_event*	events;
		events = &aio->events[start_pos];
		/* (1) Reset the events. */
		//memset(pevent, 0, sizeof(*pevent) * aio->n_slots);
		memset(events, 0, sizeof(*events) * slots_per_seg);
		
		//How long we can wait for an AIO
		struct timespec		timeout;
		timeout.tv_sec = 0;
		timeout.tv_nsec = OS_AIO_REAP_TIMEOUT;

		int	ret;
		// (2) call io_getevents() from libaio.h
		//ret = io_getevents(aio->aio_ctx[seg_idx], 1, aio->n_slots, pevent, &timeout);
		ret = io_getevents(aio->aio_ctx[seg_idx], slots_per_seg, slots_per_seg, events, &timeout);

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
    printf("\t[4.2] io_getevents() ret %d  seg_idx = %zu\n", ret, seg_idx);
#endif        
        //Check data persistent 
		for (int i = 0; i < ret; ++i) {
			struct iocb*	iocb;
			iocb = (struct iocb*)(events[i].obj);
			assert(iocb != NULL);

			Slot*	out_slot = (Slot*)(iocb->data);
			assert (out_slot != NULL);
			assert (out_slot->is_reserved);
			assert (out_slot->pos >= start_pos);
			//Get the aio slot using the out_slot->pos
            Slot* in_slot = &aio->slots[out_slot->pos];
            //Check
            if (in_slot->offset != out_slot->offset){
                printf("AIO error, the input Slot offset %zu differs to the output Slot offset %zu \n", in_slot->offset, out_slot->offset);
                assert(0);
            }
            out_slot = in_slot;

	        //__wt_spin_lock(session, &aio->seg_locks[seg_idx]);

			seg_wrapper->io_finished++;
            
            in_slot->is_aio_completed = true;
			in_slot->err = PMEM_SUCCESS;
			in_slot->ret = events[i].res2;
			in_slot->n_bytes = events[i].res;

			assert (in_slot->n_bytes == (ssize_t)in_slot->len);

            pblock_out = (PMEM_BUF_BLOCK*) in_slot->m1;
            
            TOID_ASSIGN(flush_list_tem, pblock_out->list.oid);
            plist_out = D_RW(flush_list_tem);
#if defined(UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
            //printf("==> in collect_list, seg_idx %zu plist_out %zu\n", seg_idx, plist_out->list_id);
#endif
        //   if (lock_ret == 0) 
		//	    __wt_spin_unlock(worker_session, &aio->aio_lock);
	        //__wt_spin_unlock(session, &aio->seg_locks[seg_idx]);
		} //end inner for
		
		if (flusher->is_running == false || ret > 0){
			//break if the server is shutdown or there is some complete AIO
			break;
		}
		// There is no IO complete
		switch (ret) {
		case -EAGAIN:
			/* Not enough resources! Try again. */
		case -EINTR:
		case 0:
			/* No pending request! Go back and check again. */
			continue;
		}
		//If the code reach here, there is some errors
		printf("in collect(), io_getevents() return error %d", ret);
		//return PMEM_ERROR;
		return NULL;
	}//end outer for

	TOID_ASSIGN(flush_list, pblock_out->list.oid);
	plist_out = D_RW(flush_list);
    //return PMEM_SUCCESS;
    return plist_out;
}
/////////////////// END AIO functions /////////////////////
#endif
