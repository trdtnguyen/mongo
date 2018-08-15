/*
 * Implement AIO in WiredTiger
 * */
#ifndef os0file_h
#define os0file_h


#include <libaio.h>

#if defined (UNIV_PMEMOBJ_BUF)
struct __slot;
typedef struct __slot Slot;

struct __aio;
typedef struct __aio AIO;

struct __pm_seg_wrapper;
typedef struct __pm_seg_wrapper PMEM_SEG_WRAPPER;

struct __aio_thread_param;
typedef struct __aio_thread_param AIO_THREAD_PARAM;

/** The asynchronous I/O context */
struct __slot {
	/** index of the slot in the aio array */
	uint32_t		pos;

	/** true if this slot is reserved */
	bool			is_reserved;

	/** true if this slot is aio completed */
	bool			is_aio_completed;

	/** time when reserved */
	//time_t			reservation_time;
	struct timeval		reservation_time;

	/** buffer used in i/o */
	byte*			buf;

	/** Buffer pointer used for actual IO. We advance this
	when partial IO is required and not buf */
	byte*			ptr;

	/** file offset in bytes */
	size_t		offset;

	/** file where to read or write */
	WT_FH*		file;

	/** The file node for which the IO is requested. */
	void*		m1; //fil_node_t*

	/** AIO completion status */
	int			err;

	/** Linux control block for aio */
	struct iocb		control;

	/** AIO return code */
	int			ret;

	/** bytes written/read. */
	ssize_t			n_bytes;

	/** length of the block to read or write */
	ulint			len;
}; //end struct __slot

/** The asynchronous i/o array structure */
struct __aio {
	/** the mutex protecting the aio array */
	WT_SPINLOCK aio_lock;


	/** Pointer to the slots in the array.
	Number of elements must be divisible by n_threads. */
	Slot*			slots;
	ulint			n_slots;

	/** The array to collect completed IOs. There is one such
	event for each possible pending IO. The size of the array
	is equal to m_slots.size(). */
	struct io_event*		events;
	ulint			n_events;
	
	PMEM_SEG_WRAPPER* seg_wrapper_arr;
	ulint				n_segs;
	ulint				n_slots_per_seg;
	ulint				next_free_seg;
	/** The event which is set to the signaled state when
	there is space in the aio outside the ibuf segment */
	WT_CONDVAR* not_full_cond;

	/** Number of reserved slots in the AIO array outside
	the ibuf segment */
	ulint			n_seg_reserved;
	ulint			n_slot_reserved;

	/////////// struc defined in libaio.h
	/** completion queue for IO. There is one such queue per
	segment. Each thread will work on one ctx exclusively. */
	io_context_t*		aio_ctx;

	//Thread handle
	bool is_running;
	volatile uint64_t n_workers;
	WT_SESSION_IMPL** worker_sessions; //array of internal session
	wt_thread_t*        worker_tids; //array of worker thread ids
	WT_CONDVAR*         is_aio_req_cond;
	WT_CONDVAR**         aio_req_conds;
	WT_SPINLOCK* seg_locks; //one lock per seg

	WT_FH ** fh_arr;//array of fh, allocate in DRAM, used for fsync() in AIO complete handle

}; //end struct __aio

/** The wrapper of AIO*/
struct __aio_wrapper {
	AIO** s_pm_batch_writes;
	uint64_t		n_batch;
	PMEM_SEG_WRAPPER* m_seg_wrapper_arr;
};

//temp struct for param passing when creating a AIO thread
struct __aio_thread_param {
	PMEM_WRAPPER* pmw;
	ulint seg_idx;
};

struct __pm_seg_wrapper {
	ulint			local_index;
	ulint			io_finished;
	ulint			io_pending;
	bool			is_reserved;
	struct iocb**	ppiocb;		
	//Debug
	uint64_t		list_id;
};

/////////////////// AIO functions /////////////////////
AIO* AIO_init(PMEM_WRAPPER* pmw,
	   	ulint n_slots,
		ulint n_segments);

void AIO_destroy(WT_SESSION_IMPL* session,
		AIO* aio);

bool
linux_create_io_ctx(
	ulint		max_events,
	io_context_t*	io_ctx);

int 
pm_process_batch(
		PMEM_WRAPPER* pmw,
		PMEM_BUF_BLOCK_LIST* plist
		);

static void* pm_aio_worker (void* arg);

void handle_AIO_seg_complete(
		PMEM_WRAPPER *pmw, 
				AIO* aio,
				ulint seg_idx,
				PMEM_BUF_BLOCK_LIST* plist_in
				);

PMEM_BUF_BLOCK*
 poll(PMEM_WRAPPER *pmw, 
				AIO* aio,
			   	ulint seg_idx
				);

Slot* find_completed_slot(WT_SESSION_IMPL *session, 
				AIO* aio,
			   	ulint seg_idx,
			   	ulint* n_pending);

PMEM_BUF_BLOCK_LIST*
 collect_list(PMEM_WRAPPER *pmw, 
				AIO* aio,
			   	ulint seg_idx
			   	);
int
 collect_slot(PMEM_WRAPPER *pmw, 
				AIO* aio,
			   	ulint seg_idx
			   	);
#endif /* os0file_h */

#endif
