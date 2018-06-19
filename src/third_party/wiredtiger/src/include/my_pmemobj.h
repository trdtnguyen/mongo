/* 
 * Author; Trong-Dat Nguyen
 * MongoDB REDO log with NVDIMM
 * Using libpmemobj
 * Copyright (c) 2018 VLDB Lab - Sungkyunkwan University
 * */


#ifndef __PMEMOBJ_H__
#define __PMEMOBJ_H__

//Common includes
#include <stdio.h> //for FILE
//#include <stdlib.h>
//#include <sys/types.h>                                                                      
//#include <sys/time.h> //for struct timeval, gettimeofday()
//#include <string.h>
//#include <stdint.h> //for uint64_t
//#include <math.h> //for log()
//#include <assert.h>
//#include <wchar.h>
#include <unistd.h> //for access() to check file exists

//includes from DBMS
//#include "univ.i"
//#include "ut0byte.h"
//#include "ut0rbt.h"
//#include "buf0buf.h" //for page_id_t
//#include "page0types.h"
//#include "ut0dbg.h"
//#include "ut0new.h"

//incldue from libpmem
//#include "wt_internal.h"
#include <libpmemobj.h>
//#include "my_pmem_common.h"
//cc -std=gnu99 ... -lpmemobj -lpmem

//Mapping types between InnoDB and MongoDB
typedef unsigned char byte;
typedef uint64_t ulint;
typedef void* os_thread_ret_t;
typedef uint64_t os_offset_t;

/*Common Defines, Consts*/
#define PMEM_MAX_FILES 1000
#define PMEM_MAX_FILE_NAME_LENGTH 10000
#define PMEM_HASH_MASK 1653893711

#define PMEM_ID_NONE -1 //ID not defined

//random number for checking AIO
#define PMEM_AIO_CHECK 7988

//wait for a aio write, call when there is no free block
#define PMEM_WAIT_FOR_WRITE 200
#define PMEM_WAIT_FOR_FREE_LIST 10000

//error handler
#define PMEM_SUCCESS 0
#define PMEM_ERROR -1

#define TOID_ARRAY(x) TOID(x)

#define PMEM_MB 1048576 //1024 * 1024
#define PMEMOBJ_FILE_NAME "pmemobjfile"
//OS_FILE_LOG_BLOCK_SIZE =512 is defined in os0file.h
//static const size_t PMEM_MB = 1024 * 1024;
static const size_t PMEM_MAX_LOG_BUF_SIZE = 1 * 1024 * PMEM_MB;
static const size_t PMEM_PAGE_SIZE = 16*1024; //16KB
static const size_t PMEM_MAX_DBW_PAGES= 128; // 2 * extent_size


//static uint64_t PMEM_N_BUCKETS=64;
//static uint64_t PMEM_BUCKET_SIZE=128;
//static double PMEM_BUF_FLUSH_PCT=1;
//
//static uint64_t PMEM_N_FLUSH_THREADS=32;
//set this to large number to eliminate 
//static uint64_t PMEM_PAGE_PER_BUCKET_BITS=32;

// 1 < this_value < flusher->size (32)
//static uint64_t PMEM_FLUSHER_WAKE_THRESHOLD=5;
//static uint64_t PMEM_FLUSHER_WAKE_THRESHOLD=30;
//
//static FILE* debug_file = fopen("part_debug.txt","a");

#define PMEM_MAX_LISTS_PER_BUCKET 2


enum {
	PMEM_READ = 1,
	PMEM_WRITE = 2
};
//enum PMEM_OBJ_TYPES {
//	UNKNOWN_TYPE,
//	LOG_BUF_TYPE,
//	DBW_TYPE,
//	BUF_TYPE,
//	META_DATA_TYPE
//};
typedef enum {
	UNKNOWN_TYPE,
	LOG_BUF_TYPE,
	DBW_TYPE,
	BUF_TYPE,
	META_DATA_TYPE
} PMEM_OBJ_TYPES;

typedef enum {
    PMEM_FREE_BLOCK = 1,
    PMEM_IN_USED_BLOCK = 2,
    PMEM_IN_FLUSH_BLOCK=3
} PMEM_BLOCK_STATE ;
enum pm_list_cleaner_state {
	/** Not requested any yet.
	Moved from FINISHED by the coordinator. */
	LIST_CLEANER_STATE_NONE = 0,
	/** Requested but not started flushing.
	Moved from NONE by the coordinator. */
	LIST_CLEANER_STATE_REQUESTED,
	/** Flushing is on going.
	Moved from REQUESTED by the worker. */
	LIST_CLEANER_STATE_FLUSHING,
	/** Flushing was finished.
	Moved from FLUSHING by the worker. */
	LIST_CLEANER_STATE_FINISHED
};

static inline int file_exists(char const *file);

/*
 *  * file_exists -- checks if file exists
 *   */
static inline int file_exists(char const *file)
{
	    return access(file, F_OK);
}

/*Convenient typedefs*/
struct __pmem_wrapper;
typedef struct __pmem_wrapper PMEM_WRAPPER;

struct __pmem_dbw;
typedef struct __pmem_dbw PMEM_DBW;

struct __pmem_log_buf;
typedef struct __pmem_log_buf PMEM_LOG_BUF;

#if defined (UNIV_PMEMOBJ_BUF)
struct __pmem_buf_block_t;
typedef struct __pmem_buf_block_t PMEM_BUF_BLOCK;

struct __pmem_buf_block_list_t;
typedef struct __pmem_buf_block_list_t PMEM_BUF_BLOCK_LIST;

struct __pmem_buf_free_pool;
typedef struct __pmem_buf_free_pool PMEM_BUF_FREE_POOL;


struct __pmem_buf;
typedef struct __pmem_buf PMEM_BUF;


struct __pmem_list_cleaner_slot;
typedef struct __pmem_list_cleaner_slot PMEM_LIST_CLEANER_SLOT;

struct __pmem_list_cleaner;
typedef struct __pmem_list_cleaner PMEM_LIST_CLEANER;

struct __pmem_flusher;
typedef struct __pmem_flusher PMEM_FLUSHER;

struct __pmem_buf_bucket_stat;
typedef struct __pmem_buf_bucket_stat PMEM_BUCKET_STAT;

struct __pmem_file_map_item;
typedef struct __pmem_file_map_item PMEM_FILE_MAP_ITEM;

struct __pmem_file_map;
typedef struct __pmem_file_map PMEM_FILE_MAP;

struct __pmem_sort_obj;
typedef struct __pmem_sort_obj PMEM_SORT_OBJ;

//Do not use this struct until the research finish
struct __pmem_aio_param;
typedef struct __pmem_aio_param PMEM_AIO_PARAM;
struct __pmem_aio_param_arr;
typedef struct __pmem_aio_param_arr PMEM_AIO_PARAM_ARRAY;
//End Do not use this struct until the research finish

#endif //UNIV_PMEMOBJ_BUF

POBJ_LAYOUT_BEGIN(my_pmemobj);
POBJ_LAYOUT_TOID(my_pmemobj, char);
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_LOG_BUF);
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_DBW);
#if defined(UNIV_PMEMOBJ_BUF)
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_BUF);
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_BUF_FREE_POOL);
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_BUF_BLOCK_LIST);
POBJ_LAYOUT_TOID(my_pmemobj, TOID(PMEM_BUF_BLOCK_LIST));
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_BUF_BLOCK);
POBJ_LAYOUT_TOID(my_pmemobj, TOID(PMEM_BUF_BLOCK));
#endif //UNIV_PMEMOBJ_BUF
POBJ_LAYOUT_END(my_pmemobj);


////GLOBAL variables
//PMEM_WRAPPER* gb_pmw = NULL;

////////////////////////// THE WRAPPER ////////////////////////
/*The global wrapper*/
struct __pmem_wrapper {

	char name[PMEM_MAX_FILE_NAME_LENGTH];
	PMEMobjpool* pop;
	PMEM_LOG_BUF* plogbuf;
	PMEM_DBW* pdbw;
#if defined (UNIV_PMEMOBJ_BUF)
	PMEM_BUF* pbuf;
#endif
	bool is_new;

	//GLOBAL variables
	uint64_t PMEM_BUF_SIZE;

	uint64_t PMEM_N_BUCKETS;
	uint64_t PMEM_BUCKET_SIZE;
	double PMEM_BUF_FLUSH_PCT;

	uint64_t PMEM_N_FLUSH_THREADS;
	uint64_t PMEM_FLUSHER_WAKE_THRESHOLD;
	//WiredTiger integration
	WT_SESSION_IMPL* session;
};
/* FUNCTIONS*/
PMEM_WRAPPER* 
pm_wrapper_create(
		WT_SESSION_IMPL* session,
		const char*		path,
		const size_t	pool_size);

void
pm_wrapper_free(PMEM_WRAPPER* pmw);


PMEMoid pm_pop_alloc_bytes(PMEMobjpool* pop, size_t size);
void pm_pop_free(PMEMobjpool* pop);

int
pm_wrapper_buf_alloc_or_open(
		PMEM_WRAPPER*		pmw,
		const size_t		buf_size,
		const size_t		page_size);

int pm_wrapper_buf_close(PMEM_WRAPPER* pmw);

int
pm_wrapper_buf_alloc(
		PMEM_WRAPPER*		pmw,
		const size_t		size,
		const size_t		page_size);

////////////////////// LOG BUFFER /////////////////////////////

struct __pmem_log_buf {
	size_t				size;
	PMEM_OBJ_TYPES		type;	
	PMEMoid				data; //log data
    uint64_t			lsn; 	
	uint64_t			buf_free; /* first free offset within the log buffer */
	bool				need_recv; /*need recovery, it is set to false when init and when the server shutdown
					  normally. Whenever a log record is copy to log buffer, this flag is set to true
	*/
	uint64_t			last_tsec_buf_free; /*the buf_free updated in previous t seconds, update this value in srv_sync_log_buffer_in_background() */
};

void* pm_wrapper_logbuf_get_logdata(PMEM_WRAPPER* pmw);
int
pm_wrapper_logbuf_alloc(
		PMEM_WRAPPER*		pmw,
	   	const size_t		size);

int
pm_wrapper_logbuf_realloc(
		PMEM_WRAPPER*		pmw,
	   	const size_t		size);
PMEM_LOG_BUF* pm_pop_get_logbuf(PMEMobjpool* pop);
PMEM_LOG_BUF*
pm_pop_logbuf_alloc(
		PMEMobjpool*		pop,
	   	const size_t		size);
PMEM_LOG_BUF* 
pm_pop_logbuf_realloc(
		PMEMobjpool*		pop,
	   	const size_t		size);
ssize_t  pm_wrapper_logbuf_io(PMEM_WRAPPER* pmw, 
							const int type,
							void* buf, 
							const uint64_t offset,
							unsigned long int n);

///////////// DOUBLE WRITE BUFFER //////////////////////////

struct __pmem_dbw {
	size_t size;
	PMEM_OBJ_TYPES type;	
	PMEMoid  data; //dbw data
	uint64_t s_first_free;
	uint64_t b_first_free;
	bool is_new;
};

void* pm_wrapper_dbw_get_dbwdata(PMEM_WRAPPER* pmw);

int 
pm_wrapper_dbw_alloc(
		PMEM_WRAPPER*		pmw,
	   	const size_t		size);

PMEM_DBW* pm_pop_get_dbw(PMEMobjpool* pop);
PMEM_DBW*
pm_pop_dbw_alloc(
		PMEMobjpool*		pop,
	   	const size_t		size);
ssize_t  pm_wrapper_dbw_io(PMEM_WRAPPER* pmw, 
							const int type,
							void* buf, 
							const uint64_t offset,
							unsigned long int n);

/////// PMEM BUF  //////////////////////
#if defined (UNIV_PMEMOBJ_BUF)
//This struct is used only for POBJ_LIST_INSERT_NEW_HEAD
//modify this struct according to struct __pmem_buf_block_t
struct list_constr_args{
	uint64_t		id;
//	page_id_t					id;
	size_t			size;
//	page_size_t					size;
	int							check;
	//buf_page_t*		bpage;
	PMEM_BLOCK_STATE			state;
	TOID(PMEM_BUF_BLOCK_LIST)	list;
	uint64_t					pmemaddr;
};

/*
 *A unit page in pmem
 It wrap buf_page_t and an address in pmem
 * */
struct __pmem_buf_block_t{
	PMEMrwlock					lock; //this lock protects remain properties

	uint64_t		id;
	size_t			size;
	//page_id_t					id;
	//page_size_t					size;
	//pfs_os_file_t				file_handle;
	char						file_name[256];
	int							check; //PMEM AIO flag used in fil_aio_wait
	bool	sync;
	PMEM_BLOCK_STATE			state;
	TOID(PMEM_BUF_BLOCK_LIST)	list;
	uint64_t		pmemaddr; /*
						  the offset of the page in pmem
						  note that the size of page can be got from page
						*/
};

struct __pmem_buf_block_list_t {
	PMEMrwlock				lock;
	uint64_t				list_id; //id of this list in total PMEM area
	int						hashed_id; //id of this list if it is in a bucket, PMEM_ID_NONE if it is in free list
	TOID_ARRAY(TOID(PMEM_BUF_BLOCK))	arr;
	//POBJ_LIST_HEAD(block_list, PMEM_BUF_BLOCK) head;
	TOID(PMEM_BUF_BLOCK_LIST) next_list;
	TOID(PMEM_BUF_BLOCK_LIST) prev_list;
	TOID(PMEM_BUF_BLOCK) next_free_block;

	POBJ_LIST_ENTRY(PMEM_BUF_BLOCK_LIST) list_entries;

	size_t				max_pages; //max number of pages
	size_t				cur_pages; // current buffered pages
	bool				is_flush;
	size_t				n_aio_pending; //number of pending aio
	size_t				n_sio_pending; //number of pending sync io 
	size_t				n_flush; //number of flush
	int					check;
	uint64_t				last_time;
	
	uint64_t				param_arr_index; //index of the param in the param_arrs used to carry the info of this list
	//int					flush_worker_id;
	//bool				is_worker_handling;
	
};

struct __pmem_buf_free_pool {
	PMEMrwlock			lock;
	POBJ_LIST_HEAD(list_list, PMEM_BUF_BLOCK_LIST) head;
	size_t				cur_lists;
	size_t				max_lists;
};


struct __pmem_buf {
	size_t size;
	size_t page_size;
	PMEM_OBJ_TYPES type;	

	PMEMoid  data; //pmem data
	//char* p_align; //align 
	byte* p_align; //align 

	bool is_new;
	TOID(PMEM_BUF_FREE_POOL) free_pool;
	TOID_ARRAY(TOID(PMEM_BUF_BLOCK_LIST)) buckets;
	TOID(PMEM_BUF_BLOCK_LIST) spec_list; //list of page 0 used in recovery

	FILE* deb_file;
#if defined(UNIV_PMEMOBJ_BUF_STAT)
	PMEM_BUCKET_STAT* bucket_stats; //array of bucket stats
#endif

	bool is_async_only; //true if we only capture non-sync write from buffer pool

	//Those varables are in DRAM
	bool is_recovery;
	//TODO: find the mapping os_event_t in MongoDB
	//os_event_t*  flush_events; //N flush events for N buckets
	//os_event_t free_pool_event; //event for free_pool
	WT_CONDVAR**  flush_conds; //N flush events for N buckets
	WT_CONDVAR* free_pool_cond; //event for free_pool
	

	PMEMrwlock				param_lock;
	//TODO: research AIO in MongoDB
	PMEM_AIO_PARAM_ARRAY* param_arrs;//circular array of pointers
	uint64_t			param_arr_size; //size of the array
	uint64_t			cur_free_param; //circular index, where the next free params is
	
	PMEM_FLUSHER* flusher;	

	PMEM_FILE_MAP* filemap;
};


// PARTITION //////////////
/*Map space id to hashed_id, for partition purpose
 * */
struct __pmem_file_map_item {
	uint32_t		space_id;
	char*			name;

	int*			hashed_ids; //list of hash_id this space appears on 
	uint64_t		count; //number of hashed list this space appears on

	uint64_t*		freqs; //freq[i] is the number of times this space apearts on hashed_ids[i]
};
struct __pmem_file_map {
	PMEMrwlock			lock;

	uint64_t					max_size;
	uint64_t					size;
	PMEM_FILE_MAP_ITEM**		items;
};

//this struct for space_oriented sort
struct __pmem_sort_obj {
	uint32_t			space_no;
	
	uint32_t			n_blocks;
	uint32_t*			block_indexes;
};

void 
pm_filemap_init(
		PMEM_BUF*		buf);
void
pm_filemap_close(PMEM_BUF* buf);

/*Update the page_id in the filemap
 *
 * */
void
pm_filemap_update_items(
		PMEM_BUF*		buf,
	   	//page_id_t		page_id,
	   	uint64_t		page_id,
		int				hashed_id,
		uint64_t		bucket_size); 

void
pm_filemap_print(
		PMEM_BUF*		buf, 
		FILE*			outfile);

#if defined(UNIV_PMEMOBJ_BUF_STAT)
//statistic info about a bucket
//Objects of those struct do not need in PMEM
struct __pmem_buf_bucket_stat {
	PMEMrwlock		lock;

	uint64_t		n_writes;/*number of writes on the bucket*/ 
	uint64_t		n_overwrites;/*number of overwrites on the bucket*/
	uint64_t		n_reads;/*number of reads on the list (both flushing and normal)*/	
	uint64_t		n_reads_hit;/*number of reads successful on PMEM buffer*/	
	uint64_t		n_reads_flushing;/*number of reads on the on-flushing list, n_reads_flushing < n_reads_hit < n_reads*/	
	uint64_t		max_linked_lists;
	uint64_t		n_flushed_lists; /*number of of flushes on the bucket*/
};

#endif

//bool pm_check_io(byte* frame, page_id_t  page_id);
bool pm_check_io(byte* frame, uint64_t  page_id);


PMEM_BUF* pm_pop_get_buf(PMEMobjpool* pop);

PMEM_BUF* 
pm_pop_buf_alloc(
		 PMEM_WRAPPER*		pmw,
		 const size_t		size,
		 const size_t		page_size);

int 
pm_buf_block_init(PMEMobjpool *pop, void *ptr, void *arg);

void 
pm_buf_lists_init(
		PMEM_WRAPPER*	pmw,
		const size_t	total_size,
		const size_t	page_size);

// allocate and init pages in a list
void
pm_buf_single_list_init(
		PMEMobjpool*				pop,
		TOID(PMEM_BUF_BLOCK_LIST)	inlist,
		size_t*						offset,
		struct list_constr_args*	args,
		const size_t				n,
		const size_t				page_size);
int
pm_buf_write(
		   	PMEM_WRAPPER*		pmw,
		   	//page_id_t		page_id,
		   	//page_size_t		size,
		   	uint64_t		page_id,
		   	size_t		size,
		   	byte*			src_data,
		   	bool			sync);

int
pm_buf_write_no_free_pool(
		   	PMEM_WRAPPER*		pmw,
		   	//page_id_t		page_id,
		   	//page_size_t		size,
		   	uint64_t		page_id,
		   	size_t		size,
		   	byte*			src_data, 
			bool			sync);

int
pm_buf_write_with_flusher(
		   	PMEM_WRAPPER*		pmw,
		   	//page_id_t		page_id,
		   	//page_size_t		size,
		   	uint64_t		page_id,
		   	size_t		size,
		   	byte*			src_data,
		   	bool			sync);
int
pm_buf_write_with_flusher_append(
		   	PMEM_WRAPPER*		pmw,
		   	//page_id_t		page_id,
		   	//page_size_t		size,
		   	uint64_t		page_id,
		   	size_t		size,
		   	byte*			src_data,
		   	bool			sync);


const PMEM_BUF_BLOCK*
pm_buf_read(
		   	PMEM_WRAPPER*			pmw,
		   	//const page_id_t		page_id,
		   	//const page_size_t	size,
		   	const uint64_t		page_id,
		   	const size_t	size,
		   	byte*				data,
		   	bool sync);

const PMEM_BUF_BLOCK*
pm_buf_read_lasted(
			PMEM_WRAPPER*		pmw,
		   	//const page_id_t		page_id,
		   	//const page_size_t	size,
		   	const uint64_t		page_id,
		   	const size_t	size,
		   	byte*				data,
		   	bool sync);

const PMEM_BUF_BLOCK*
pm_buf_read_page_zero(
		   	PMEM_WRAPPER*			pmw,
			char*				file_name,
		   	byte*				data);

void
pm_buf_flush_list(
			PMEMobjpool*			pop,
		   	PMEM_BUF*				buf,
		   	PMEM_BUF_BLOCK_LIST*	plist);
void
pm_buf_resume_flushing(
			PMEM_WRAPPER*			pmw);
		   


void
pm_buf_handle_full_hashed_list(
	PMEM_WRAPPER*		pmw,
	uint64_t			hashed);

void
pm_buf_assign_flusher(
	PMEM_WRAPPER*				pmw,
	PMEM_BUF_BLOCK_LIST*	phashlist);

void
pm_buf_write_aio_complete(
			PMEM_WRAPPER*			pmw,
		   	TOID(PMEM_BUF_BLOCK)*	ptoid_block);

PMEM_BUF* pm_pop_get_buf(PMEMobjpool* pop);

//Flusher

/*The flusher thread
 *if is waked by a signal (there is at least a list wait for flush): scan the waiting list and assign a worker to flush that list
 if there is no list wait for flush, sleep and wait for a signal
 * */
struct __pmem_flusher {

	//TODO: find the ib_mutex_t mapping in MongoDB
	//ib_mutex_t			mutex;
	WT_SPINLOCK		flusher_lock;
	//for worker
	//TODO: find the mapping os_event_t in MongoDB
	//os_event_t			is_req_not_empty; //signaled when there is a new flushing list added
	//os_event_t			is_req_full;

	//os_event_t			is_all_finished; //signaled when all workers are finished flushing and no pending request 
	//os_event_t			is_all_closed; //signaled when all workers are closed 
	WT_CONDVAR*			is_req_not_empty_cond; //signaled when there is a new flushing list added
	WT_CONDVAR*			is_req_full_cond;

	WT_CONDVAR*			is_all_finished_cond; //signaled when all workers are finished flushing and no pending request 
	WT_CONDVAR*			is_all_closed_cond; //signaled when all workers are closed 
	volatile uint64_t n_workers;

	//the waiting_list
	uint64_t size;
	uint64_t tail; //always increase, circled counter, mark where the next flush list will be assigned
	uint64_t n_requested;
	//uint64_t n_flushing;

	bool is_running;

	PMEM_BUF_BLOCK_LIST** flush_list_arr;
};
int
pm_flusher_init(
				PMEM_WRAPPER*		pmw, 
				const size_t	size);
int
pm_buf_flusher_close(PMEM_WRAPPER*	pmw);


// AIO 
//TODO: research how MongoDB handle AIO

struct __pmem_aio_param {
	const char*         name;
	int       file;   //fid
	void*               buf; 
	//os_offset_t         offset;
	uint64_t         offset;
	ulint               n;   
//	fil_node_t*         m1;  
	int*				m1;  
	void*               m2;  
};
struct __pmem_aio_param_arr {
	bool    is_free;
	PMEM_AIO_PARAM* params;
};

//dberr_t
//pm_fil_io_batch(
//		const IORequest&	type,
//		void*				pop_in,
//		void*				pmem_buf_in,
//		void*				plist_in);
//
//void
//pm_buf_flush_spaces_in_list(
//		void* pop_in,
//	   	void* buf_in,
//	   	void* flush_list_in);







#if defined(UNIV_PMEMOBJ_BUF_STAT)
void
	pm_buf_bucket_stat_init(PMEM_WRAPPER* pmw);

void
	pm_buf_stat_print_all(PMEM_WRAPPER* pmw);
#endif
//DEBUG functions

void pm_buf_print_lists_info(PMEM_WRAPPER* pmw);


//version 1: implemented in pmem0buf, directly handle without using thread slot
void
pm_handle_finished_block(
	   	PMEM_WRAPPER*			pmw,
	   	PMEM_BUF_BLOCK* pblock);


//Implemented in buf0flu.cc using with pm_buf_write_with_flsuher
void
pm_handle_finished_block_with_flusher(
	   	PMEM_WRAPPER*			pmw,
	   	PMEM_BUF_BLOCK*		pblock);

//version 2 is implemented in buf0flu.cc that handle threads slot
void
pm_handle_finished_block_v2(PMEM_BUF_BLOCK* pblock);

bool
pm_lc_wait_finished(
	uint64_t*	n_flushed_list);

uint64_t
pm_lc_sleep_if_needed(
	uint64_t		next_loop_time,
	int64_t		sig_count);
///////////////////////////////////////////////////////////
// Thread handler funcitons
// TODO: research how MongoDB handle thread 
// ///////////////////////////////////////////////////////////
/*
extern "C"
os_thread_ret_t
DECLARE_THREAD(pm_buf_flush_list_cleaner_coordinator)(
		void* arg);

extern "C"
os_thread_ret_t
DECLARE_THREAD(pm_buf_flush_list_cleaner_worker)(
		void* arg);

extern "C"
os_thread_ret_t
DECLARE_THREAD(pm_flusher_coordinator)(
		void* arg);

extern "C"
os_thread_ret_t
DECLARE_THREAD(pm_flusher_worker)(
		void* arg);
#ifdef UNIV_DEBUG

void
pm_buf_flush_list_cleaner_disabled_loop(void);
#endif
*/

///////////////////////////////////////////////////////////
// HASH funciton
// ////////////////////////////////////////////////////////

uint64_t 
hash_f1(
		uint64_t		hashed,
		uint32_t		space_no,
	   	uint32_t		page_no,
	   	uint64_t		n,
		uint64_t		B,	
		uint64_t		S,
		uint64_t		P);

#define PMEM_BUF_LIST_INSERT(pop, list, entries, type, func, args) do {\
	POBJ_LIST_INSERT_NEW_HEAD(pop, &list.head, entries, sizeof(type), func, &args); \
	list.cur_size++;\
}while (0)

/*Evenly distributed map that one space_id evenly distribute across buckets*/
#define PMEM_HASH_KEY(hashed, key, n) do {\
	hashed = key ^ PMEM_HASH_MASK;\
	hashed = hashed % n;\
}while(0)

#define FOLD(out, a, b) do {\
	out = (a << 20) + a + b;\
}while(0)

#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
/*LESS_BUCKET partition
 *One space is mapped with as less buckets as possible
@hashed		[out]: return hashed value
@space		[in]: space_no
@page		[in]: page_no
@n			[in]: number of buckets 
@B			[in]: number of bits present number of buckets
@S			[in]: number of bits present space_no
@P			[in]: number of bits present max number of pages per space on a bucket, this value is log2(page_per_bucket)
 * */

//Use this macro for production build 
#define PMEM_LESS_BUCKET_HASH_KEY(hashed, space, page)\
   	PARTITION_FUNC1(hashed, space, page,\
		   	PMEM_N_BUCKETS,\
		   	PMEM_N_BUCKET_BITS,\
		   	PMEM_N_SPACE_BITS,\
		   	PMEM_PAGE_PER_BUCKET_BITS) 

#define PARTITION_FUNC1(hashed, space, page, n, B, S, P) do {\
	hashed = (((space & (0xffffffff >> (32 - S))) << (B - S)) + ((page & (0xffffffff >> (32 - P - (B - S)))) >> P)) % n;\
} while(0)
#endif //UNIV_PMEMOBJ_BUF_PARTITION

#endif //UNIV_PMEMOBJ_BUF

#endif /*__PMEMOBJ_H__ */
