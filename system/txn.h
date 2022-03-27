#pragma once 

#include "global.h"
#include "helper.h"
#include "coro.h"

class workload;
class thread_t;
class row_t;
class table_t;
class base_query;
class INDEX;
class index_base;
class persistent_log;

// each thread has a txn_man. 
// a txn_man corresponds to a single transaction.

//For VLL
enum TxnType {VLL_Blocked, VLL_Free};

class Access {
public:
	access_t 	type;
	row_t * 	orig_row;
	row_t * 	data;
	row_t * 	orig_data;
	void cleanup();
#if CC_ALG == TICTOC
	ts_t 		wts;
	ts_t 		rts;
#elif (CC_ALG == SILO || CC_ALG == HLOCK || CC_ALG == PLOR || CC_ALG == MOCC)
	ts_t 		tid;
	ts_t 		epoch;
#elif CC_ALG == HEKATON
	void * 		history_entry;	
#endif

};

#if CC_ALG == MOCC
struct RLL {
	int lt;
	row_t *row;
	bool state;
};
#endif

class txn_man
{
public:
	virtual void init(thread_t * h_thd, workload * h_wl, uint64_t part_id);
	void release();
	thread_t * h_thd;
	workload * h_wl;
	myrand * mrand;
	uint64_t abort_cnt;
	persistent_log *log;

	virtual RC 		run_txn(base_query * m_query) = 0;
	uint64_t 		get_thd_id();
	workload * 		get_wl();
	void 			set_txn_id(txnid_t txn_id);
	txnid_t 		get_txn_id();

	void 			set_ts(ts_t timestamp);
	ts_t 			get_ts();

	pthread_mutex_t txn_lock;
	row_t * volatile cur_row;
#if CC_ALG == HEKATON
	void * volatile history_entry;
#endif
	// [DL_DETECT, NO_WAIT, WAIT_DIE]
	bool volatile 	lock_ready;
	bool volatile 	lock_abort; // forces another waiting txn to abort.
	// [TIMESTAMP, MVCC]
	bool volatile 	ts_ready; 
	// [HSTORE]
	int volatile 	ready_part;
	RC 				finish(RC rc);
	void 			cleanup(RC rc);
#if CC_ALG == TICTOC
	ts_t 			get_max_wts() 	{ return _max_wts; }
	void 			update_max_wts(ts_t max_wts);
	ts_t 			last_wts;
	ts_t 			last_rts;
#elif (CC_ALG == SILO || CC_ALG == HLOCK || CC_ALG == MOCC)
	ts_t 			last_tid;
	// uint64_t lock_holding;
	// uint64_t lock_releasing;
#endif
	
	// For WOUND_WAIT and OLOCK
	bool wound = false;
	bool waiting = false;
	bool ex_mode = false;
	bool readonly = false;
	bool read_committed = false;

	int conflict_type = 0; // rw:1; ww:2;
#ifdef DEBUG_WOUND
	int wound_cnt = 1;
	int wound_cnt_discovered = 0;
	int wound_cnt_discovered1 = 0;
	int last_wound;
	int cur_owner_id = 0;
	int lock_cnt = 0;
#endif

	int row_num_last_tx = 0;
	
	// For OCC
	uint64_t 		start_ts;
	uint64_t 		end_ts;
	// following are public for OCC
	int 			row_cnt;
	int	 			wr_cnt;
	int				rdwr_cnt;
	Access **		accesses;
	int 			num_accesses_alloc;

	// uint64_t w_cnt = 0;

	// For VLL
	TxnType 		vll_txn_type;
	itemid_t *		index_read(index_base * index, idx_key_t key, int part_id);
	void 			index_read(index_base * index, idx_key_t key, int part_id, itemid_t *& item);

	RC				index_read_multiple(index_base* index, idx_key_t key, itemid_t** items, size_t& count, int part_id);
	RC				index_read_range(index_base* index, idx_key_t min_key, idx_key_t max_key, itemid_t** items, size_t& count, int part_id);
	RC				index_read_range_rev(index_base* index, idx_key_t min_key, idx_key_t max_key, itemid_t** items, size_t& count, int part_id);

	row_t * 		get_row(row_t * row, access_t type);
	void *			reserve();
	RC apply_index_changes(RC rc);

	bool insert_idx(index_base* index, uint64_t key, row_t* row, int part_id);
	bool remove_idx(index_base* index, uint64_t key, row_t* row, int part_id);

	row_t* search(index_base* index, size_t key, int part_id, access_t type);

	// idx_key_t cur_key;
	// bool 			do_print = false;
	// bool			lock_print = false;
	// int				owner_before;
	// int				owner_cur;
	// void 			*lock_addr;
	// int 		locked;
	// int 		locked_total;
	// uint64_t 	insert_fail = 0;
	// bool 		do_print = false;
	// bool 		payment_print = false;
#if CC_ALG == MOCC
	bool is_locked(uint64_t key);
	void remove_non_cononical_lock(uint64_t key);
	void insert_cononical_lock(int lt, row_t *row);
	void remove_cononical_lock(uint64_t key);
	void unlock_read_locks_all();
	void clear_lock_state(RC rc);
	bool track_perf_sig = false;
	int	lock_rd_cnt = 0;
#endif
	
protected:	
	bool 			insert_row(row_t * &row, table_t * table, int part_id, uint64_t& out_row_id);
	bool 			remove_row(row_t* row);
private:
	// insert rows
	uint64_t 		insert_cnt;
	// uint64_t		inserted;
	// uint64_t        inserted_total;
	row_t * 		insert_rows[MAX_ROW_PER_TXN];
	uint64_t 		remove_cnt;
	row_t * 		remove_rows[MAX_ROW_PER_TXN];

	// insert/remove indexes
	uint64_t 		   insert_idx_cnt;
	index_base*   insert_idx_idx[MAX_ROW_PER_TXN];
	idx_key_t	     insert_idx_key[MAX_ROW_PER_TXN];
	row_t* 		     insert_idx_row[MAX_ROW_PER_TXN];
	int	       	   insert_idx_part_id[MAX_ROW_PER_TXN];

	uint64_t 		   remove_idx_cnt;
	index_base*   remove_idx_idx[MAX_ROW_PER_TXN];
	idx_key_t	     remove_idx_key[MAX_ROW_PER_TXN];
	int	      	   remove_idx_part_id[MAX_ROW_PER_TXN];

#if CC_ALG == MOCC
	RLL cur_lock_list[MAX_ROW_PER_TXN];
	int cur_lock_list_head = 0;
#endif


	txnid_t 		txn_id;
	ts_t 			timestamp;

	bool _write_copy_ptr;
#if (CC_ALG == TICTOC || CC_ALG == SILO || CC_ALG == HLOCK || CC_ALG == MOCC)
	bool 			_pre_abort;
	bool 			_validation_no_wait;
#endif
#if CC_ALG == TICTOC
	bool			_atomic_timestamp;
	ts_t 			_max_wts;
	// the following methods are defined in concurrency_control/tictoc.cpp
	RC				validate_tictoc();
#elif (CC_ALG == SILO)
	ts_t 			_cur_tid;
	RC				validate_silo();
#elif (CC_ALG == MOCC)
	ts_t			_cur_tid;
	int 			step = 0;
	RC 				validate_mocc();
#elif CC_ALG == HLOCK
	ts_t 			_cur_tid;
	RC				validate_hlock();
	
#elif CC_ALG == HEKATON
	RC 				validate_hekaton(RC rc);
#endif
#if CC_ALG == OLOCK
	void* entries;
	int n_entry;
#endif
	
	RC validate();
};
