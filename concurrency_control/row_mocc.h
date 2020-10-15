#pragma once 

class table_t;
class Catalog;
class txn_man;
struct TsReqEntry;

#if CC_ALG==MOCC
#define LOCK_BIT (1UL << 63)

#define TEMP_THRESHOLD 100

class Row_mocc {
public:
	void 				init(row_t * row);
	RC 					access(txn_man * txn, TsType type, row_t * local_row);

	bool				validate(ts_t tid, bool in_write_set);
	void				write(row_t * data, uint64_t tid);

	void				set_tid(uint64_t tid);

    bool lock(txn_man *txn, int lt, bool enable_hotness);
    bool lock_insert(txn_man *txn, int lt, bool enable_hotness);
    bool try_lock(txn_man *txn, int lt, bool enable_hotness);
    void unlock(txn_man *txn, int lt);

	uint64_t 			get_tid();
	void 				assert_lock() {/*assert(_tid_word & LOCK_BIT); */}
private:
#if ATOMIC_WORD
	volatile uint64_t	_tid_word;  // for version checking.

    volatile uint64_t   _lock_bits;
    /*
    *   [0   1   2   3   4   ...   63]
    *   L                            H
    *   R1   R2  R3  R4  R5  ...   W1
    */
#else
	pthread_mutex_t * 	_latch;
	ts_t 				_tid;
#endif
   	bool 				lock_wr(txn_man *txn);
    bool				try_lock_wr(txn_man *txn);
	void 				unlock_wr(txn_man *txn);

	bool				lock_rd(txn_man *txn);
    bool				try_lock_rd(txn_man *txn);
	void				unlock_rd(txn_man *txn);
	row_t * 			_row;
};

#endif
