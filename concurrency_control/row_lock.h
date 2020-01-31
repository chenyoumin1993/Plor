#ifndef ROW_LOCK_H
#define ROW_LOCK_H

struct LockEntry {
    lock_t type;
    txn_man * txn;
	LockEntry * next;
	LockEntry * prev;
	bool wound;
};

class Row_lock {
public:
	void init(row_t * row);
	// [DL_DETECT] txnids are the txn_ids that current txn is waiting for.
    RC lock_get(lock_t type, txn_man * txn);
    RC lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt);
    RC lock_release(lock_t type, txn_man * txn);
	
// private:
#if USE_SPINLOCK == 1
	pthread_spinlock_t * latch;
#else
    pthread_mutex_t * latch;
#endif
	bool blatch;

	uint64_t counter = 0;
	
	bool 		conflict_lock(lock_t l1, lock_t l2);
	LockEntry * get_entry();
	void 		return_entry(LockEntry * entry);
	row_t * _row;
    lock_t lock_type;
    UInt32 owner_cnt;
    UInt32 waiter_cnt;
	
	// owners is a single linked list
	// waiters is a double linked list 
	// [waiters] head is the oldest txn, tail is the youngest txn. 
	//   So new txns are inserted into the tail.
	LockEntry * owners;	
	LockEntry * waiters_head;
	LockEntry * waiters_tail;

	LockEntry * woundees;
	UInt32 woundee_cnt;


	// bool wounding = false;
	// LockEntry *wounders;
	// lock_t wound_type;
#ifdef DEBUG_WOUND
	int owner_list[10000];
	int owner_ts_list[10000];
	int release_list[10000];
	int oo = 0, rr = 0, tt = 0;
#endif
};

#endif
