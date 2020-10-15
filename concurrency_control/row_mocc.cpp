#include "txn.h"
#include "row.h"
#include "row_mocc.h"
#include "mem_alloc.h"

#if CC_ALG==MOCC

void 
Row_mocc::init(row_t * row) 
{
	_row = row;
#if ATOMIC_WORD
	_tid_word = 0;
    _lock_bits = 0;
#else 
	_latch = (pthread_mutex_t *) _mm_malloc(sizeof(pthread_mutex_t), 64);
	pthread_mutex_init( _latch, NULL );
	_tid = 0;
#endif
}

void
Row_mocc::set_tid(uint64_t tid) {
    if (((_tid_word & LOCK_BIT) || (_lock_bits & LOCK_BIT)) == false) {
        printf("set tid fail.\n");
        while (1) ;
    }
  assert((_tid_word & LOCK_BIT) || (_lock_bits & LOCK_BIT));
#if ATOMIC_WORD
	_tid_word = tid;
#else
	_tid = tid;
#endif
}

RC
Row_mocc::access(txn_man * txn, TsType type, row_t * local_row) {
#if ATOMIC_WORD
    int lt = (type == R_REQ) ? LOCK_SH : LOCK_EX;
    lock(txn, lt, true);

	uint64_t v = 0;
	uint64_t v2 = 1;
	while (v2 != v) {
		v = _tid_word;
		while (v & LOCK_BIT) {
			PAUSE
			v = _tid_word;
		}
		local_row->copy(_row);
		COMPILER_BARRIER
		v2 = _tid_word;
	} 
	txn->last_tid = v & (~LOCK_BIT);
#else 
	lock();
	local_row->copy(_row);
	txn->last_tid = _tid;
	release();
#endif
	return RCOK;
}

bool
Row_mocc::validate(ts_t tid, bool in_write_set) {
#if ATOMIC_WORD
	uint64_t v = _tid_word;
	if (in_write_set)
		return tid == (v & (~LOCK_BIT));

	if (v & LOCK_BIT) // Locked.
		return false;
	else if (tid != (v & (~LOCK_BIT)))  // Version changed.
		return false;
	else 
		return true;
#else
	if (in_write_set)	
		return tid == _tid;
	if (!try_lock())
		return false;
	bool valid = (tid == _tid);
	release();
	return valid;
#endif
}

void
Row_mocc::write(row_t * data, uint64_t tid) {
#if ATOMIC_WORD
    // already hold the lock.
    uint64_t v = _tid_word;
    assert((v & (~LOCK_BIT)) == v);
    assert(__sync_bool_compare_and_swap(&_tid_word, v, (v | LOCK_BIT)) == true);  // set the lock bit in _tid_word.
#endif

#if INTERACTIVE_MODE == 1
	_row->remote_write(data);
#else
	wait_cycles(WAIT_CYCLE);
	_row->copy(data);
#endif
#if ATOMIC_WORD
    PAUSE
	v = _tid_word;
	M_ASSERT(tid > (v & (~LOCK_BIT)) && (v & LOCK_BIT), "tid=%ld, v & LOCK_BIT=%ld, v & (~LOCK_BIT)=%ld\n", tid, (v & LOCK_BIT), (v & (~LOCK_BIT)));
	_tid_word = (tid & (~LOCK_BIT));
    COMPILER_BARRIER
#else
	_tid = tid;
#endif
}

bool
Row_mocc::lock_wr(txn_man *txn) {
	while (1) {
        if (__sync_bool_compare_and_swap(&_lock_bits, 0, LOCK_BIT))
            break;
		asm volatile ("lfence" ::: "memory");
	}
    return true;
}

bool
Row_mocc::try_lock_wr(txn_man *txn) {
    return __sync_bool_compare_and_swap(&_lock_bits, 0, LOCK_BIT);
}

void
Row_mocc::unlock_wr(txn_man *txn) {
    bool ret = __sync_bool_compare_and_swap(&_lock_bits, LOCK_BIT, 0);
    
    // if (ret == false) {
    //     printf("%llx - %d\n", _lock_bits, txn->get_thd_id());
    //     while (1) ;
    //     assert(false);
    // }
}

bool
Row_mocc::lock_rd(txn_man *txn) {
    uint64_t l, l_old, l_new;

	while (1) {
        l = _lock_bits;
        l_old = l & (~LOCK_BIT); // remove the write lock bit if any.
        l_new = l_old | (1ULL << txn->get_thd_id()); // add read lock bit.
        if (__sync_bool_compare_and_swap(&_lock_bits, l_old, l_new))
            break;
		asm volatile ("lfence" ::: "memory");
	}
    txn->lock_rd_cnt += 1;
    return true;
}

bool
Row_mocc::try_lock_rd(txn_man *txn) {
    uint64_t l = _lock_bits;
    uint64_t l_old = l & (~LOCK_BIT); // remove the write lock bit if any.
    uint64_t l_new = l_old | (1ULL << txn->get_thd_id()); // add read lock bit.
    return __sync_bool_compare_and_swap(&_lock_bits, l_old, l_new);
}

void
Row_mocc::unlock_rd(txn_man *txn) {
    // for sure the read bit has been set.
    assert((_lock_bits & (1ULL << txn->get_thd_id())) !=  0);
    __sync_fetch_and_add(&_lock_bits, -(1ULL << txn->get_thd_id()));
    assert((_lock_bits & (1ULL << txn->get_thd_id())) ==  0);
    txn->lock_rd_cnt -= 1;
}

bool
Row_mocc::lock(txn_man *txn, int lt, bool enable_hotness) {
    // Check if already locked.
    if (txn->is_locked(_row->get_primary_key()))
        return true;
    
    if (enable_hotness) {
        // check hotness of this record, lock if necessary.
        // return true;
        if (_row->get_temperature() < TEMP_THRESHOLD)
            return true;
        if (!txn->track_perf_sig)
            txn->track_perf_sig = true;
    }

    // enforce canonical mode before locking.
    txn->remove_non_cononical_lock(_row->get_primary_key());

    if (lt == LOCK_EX) {
        lock_wr(txn);
    } else {
        lock_rd(txn);
    }

    txn->insert_cononical_lock(lt, _row);

    return true;
}

bool
Row_mocc::lock_insert(txn_man *txn, int lt, bool enable_hotness) {
    if (lt == LOCK_EX) {
        lock_wr(txn);
    } else {
        lock_rd(txn);
    }

    return true;
}

bool
Row_mocc::try_lock(txn_man *txn, int lt, bool enable_hotness) {
    // Check if already locked.
    if (txn->is_locked(_row->get_primary_key()))
        return true;

    if (enable_hotness) {
        // check hotness of this record, lock if necessary.
        // return true;
        if (_row->get_temperature() < TEMP_THRESHOLD)
            return true;
        
        if (!txn->track_perf_sig)
            txn->track_perf_sig = true;
    }

    // enforce canonical mode before locking.
    txn->remove_non_cononical_lock(_row->get_primary_key());

    bool ret = false;

    if (lt == LOCK_EX) {
        ret = try_lock_wr(txn);
    } else {
        ret = try_lock_rd(txn);
    }

    if (ret == true) {
        txn->insert_cononical_lock(lt, _row);
    }
    
    return ret;
}

void
Row_mocc::unlock(txn_man *txn, int lt) {
    if (lt == LOCK_EX) {
        unlock_wr(txn);
    } else {
        unlock_rd(txn);
    }
    txn->remove_cononical_lock(_row->get_primary_key());
}

uint64_t 
Row_mocc::get_tid()
{
#if ATOMIC_WORD
	ASSERT(ATOMIC_WORD);
	return _tid_word & (~LOCK_BIT);
#else
	return _tid;
#endif
}

#endif
