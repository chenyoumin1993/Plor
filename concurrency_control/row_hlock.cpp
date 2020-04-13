#include "txn.h"
#include "row.h"
#include "row_hlock.h"
#include "mem_alloc.h"

#if CC_ALG==HLOCK

extern txn_man *txn_tb[];

void 
Row_hlock::init(row_t * row) 
{
	_row = row;
	// _tid_word = 0;
	bmpRd = new BitMap(THREAD_CNT);
	lockWr = new WrLockItem();
}

RC
Row_hlock::access(txn_man * txn, TsType type, row_t * local_row) {
	if (txn->wound) {
		// INC_STATS(txn->get_thd_id(), wound1, 1);
		return Abort;
	}
	if (type == R_REQ || type == RW_REQ) {
		bool ret = lock_rd(txn);
		if (!ret) {
			assert(!txn->read_committed);
			return Abort;
		}
	}
	COMPILER_BARRIER
	// uint64_t v = 0;
	// uint64_t v2 = 1;
	// while (v2 != v && !txn->wound) {
	// 	v = _tid_word;
	// 	while (v & LOCK_BIT && !txn->wound) {
	// 		PAUSE
	// 		v = _tid_word;
	// 	}
	// 	if (txn->wound)
	// 		return Abort;
	// 	local_row->copy(_row);
	// 	COMPILER_BARRIER
	// 	v2 = _tid_word;
	// } 
	// if (txn->wound)
	// 	return Abort;
#if INTERACTIVE_MODE == 0
	if (type != R_REQ) {
		local_row->copy(_row);
	} else { // Read, only need a reference to *data.
		local_row->ref(_row);
	}
#else
	local_row->copy(_row);
#endif
	txn->last_tid = lockWr->_ts;
	if (txn->read_committed) {
		unlock_rd(txn);
	}
	return RCOK;
}

void
Row_hlock::write(row_t * data, uint64_t tid) {
#if INTERACTIVE_MODE == 1
	_row->remote_write(data);
#else
	_row->copy(data);
#endif
	WrLockItem l_old, l_new;

_start:
	l_old.l_wr = lockWr->l_wr;
	l_new.l_wr = l_old.l_wr;
	l_new._ts = tid;
	if (!__sync_bool_compare_and_swap(&lockWr->l_wr, l_old.l_wr, l_new.l_wr)) {
		asm volatile ("lfence" ::: "memory");
		goto _start;
	}
	// uint64_t v = _tid_word;
	// M_ASSERT(tid > (v & (~LOCK_BIT)) && (v & LOCK_BIT), "tid=%ld, v & LOCK_BIT=%uld, v & (~LOCK_BIT)=%ld\n", tid, (v & LOCK_BIT), (v & (~LOCK_BIT)));
	// _tid_word = (tid | LOCK_BIT);
}

int
Row_hlock::lock_wr(txn_man *txn) {
	WrLockItem l_old, l_new;
	// who_am_i = txn->get_thd_id();
	// my_ts = txn->get_ts();
	// if (lockWr->_tid == (txn->get_thd_id() + 1)) {
		// printf("tid = %d, owner_ts = %d, cur_ts = %d\n", txn->get_thd_id(), owner_ts, txn->get_ts());
	// }
	// assert(lockWr->_tid != (txn->get_thd_id() + 1) && lockWr->_tid <= THREAD_CNT);
	if (lockWr->_tid == (txn->get_thd_id() + 1)) {
		assert(false);
	}

	ts_t wait_start = get_sys_clock();
	ts_t wait_end;
	bool tag = false;

	// ts_t wound_start = 0;
	while (!txn->wound) {
		// Read a snapshot.
		if (_row->is_deleted)
			return 2;
		asm volatile ("lfence" ::: "memory");
		l_old.l_wr = lockWr->l_wr;
		l_new.l_wr = l_old.l_wr;
		if (l_old._tid == 0) {
			// Become the owner.
			l_new._tid = txn->get_thd_id() + 1; // tid can be zero, be careful.
			l_new.wound = 0;
			if (__sync_bool_compare_and_swap(&lockWr->l_wr, l_old.l_wr, l_new.l_wr)) {
				// become the new owner.
				tag = true;
				goto _success;
			}
		} else {
			// waiting_for = l_old._tid - 1;
			while (txn_tb[l_old._tid - 1] == NULL) ;
			// waiting_ts = txn_tb[l_old._tid - 1]->get_ts();
			if (txn_tb[l_old._tid - 1]->get_ts() > txn->get_ts()) {
				// I have higher priority, kill it.
				// Note: don't grap the lock now. wait for the current owner to release the lock.
				if(!l_old.wound) {
					l_new.wound = 1;
					if (__sync_bool_compare_and_swap(&lockWr->l_wr, l_old.l_wr, l_new.l_wr)) {
						txn_tb[l_old._tid - 1]->wound = true;
						asm volatile ("mfence" ::: "memory");
						// wound_start = get_sys_clock();
					}
				} else {
					// already wound it.
					// if (get_sys_clock() - wound_start > 1000) {
					// 	txn_tb[l_old._tid - 1]->wound = true;
					// }
				}
			}
		}
		asm volatile ("lfence" ::: "memory");
	}
_success:

	wait_end = get_sys_clock();
	if (PRINT_LAT_DEBUG && txn->get_thd_id() == 0) {
		last_waiting_time += wait_end - wait_start; // ns
	}

	if (_row->is_deleted)
		return 2;
	
	// txn->waiting = false;
	if (txn->wound && !tag) {
		// Lock is not acquired.
		// INC_STATS(txn->get_thd_id(), wound2, 1);
		return 2;
	}

	// txn->lock_holding += 1;
	owner_ts = txn->get_ts();

	// INC_STATS(txn->get_thd_id(), debug5, (who_am_i + my_ts + waiting_for + waiting_ts + waiting_reader + waiting_reader_ts));
	// Scan the readers, wait for older ones and wound newer ones.
	bmpRd->Set(63);
	if (bmpRd->isEmpty())
		return 0;
	for (uint i = 0; i < THREAD_CNT; ++i) {
		txn->waiting = false;
        if (i != txn->get_thd_id() && i != 63 && bmpRd->isSet(i)) {
	    	while (txn_tb[i] == NULL) ;
            if (txn_tb[i]->get_ts() > txn->get_ts()) {
                // Kill him.
                txn_tb[i]->wound = true;
				// INC_STATS(txn->get_thd_id(), debug5, 1);
            } else {
                // Wait it.
				// txn->waiting = true;
				// asm volatile ("sfence" ::: "memory");
                while (bmpRd->isSet(i) && !txn->wound && (txn_tb[i]->get_ts() < txn->get_ts())) {
					// waiting_reader = i;
					// waiting_reader_ts = txn_tb[i]->get_ts();
					asm volatile ("lfence" ::: "memory");
					PAUSE
                }
				// txn->waiting = false;
                if (txn->wound) {
					// Lock is acquired.
					// INC_STATS(txn->get_thd_id(), wound3, 1);
                    return 1;
                }
            }
        }
		asm volatile ("lfence" ::: "memory");
    }
	return 0;
}

void
Row_hlock::unlock_wr(txn_man *txn) {
	unlock_wr_(txn);
}

void
Row_hlock::unlock_wr_(txn_man *txn) {
	WrLockItem l_new;
	WrLockItem l_old;

	// The current owner must be me.
	// if ((uint8_t)(lockWr._tid - 1) != (uint8_t)txn->get_thd_id()) printf("%d\n", lockWr._tid);
_start:
	l_new.l_wr = lockWr->l_wr;
	l_old.l_wr = l_new.l_wr;

    if (l_old._tid == 0) {
		// not locked.
		return;
	}
    if ((uint8_t)(l_old._tid - 1) != (uint8_t)txn->get_thd_id()) {
		printf("inconsistent: holder=%d, me=%d\n", (uint8_t)(lockWr->_tid - 1), (uint8_t)txn->get_thd_id());
	}
	assert((uint8_t)(l_old._tid - 1) == (uint8_t)txn->get_thd_id());
	// if (lockWr->wound) {
	// 	while (!txn->wound) ;
	// }
	if (bmpRd->isSet(63)) 
		bmpRd->Unset(63);
	l_new.wound = 0;
	l_new._tid = 0;
	if (!__sync_bool_compare_and_swap(&(lockWr->l_wr), l_old.l_wr, l_new.l_wr)) {
		asm volatile ("lfence" ::: "memory");
		goto _start;
	}
}

// Unused.
bool
Row_hlock::try_lock_wr()
{	
	return false;
	// uint64_t v = _tid_word;
	// if (v & LOCK_BIT) // already locked
	// 	return false;
	// return __sync_bool_compare_and_swap(&_tid_word, v, (v | LOCK_BIT));
}

bool
Row_hlock::lock_rd(txn_man *txn) {
	if (txn->wound) {
		// INC_STATS(txn->get_thd_id(), wound4, 1);
		return false;
	}
	WrLockItem l;
_start:
	l.l_wr = lockWr->l_wr;
	if (!txn->readonly)
		bmpRd->Set(txn->get_thd_id());
	if (bmpRd->isSet(63) && l._tid != 0) {
		while (txn_tb[l._tid - 1] == NULL) ;
		// bmpRd->Unset(txn->get_thd_id());
		// return false;
		if (txn_tb[l._tid - 1]->get_ts() < txn->get_ts()) {
			if (!txn->readonly)
				bmpRd->Unset(txn->get_thd_id());
			// wait for unset.
			while (bmpRd->isSet(63) && !txn->wound && txn_tb[l._tid - 1]->get_ts() < txn->get_ts()) {
				l.l_wr = lockWr->l_wr;
				if (l._tid == 0) {
					break;
				}
			}

			if (txn->wound && !txn->read_committed) {
				return false;
			} else {
				if (!txn->readonly)
					bmpRd->Set(txn->get_thd_id());
				return true;
			}
		}
	}
	return true;
}

bool
Row_hlock::is_locked_rd(txn_man *txn) {
	return bmpRd->isSet(txn->get_thd_id());
}

void
Row_hlock::unlock_rd(txn_man *txn) {
	if (bmpRd->isSet(txn->get_thd_id()))
		bmpRd->Unset(txn->get_thd_id());
}

uint64_t 
Row_hlock::get_tid()
{
	return lockWr->_ts;
}

#endif
