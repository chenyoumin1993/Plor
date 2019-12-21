#include "row.h"
#include "txn.h"
#include "row_lock.h"
#include "mem_alloc.h"
#include "manager.h"

void Row_lock::init(row_t * row) {
	_row = row;
	owners = NULL;
	waiters_head = NULL;
	waiters_tail = NULL;
	woundees = NULL;
	owner_cnt = 0;
	waiter_cnt = 0;
	woundee_cnt = 0;
#ifdef USE_SPINLOCK
	latch = new pthread_spinlock_t;
	pthread_spin_init(latch, 0);
#else
	latch = new pthread_mutex_t;
	pthread_mutex_init(latch, NULL);
#endif
	
	lock_type = LOCK_NONE;
	blatch = false;

}

RC Row_lock::lock_get(lock_t type, txn_man * txn) {
	uint64_t *txnids = NULL;
	int txncnt = 0;
	return lock_get(type, txn, txnids, txncnt);
}

RC Row_lock::lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt) {
	assert (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == WOUND_WAIT);
	RC rc;
	int part_id =_row->get_part_id();
	if (g_central_man)
		glob_manager->lock_row(_row);
	else 
#ifdef USE_SPINLOCK
		pthread_spin_lock( latch );
#else
		pthread_mutex_lock( latch );
#endif

	counter += 1;
	assert(owner_cnt <= g_thread_cnt);
	assert(waiter_cnt < g_thread_cnt);
	if (owners != NULL)
		assert(lock_type == owners->type); 
	else 
		assert(lock_type == LOCK_NONE);
#if DEBUG_ASSERT
	LockEntry * en = owners;
	UInt32 cnt = 0;
	while (en) {
		assert(en->txn->get_thd_id() != txn->get_thd_id());
		cnt ++;
		en = en->next;
	}
	assert(cnt == owner_cnt);
	en = waiters_head;
	cnt = 0;
	while (en) {
		cnt ++;
		en = en->next;
	}
	assert(cnt == waiter_cnt);
#endif

	bool conflict = conflict_lock(lock_type, type);

	if (CC_ALG == WOUND_WAIT && !conflict && waiters_head && txn->get_ts() > waiters_tail->txn->get_ts()) {
		//  Has to be put into the wait list.
		conflict = true;
	}

	if (CC_ALG == WAIT_DIE && !conflict) {
		// only allow it to be inserted 
		// in the wait queue if it is newer than some of them in the wait queue.
		// if (waiters_head && txn->get_ts() < waiters_head->txn->get_ts())
		if (waiters_head && txn->get_ts() < waiters_head->txn->get_ts())
			conflict = true;
	}

	// Some txns coming earlier is waiting. Should also wait.
	if (CC_ALG == DL_DETECT && waiters_head != NULL)
		conflict = true;
	
	if (conflict) { 
		// Cannot be added to the owner list.
		if (CC_ALG == NO_WAIT) {
			rc = Abort;
			goto final;
		} else if (CC_ALG == DL_DETECT) {
			LockEntry * entry = get_entry();
			entry->txn = txn;
			entry->type = type;
			LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
			waiter_cnt ++;
            txn->lock_ready = false;
            rc = WAIT;
		} else if (CC_ALG == WAIT_DIE) {
            ///////////////////////////////////////////////////////////
            //  - T is the txn currently running
			//	IF T.ts < ts of all owners
			//		T can wait
            //  ELSE
            //      T should abort
            //////////////////////////////////////////////////////////

			bool canwait = true;
			LockEntry * en = owners;
			while (en != NULL) {
                if (en->txn->get_ts() < txn->get_ts()) {
					canwait = false;
					break;
				}
				en = en->next;
			}
			
			if (canwait) {
				// insert txn to the right position
				// the waiter list is always in timestamp decreasing order, tail get the lock firstly.
				LockEntry * entry = get_entry();
				entry->txn = txn;
				entry->type = type;
				en = waiters_head; // the oldest.
				while (en != NULL && txn->get_ts() < en->txn->get_ts()) 
					en = en->next;
				if (en) {
					LIST_INSERT_BEFORE(en, entry);
					if (en == waiters_head)
						waiters_head = entry;
				} else 
					LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
				waiter_cnt ++;
                txn->lock_ready = false;
                rc = WAIT;
            }
            else 
                rc = Abort;
        } else if (CC_ALG == WOUND_WAIT) {
            ///////////////////////////////////////////////////////////
            //  - T is the txn currently running
			//	IF T.ts < ts of all owners
			//		T wound them
            //  ELSE
            //      T should wait
            //////////////////////////////////////////////////////////

			bool wound = true;
			LockEntry * en = owners, * prev = NULL;
			while (en != NULL) {
                if (en->txn->get_ts() < txn->get_ts()) {
					wound = false;
					break;
				}
				en = en->next;
			}
		#ifdef DEBUG_WOUND
			txn->cur_owner_id = owners->txn->get_thd_id();		
		#endif	
			if (wound) {
				// T is older than all the owners, wound them.
				en = owners;
				// if (owners == NULL) {
				// 	printf("me: %d, counter = %d, lock_type = %d, owner_cnt = %d, waiter_cnt = %d, woundee_cnt = %d\n", 
				// 	txn->get_thd_id(), counter, lock_type, owner_cnt, waiter_cnt, woundee_cnt);
				// }
				// ASSERT(owners != NULL);
				int temp = woundee_cnt;
				while (en != NULL) {
					en->txn->wound = true;
					txn->wound_other = true;
					// printf("%d wound %d \n", (int)txn->get_thd_id(), (int)en->txn->get_thd_id());
				#ifdef DEBUG_WOUND
					txn->last_wound = en->txn->get_thd_id();
					// printf("%d wound %d cnt = %d. \n", (int)txn->get_thd_id(), (int)en->txn->get_thd_id(), en->txn->wound_cnt);
				#endif
					prev = en;
					en = en->next;
					prev->next = prev->prev = NULL;
					STACK_PUSH(woundees, prev);
					woundee_cnt ++;
					// delete them from the owner list directly.
					owner_cnt --;
				}
				owners = NULL;
				// then put myself in the owners but with unready status.
				ASSERT(owner_cnt == 0);
				LockEntry * entry = get_entry();
				entry->txn = txn;
				entry->type = type;
				entry->wound = true;
				entry->next = entry->prev = NULL;
				entry->come_from = 3;
				entry->kill_who = woundees->txn->get_thd_id();
				entry->kill_num = woundee_cnt - temp;
				STACK_PUSH(owners, entry);
				owner_cnt += 1;
				lock_type = type;
				// Althrough we wound the owner, we cannot acquire the lock immediately.
				rc = WAIT;
				txn->lock_ready = false;
			} else { // wait
				// insert txn to the right position 
				// the waiter list is always in timestamp decreasing order, tail get the lock firstly.
				LockEntry * entry = get_entry();
				entry->txn = txn;
				entry->type = type;
				entry->wound = false;
				en = waiters_head; // the oldest.
				while (en != NULL && txn->get_ts() < en->txn->get_ts()) 
					en = en->next;
				if (en) {
					LIST_INSERT_BEFORE(en, entry);
					if (en == waiters_head)
						waiters_head = entry;
				} else 
					LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
				waiter_cnt ++;
                txn->lock_ready = false;
                rc = WAIT;
            }
		}
	} else {
		LockEntry * entry = get_entry();
		entry->type = type;
		entry->txn = txn;
		entry->wound = false;
		entry->come_from = 1;
		STACK_PUSH(owners, entry);
	#ifdef DEBUG_WOUND
		owner_list[oo++] = entry->txn->get_thd_id();
		owner_ts_list[tt++] = entry->txn->get_ts();
	#endif
		owner_cnt ++;
		lock_type = type;
		if (CC_ALG == DL_DETECT) 
			ASSERT(waiters_head == NULL);
		if (CC_ALG == WOUND_WAIT && woundee_cnt > 0) {
			// The owners is the wounder, and is not running.
			// ASSERT(en->txn->lock_ready == false);
			rc = WAIT;
			txn->lock_ready = false;
		} else {
			rc = RCOK;
		}
	}
final:
	
	if (rc == WAIT && CC_ALG == DL_DETECT) {
		// Update the waits-for graph
		ASSERT(waiters_tail->txn == txn);
		txnids = (uint64_t *) mem_allocator.alloc(sizeof(uint64_t) * (owner_cnt + waiter_cnt), part_id);
		txncnt = 0;
		LockEntry * en = waiters_tail->prev;
		while (en != NULL) {
			if (conflict_lock(type, en->type)) 
				txnids[txncnt++] = en->txn->get_txn_id();
			en = en->prev;
		}
		en = owners;
		if (conflict_lock(type, lock_type)) 
			while (en != NULL) {
				txnids[txncnt++] = en->txn->get_txn_id();
				en = en->next;
			}
		ASSERT(txncnt > 0);
	}

	if (g_central_man)
		glob_manager->release_row(_row);
	else
#ifdef USE_SPINLOCK
		pthread_spin_unlock( latch );
#else
		pthread_mutex_unlock( latch );
#endif

	return rc;
}


RC Row_lock::lock_release(txn_man * txn) {	
	int road = 0;
	if (g_central_man)
		glob_manager->lock_row(_row);
	else 
#ifdef USE_SPINLOCK
		pthread_spin_lock( latch );
#else
		pthread_mutex_lock( latch );
#endif

	// Try to find the entry in the owners
	LockEntry * en = owners;
	LockEntry * prev = NULL;

	while (en != NULL && en->txn != txn) {
		prev = en;
		en = en->next;
	}
	if (en) { // find the entry in the owner list
		// ASSERT(woundee_cnt == 0);
		if (prev) prev->next = en->next;
		else owners = en->next;
	#ifdef DEBUG_WOUND
		release_list[rr++] = en->txn->get_thd_id();
	#endif
		return_entry(en);
		road = 1;
		owner_cnt --;
		if (owner_cnt == 0)
			lock_type = LOCK_NONE;
	} else {
		// Not in owners list, try waiters list.
		en = waiters_head;
		while (en != NULL && en->txn != txn)
			en = en->next;
		if (CC_ALG != WOUND_WAIT)
			ASSERT(en != NULL);
		if (en == NULL) {
			// I was wounded by others.
			ASSERT(CC_ALG == WOUND_WAIT);
			// find me in the woundees.
			en = woundees;
			prev = NULL;
			while (en != NULL && en->txn != txn) {
				prev = en;
				en = en->next;
			}
			ASSERT(en != NULL);
			if (prev) prev->next = en->next;
			else woundees = en->next;
			woundee_cnt --;
			if (woundee_cnt == 0)
				woundees = NULL;
			road = 2;
			ASSERT(woundee_cnt >= 0);
			return_entry(en);
			asm volatile ("mfence" ::: "memory");
			if (woundee_cnt == 0) {
				// make owners alive.
				// if (owner_cnt != 0 || owners->txn->lock_ready == true)
				// 	printf("me: %d, owner_cnt = %d, cur_owner = %d, lock = %d\n", txn->get_thd_id(), owner_cnt, owners->txn->get_thd_id(), owners->txn->lock_ready == true);
				// ASSERT(owner_cnt != 0);
				en = owners;
				while (en != NULL) {
					// printf("me: %d, owner = %d wound = %d, lock_addr = %p, lock = %d\n", txn->get_thd_id(), en->txn->get_thd_id(), txn->wound, &(en->txn->lock_ready), en->txn->lock_ready);
					// sleep(1);
					// if (en->txn->lock_ready != false)
					// 	printf("me = %d, fail to change owner (%d) to true.\n", txn->get_thd_id(), en->txn->get_thd_id());
					// if (en->wound == true) {
					// 	ASSERT(en->txn->lock_ready == false);
					// 	en->wound = false;
					// 	en->txn->lock_ready = true;
					// }
					// 	printf("me = %d, change owner (%d) to true (%p).\n", txn->get_thd_id(), en->txn->get_thd_id(), this);
					if (en->wound == false) {
						ASSERT(en->txn->lock_ready == true);
					} else {
						ASSERT(en->txn->lock_ready == false);
					}
					en->txn->lock_ready = true;
					en = en->next;
				}
			}
		} else {
		#ifdef DEBUG_WOUND
			release_list[rr++] = en->txn->get_thd_id();
		#endif
			LIST_REMOVE(en);
			if (en == waiters_head)
				waiters_head = en->next;
			if (en == waiters_tail)
				waiters_tail = en->prev;
			return_entry(en);
			waiter_cnt --;
			road = 3;
		}
	}
	if (owner_cnt == 0)
		ASSERT(lock_type == LOCK_NONE);
#if DEBUG_ASSERT && CC_ALG == WAIT_DIE 
		for (en = waiters_head; en != NULL && en->next != NULL; en = en->next)
			assert(en->next->txn->get_ts() < en->txn->get_ts());
#endif

	LockEntry * entry;
	// If any waiter can join the owners, just do it!
	bool add_new_one = false;
#if CC_ALG != WOUND_WAIT
	while (waiters_head && !conflict_lock(lock_type, waiters_head->type)) {
		LIST_GET_HEAD(waiters_head, waiters_tail, entry);
#else 
	while (waiters_tail && !conflict_lock(lock_type, waiters_tail->type)) {
		LIST_GET_TAIL(waiters_head, waiters_tail, entry);
#endif
	#ifdef DEBUG_WOUND
		owner_list[oo++] = entry->txn->get_thd_id();
		owner_ts_list[tt++] = entry->txn->get_ts();
	#endif
		STACK_PUSH(owners, entry);
		add_new_one = true;
		// printf("me = %d, change waiter (%d) to true, road = %d, woundee_cnt = %d, owner_cnt = %d, waiter_cnt = %d.\n", txn->get_thd_id(), entry->txn->get_thd_id(), road, woundee_cnt, owner_cnt, waiter_cnt);
		owner_cnt ++;
		waiter_cnt --;

		// ASSERT(woundee_cnt == 0);
		// if (entry->txn->lock_ready != false) {
		// 	printf("me: %d, %d is already ready.(%p)\n", txn->get_thd_id(), entry->txn->get_thd_id(), this);
		// 	printf("own_cnt = %d:\t", owner_cnt);
		// 	en = owners;
		// 	while (en != NULL) {
		// 		printf("%d\t", en->txn->get_thd_id());
		// 		en = en->next;
		// 	}
		// 	printf("\n");
		// }
		entry->come_from = 2;
		// if (entry->txn->lock_ready != false)
		// 	printf("me: %d, already change %d to true. (%p)\n", txn->get_thd_id(), entry->txn->get_thd_id(), this);
		ASSERT(entry->txn->lock_ready == false);
		entry->txn->lock_ready = true;
		// printf("me: %d, change %d to true. (%p)\n", txn->get_thd_id(), entry->txn->get_thd_id(), this);
		lock_type = entry->type;
	}
	
	if (owner_cnt == 0) {
		ASSERT(lock_type == LOCK_NONE);
	}

	ASSERT((owners == NULL) == (owner_cnt == 0));

	if (g_central_man)
		glob_manager->release_row(_row);
	else
#ifdef USE_SPINLOCK
		pthread_spin_unlock( latch );
#else
		pthread_mutex_unlock( latch );
#endif

	return RCOK;
}

bool Row_lock::conflict_lock(lock_t l1, lock_t l2) {
	if (l1 == LOCK_NONE || l2 == LOCK_NONE)
		return false;
    else if (l1 == LOCK_EX || l2 == LOCK_EX)
        return true;
	else
		return false;
}

LockEntry * Row_lock::get_entry() {
	LockEntry * entry = (LockEntry *) 
		mem_allocator.alloc(sizeof(LockEntry), _row->get_part_id());
	return entry;
}
void Row_lock::return_entry(LockEntry * entry) {
	mem_allocator.free(entry, sizeof(LockEntry));
}

