#include "row.h"
#include "txn.h"
#include "row_olock.h"
#include "mem_alloc.h"
#include "manager.h"

extern txn_man *txn_tb[];

RingBuffer la[THREAD_CNT][BUFFER_LEN]; // Per-thread locking table.

// thread_local int lock_cnt = 0;

#if CC_ALG == OLOCK

void Row_olock::init(row_t *row) {
    _row = row;
    owner.owner = NULL;
    waiters = NULL;
    wound = 0;
}

RC Row_olock::lock_get(lock_t type, txn_man *txn) {
    assert(CC_ALG == OLOCK);
    assert(type == LOCK_EX);
    txn->lock_ready = false;
    RC rc = WAIT;
    // 1. Insert a lock in the waiter list.
    OLockEntry *n = (OLockEntry *)txn->reserve();
    n->txn = txn;
    insert(n);
    // 2. Check if owner is NULL, become the owner if true.
_start:
    Owner o = owner;
    if (o.owner == NULL) {
        // Try to become the owner.
        if (!__sync_bool_compare_and_swap(&owner.owner, (uint64_t)NULL, (uint64_t)txn)) {
            // fail. wait.
            ;
        } else {
            // I'm the owner.
            txn->lock_ready = true;
            rc = RCOK;
        }
    } else if (!o.wound) {
        // check if I can wound it.
        if (o.owner->get_ts() > txn->get_ts()) {
            // o might already leave ..... be careful.
            Owner w = o;
            w.wound  = 1;
            if (__sync_bool_compare_and_swap(&owner.owner, (uint64_t)o.owner, (uint64_t)w.owner)) {
                // I can make sure it is still that owner.
                ((txn_man *)o.owner)->wound = true;
            } else {
                // May fail if the owner is changing it.
                asm volatile ("lfence" ::: "memory");
                goto _start;
            }
        }
    }
    // queue.push((uint64_t)txn);
    return rc;
}

RC Row_olock::lock_release(lock_t type, txn_man *txn) {
    // 0. Remove myself from the list.
    remove(txn);
    // 1. choose one with the oldest ts from the list and activate it.
    Owner o1, o2;
_start:
    o1 = o2 = owner;
    o1.wound = 0;
    if (o1.owner == txn && o2.wound) {
        // Someone else wound me, so I need to wait until wound arrive.
        while (txn->wound == false) {
            asm volatile ("lfence" ::: "memory");
        }
    }
    // find the oldest and make him the owner.
    txn_man *txn_old = find_oldest(); // return NULL if empty.
    if (!__sync_bool_compare_and_swap(&owner.owner, (uint64_t)o2.owner, (uint64_t)txn_old)) {
        // Someone is wounding me!
        // asm volatile ("lfence" ::: "memory");
        goto _start;
    } else if (txn_old != NULL) {
        txn_old->lock_ready = true;
    }
    // queue.push((uint64_t)txn + 1);
    return RCOK;
}

void Row_olock::poll_lock_state(txn_man *txn) {
    // Check if the owner is empty so someone can become the new owner.
    Owner o = owner;
    o.wound = 0;
    if (o.owner == NULL) {
        txn_man *txn_old = find_oldest(); // return NULL if empty.
        if (__sync_bool_compare_and_swap(&owner.owner, (uint64_t)NULL, (uint64_t)txn_old)) {
            if (txn_old != NULL)
                txn_old->lock_ready = true;
        }
    }
    if (o.owner != NULL && o.owner->get_ts() > txn->get_ts()) { // FIXME.
        // wound it.
        Owner w = o;
        w.wound  = 1;
        if (__sync_bool_compare_and_swap(&owner.owner, (uint64_t)o.owner, (uint64_t)w.owner)) {
            // I can make sure it is still that owner.
            ((txn_man *)o.owner)->wound = true;
        }
    }
}

// FIXME:
// An opt choice: keep the list ordered with two advantages:
// 1. Threads insert from different position, improve concurrency.
// 2. The owner can choose a candidate from the header.
// However, the benefit of (1) might not that obvious.
// Still deserves a try.

void Row_olock::insert(OLockEntry *n) {
	// Insert myself to the waiters.
_start:
	// Read a snapshot
    asm volatile ("lfence" ::: "memory");
	uint64_t h = (uint64_t)waiters;
	if ((OLockEntry *)h == NULL) {
		n->next = NULL;
        asm volatile ("mfence" ::: "memory");
		if (!__sync_bool_compare_and_swap(&waiters, (uint64_t)NULL, (uint64_t)n)) {
            // asm volatile ("lfence" ::: "memory");
			goto _start;
        }
	} else {
		// Read a snapshot of waiters->next.
		n->next = (OLockEntry *)h;
        asm volatile ("mfence" ::: "memory");
		if (!__sync_bool_compare_and_swap(&waiters, (uint64_t)h, (uint64_t)n)) {
            // asm volatile ("lfence" ::: "memory");
			goto _start;
		}
	}
}

void Row_olock::remove(txn_man *txn) {
	OLockEntry *cur, *prev;
    int retry = 0;
    // usleep(100);
_start:
	cur = waiters;
	prev = NULL;
	// Find myself
	while (cur != NULL) {
		uint64_t next = (uint64_t)cur->next;
        if (cur->txn == txn) {
            assert(!is_mark_set(next));
			break;
        }
		if (is_mark_set(next)) {
            // usleep(100);
            asm volatile ("lfence" ::: "memory");
            // uint64_t ttt = next - 1;
            // OLockEntry *temp = ((OLockEntry *)ttt);
            // printf("txn (me) = %p, cur->txn = %p, owner = %p, last_unset = %p, next->txn = %p\n", 
            //     txn, cur->txn, owner.owner, last_unset, temp->txn);
            
            // uint64_t t = 0;
            // queue.pop(t);
            // printf("%p----\n", t);
            // printf("next = %d\n", next);
            // assert(false);
			goto _start;
        }
		prev = cur;
		cur = (OLockEntry *)next;
	}
	if (cur == NULL) {
		// cannot find myself.
        if (retry < 10) {
            asm volatile ("lfence" ::: "memory");
            goto _start;
        } else {
            assert(false);
        }
	} else {
		// Set myself to deleted.
        assert(cur->txn == txn && !cur->deleted);
		uint64_t t1, t2;
		t1 = t2 = (uint64_t)cur->next;
        assert(!is_mark_set((uint64_t)cur->next));
		set_mark(&t2);
		if (!__sync_bool_compare_and_swap(&cur->next, (uint64_t)t1, (uint64_t)t2)) 
			goto _start;
		asm volatile ("mfence" ::: "memory");
		if (prev == NULL) {
			// I'm the first
			if (!__sync_bool_compare_and_swap(&waiters, (uint64_t)cur, (uint64_t)t1)) {
                // label = 1;
                // I need to unset the mark, this can success with one atomic op, since the mart has already been set.
                // last_unset = txn;
                assert(__sync_bool_compare_and_swap(&cur->next, t2, t1));
                // asm volatile ("lfence" ::: "memory");
				goto _start;
			}
		} else {
            // We assue prev->next is unmarked.
			if (!__sync_bool_compare_and_swap(&prev->next, (uint64_t)cur, (uint64_t)t1)) {
                // label = 2;
                // last_unset = txn;
                assert(__sync_bool_compare_and_swap(&cur->next, t2, t1));
                // asm volatile ("lfence" ::: "memory");
				goto _start;
			}
		}
        asm volatile ("mfence" ::: "memory");
        assert(__sync_bool_compare_and_swap(&cur->next, (uint64_t)t2, (uint64_t)0));
	}
}

txn_man* Row_olock::find_oldest() {
	OLockEntry *cur;
    uint64_t ts = -1;
    txn_man *txn = NULL;
_start:
	cur = waiters;
	while (cur != NULL) {
		uint64_t next = (uint64_t)cur->next;
		if (is_mark_set(next)) {
			goto _start;
        }
		if (cur->txn->get_ts() < ts) {
            txn = cur->txn;
            ts = cur->txn->get_ts();
        }
		cur = (OLockEntry *)next;
	}
    return txn;
}


#elif CC_ALG == DLOCK

void Row_dlock::init(row_t *row) {
    _row = row;
    owner._owner = 0;
    s_lock = 0;
    readers = 0;
    bmpWr = new BitMap(THREAD_CNT);
    bmpRd = new BitMap(THREAD_CNT);
}


RC Row_dlock::lock_get(lock_t type, txn_man *txn) {
    if (type == LOCK_SH) {
        return lock_get_sh(type, txn);
    } else {
        return lock_get_ex(type, txn);
    }
}

RC Row_dlock::lock_get_ex(lock_t type, txn_man *txn) {
    assert(type == LOCK_EX);
    // assert((owner.tid - 1) != (uint8_t)txn->get_thd_id());
    txn->lock_ready = false;
    // 1. Check if owner is NULL, become the owner if true.
    Owner o, o_new;

_start:
    // Read a snapshot.
    o._owner = owner._owner;
    o_new._owner = o._owner;
    uint64_t temp = o.owner;
    txn_man *cur_owner = (txn_man *)temp;
    
    if (cur_owner == NULL) {
        // Try to become the owner.
        assert(!o.wound);
        assert(o.tid == 0);
        o_new.owner = (uint64_t)txn;
        o_new.pad = 0;
        o_new.tid = (uint8_t)txn->get_thd_id() + 1;
        if (__sync_bool_compare_and_swap((uint64_t *)&owner._owner, (uint64_t)o._owner, (uint64_t)o_new._owner)) {
            txn->lock_ready = true;
            // txn->owner_before = o.tid;
            // txn->owner_cur = o_new.tid;
            // txn->lock_addr = (void*)(&owner._owner);
            // if (do_print) {
            //     printf("lock_direct, txn=%d, row=%p\n", txn->get_thd_id(), _row);
            // }
            // mtx.unlock();
            return RCOK;
        } else {
            // Can fail if others become the owner.
            asm volatile ("lfence" ::: "memory");
            goto _start;
        }
    } else {
        // Try if I can wound it.
        if ((o.tid - 1) == (uint8_t)txn->get_thd_id()) {
            // printf("%d, tid = %d\n", txn->get_thd_id(), o.tid - 1);
            // assert((o.tid - 1) != (uint8_t)txn->get_thd_id());
        }
        if (cur_owner->get_ts() > txn->get_ts() && !o.wound) {
            o_new.wound = 1;
            if (__sync_bool_compare_and_swap((uint64_t *)&owner._owner, (uint64_t)o._owner, (uint64_t)o_new._owner)) {
                cur_owner->wound = true;
            } else {
                // Can fail if other readers are wounding it.
                asm volatile ("lfence" ::: "memory");
                goto _start;
            }
        }
    }
    // Add myself to the bmp and wait.
    if (THREAD_CNT <= MAX_THREAD_ATOMIC) {
        bmpWr->Set(txn->get_thd_id());
    } else {
        if (!dirLock.lock((uint8_t)txn->get_thd_id(), type)) {
            // No available slots in dirLock.
            if (!writeLock.lock((uint8_t)txn->get_thd_id(), txn->get_ts(), (uint64_t)this)) {
                return Abort;
            }
        }
    }
    // if (do_print) {
    //     printf("lock_wait, txn=%d, row=%p\n", txn->get_thd_id(), _row);
    // }
    // mtx.unlock();
    return WAIT;
}

RC Row_dlock::lock_get_sh(lock_t type, txn_man *txn) {
    Owner o;
    txn_man *cur_owner;
    uint64_t temp;
_start:
    o._owner = owner._owner;
    temp = o.owner;
    cur_owner = (txn_man *)temp;
    if (!txn->readonly)
        bmpRd->Set(txn->get_thd_id());
    if (bmpRd->isSet(63) && cur_owner != NULL) {
        if (cur_owner->get_ts() < txn->get_ts()) {
            if (!txn->readonly)
                bmpRd->Unset(txn->get_thd_id());
            while (bmpRd->isSet(63) && !txn->wound) {
                PAUSE
                asm volatile ("lfence" ::: "memory");
            }
            if (txn->wound && !txn->read_committed) {
                return Abort;
            } else {
                goto _start;
            }
        }
    }
    return RCOK;
}

RC Row_dlock::lock_release(lock_t type, txn_man *txn) {
    if (type == LOCK_SH) {
        return lock_release_sh(type, txn);
    } else {
        return lock_release_ex(type, txn);
    }
}

RC Row_dlock::validate(txn_man *txn) {
    if (txn->wound) {
        return Abort;
    }
    
    bmpRd->Set(63);
    // Check the readers, wound all the young readers and wait for all the old readers.
    // Read a snapshot:
    // BitMap bmpRdSnapshot = *bmpRd;

    if (bmpRd->isEmpty())
        return RCOK;

    for (int i = 0; i < THREAD_CNT; ++i) {
        // wait for older TXs and wound younger TXs.
        if (i != 63 && bmpRd->isSet(i)) {
            if (txn_tb[i]->get_ts() > txn->get_ts()) {
                // Kill him.
                while (txn_tb[i] == NULL) ;
                txn_tb[i]->wound = true;
            } else {
                // Wait it.
                while (bmpRd->isSet(i) && !txn->wound && (txn_tb[i]->get_ts() < txn->get_ts())) {
                    asm volatile ("lfence" ::: "memory");
                    PAUSE
                }
                if (txn->wound) {
                    // bmpRd->Unset(txn->get_thd_id());
                    return Abort;
                }
            }
        }
    }
    // bmpRd->Unset(txn->get_thd_id());
    return RCOK;
}

RC Row_dlock::lock_release_ex(lock_t type, txn_man *txn) {
    Owner o, o_new;
    // uint32_t total_readers;

    // // If I was wounded, don't need to care about the readers, release the lock directly.
    // if (txn->wound) {
    //     goto _release;
    // }

    // if (bmpRd->isEmpty())
    //     goto _release;

    // // Switch to EX mode and clean the readers.
    // if (THREAD_CNT <= MAX_THREAD_ATOMIC) {
    //     // Solution A.
    //     bmpRd->Set(txn->get_thd_id());
    //     // Check the readers, wound all the young readers and wait for all the old readers.
    //     for (int i = 0; i < THREAD_CNT; ++i) {
    //         // wait for older TXs and wound younger TXs.
    //         if (bmpRd->isSet(i) && (i != (int)txn->get_thd_id())) {
    //             if (txn_tb[i]->get_ts() > txn->get_ts()) {
    //                 // Kill him.
    //                 while (txn_tb[i] == NULL) ;
    //                 txn_tb[i]->wound = true;
    //             } else {
    //                 // Wait it.
    //                 while (bmpRd->isSet(i) && !txn->wound && (txn_tb[i]->get_ts() < txn->get_ts())) {
    //                     asm volatile ("lfence" ::: "memory");
    //                 }
    //                 if (txn->wound) {
    //                     bmpRd->Unset(txn->get_thd_id());
    //                     goto _release;
    //                 }
    //             }
    //         }
    //     }
    //     bmpRd->Unset(txn->get_thd_id());
    // } else {
    //     // Solution B.
    //     // 1. Scan the bitmap.
    //     uint32_t *shared = (uint32_t *)dirLock._lt; // Point to the first 4~B (readBmp).
    //     if (*shared != 0) {
    //         for (int off = 0; off < 4; ++off) {
    //             auto i = dirLock._lt[off];
    //             if (i != 0) {
    //                 if (txn_tb[i]->get_ts() > txn->get_ts()) {
    //                     // Kill him.
    //                     while (txn_tb[i] == NULL) ;
    //                     txn_tb[i]->wound = true;
    //                 } else {
    //                     // Wait it.
    //                     while (bmpRd->isSet(i) && !txn->wound) {
    //                         asm volatile ("lfence" ::: "memory");
    //                     }
    //                     if (txn->wound) {
    //                         bmpRd->Unset(txn->get_thd_id());
    //                         goto _release;
    //                     }
    //                 }
    //             }
    //         }
    //     }
        
    //     // 2. Scan the ringbuf.
    //     uint64_t temp = readLock._slot; // Read a snapshot.
    //     LockItem *me = (LockItem *)&temp;
    //     if (me->off != (uint16_t)-1 && me->ID != (uint16_t)-1) {
    //         for (int i = me->tail; i < me->head; i++) {
    //             if (la[me->ID][me->off].e[i % RING_SIZE].ts > txn->get_ts()) {
    //                 auto tid = la[me->ID][me->off].e[i % RING_SIZE].tid;
    //                 while (txn_tb[tid] == NULL) ;
    //                 txn_tb[tid]->wound = true;
    //             } else {
    //                 while (la[me->ID][me->off].e[i % RING_SIZE].ts != 0 && !txn->wound) {
    //                     asm volatile ("lfence" ::: "memory");
    //                 }
    //                 if (txn->wound)
    //                     goto _release;
    //             }
    //         }
    //     }
    // }

// _release:
    // if (do_print) {
    //     if (txn->wound) {
    //         printf("unlock for wound, txn=%d, row=%p\n", txn->get_thd_id(), _row);
    //     } else {
    //         printf("unlock, txn=%d, row=%p\n", txn->get_thd_id(), _row);
    //     }
    // }
    if (THREAD_CNT <= MAX_THREAD_ATOMIC) {
        if (bmpWr->isSet(txn->get_thd_id()))
            bmpWr->Unset(txn->get_thd_id());
    } else {
        // Extra overhead, not good.
        if (!dirLock.unlock((uint8_t)txn->get_thd_id(), type)) {
            // No available slots in dirLock.
            writeLock.unlock((uint8_t)txn->get_thd_id());
        }
    }

_start:
    o._owner = owner._owner;
    o_new._owner = o._owner;
    if ((uint8_t)txn->get_thd_id() != (o.tid - 1)) {
        // printf("txn = %d, cur_txn = %d, row = %p\n", txn->get_thd_id(), o.tid, _row);
        return RCOK;
    }

    if (bmpRd->isSet(63))
        bmpRd->Unset(63);

    // if (_row->get_primary_key() == 11)
    //     txn->locked -= 1;

    // if (txn->locked < 0) {
    //     printf("owner = %d, me = %d, wound = %d\n", o.tid - 1, txn->get_thd_id(), txn->wound);
    // }
        // printf("%d try unlock.\n", txn->get_thd_id());
    // assert((uint8_t)txn->get_thd_id() == o.tid);
    // uint64_t temp = o.owner;
    // txn_man *cur_owner = (txn_man *)temp;

    // if (cur_owner == txn && o.wound) {
    //     // Someone else wound me, so I need to wait until wound arrive.
    //     while (txn->wound == false) {
    //         PAUSE
    //         asm volatile ("lfence" ::: "memory");
    //     }
    // }

    // find the oldest and make him the owner.
    txn_man *txn_old = NULL;//find_oldest(); // return NULL if empty.
    // if (txn_old != NULL && txn_old->wound)
    //     txn_old = NULL;
    o_new.owner = (uint64_t)txn_old;
    o_new.wound = 0;
    if (txn_old != NULL) {
        o_new.tid = (uint8_t)txn_old->get_thd_id() + 1;
    } else {
        o_new.tid = 0;
    }

    if (!__sync_bool_compare_and_swap((uint64_t *)&owner._owner, (uint64_t)o._owner, (uint64_t)o_new._owner)) {
        // Someone is wounding me!
        goto _start;
    }

    // if (_row->get_primary_key() == 11)
    //     usleep(1);

    if (txn_old != NULL) {
        // asm volatile ("sfence" ::: "memory");
        assert(txn_old != txn);
        asm volatile ("sfence" ::: "memory");
        txn_old->lock_ready = true;
    }
    return RCOK;
}

RC Row_dlock::lock_release_sh(lock_t type, txn_man *txn) {
    // Unset myself is enough.
    if (txn->readonly)
        return RCOK;
   if (THREAD_CNT <= MAX_THREAD_ATOMIC) {
        bmpRd->Unset(txn->get_thd_id());
    } else {
        if (!dirLock.unlock((uint8_t)txn->get_thd_id(), type)) {
            // No available slots in dirLock.
            assert(readLock.unlock((uint8_t)txn->get_thd_id()) == true);
        }
    }
    // lock_cnt -= 1;
    return RCOK;
}

// Note that readers don't need to poll state!
void Row_dlock::poll_lock_state(txn_man *txn) {
    // Check if the owner is empty so someone can become the new owner.
    PAUSE
    asm volatile ("lfence" ::: "memory");
    Owner o, o_new;
    o._owner = owner._owner;
    o_new._owner = o._owner;
    uint64_t temp = o.owner;
    txn_man *cur_owner = (txn_man *)temp;

    if (cur_owner == NULL) {
        assert(o.wound != 1);
        assert(o.tid == 0);
        txn_man *txn_old = find_oldest(); // return NULL if empty. TBD...
        if (txn_old != txn) {
            // not myself.
            return;
        }
        o_new.owner = (uint64_t)txn_old;
        if (txn_old != NULL) {
            o_new.tid = (uint8_t)txn_old->get_thd_id() + 1;
        } else {
            o_new.tid = 0;
        }

        if (__sync_bool_compare_and_swap((uint64_t *)&owner._owner, (uint64_t)o._owner, (uint64_t)o_new._owner)) {
            asm volatile ("sfence" ::: "memory");
            if (txn_old != NULL)
                txn_old->lock_ready = true;
        }
    } else if (!o.wound && cur_owner->get_ts() > txn->get_ts()) { // FIXME.
        // wound it.
        o_new.wound = 1;
        if (__sync_bool_compare_and_swap((uint64_t *)&owner._owner, (uint64_t)o._owner, (uint64_t)o_new._owner)) {
            // I can make sure it is still that owner.
            asm volatile ("sfence" ::: "memory");
            cur_owner->wound = true;
        }
    }
}

txn_man* Row_dlock::find_oldest() {
    txn_man *txn = NULL;
    uint ts = -1;
    if (THREAD_CNT <= MAX_THREAD_ATOMIC) {
        if (bmpWr->isEmpty())
            return txn;
        for (int i = 0; i < THREAD_CNT; ++i) {
            if (bmpWr->isSet(i)) {
                while (txn_tb[i] == NULL) ;
                if (txn_tb[i]->get_ts() < ts) {
                    ts = txn_tb[i]->get_ts();
                    txn = txn_tb[i];
                }
            }
        }
    } else {
        uint32_t *temp = (uint32_t *)&dirLock._lt[4];
        uint64_t _slot = writeLock._slot; // Read a snapshot.
        LockItem *me = (LockItem *)&_slot;
        if (*temp == 0 && (me->head == me->tail))
            return txn;
        if (*temp != 0) {
            for (int i = 4; i < 8; ++i) {
                auto tid = dirLock._lt[i];
                if (tid != 0) {
                    while (txn_tb[tid] == NULL) ;
                    if (txn_tb[tid]->get_ts() < ts) {
                        ts = txn_tb[tid]->get_ts();
                        txn = txn_tb[tid];
                    }
                }
            }
        }
        if (me->ID != (uint16_t)-1 && me->off != (uint16_t)-1) {
            for (int i = me->tail; i < me->head; ++i) {
                if (la[me->ID][me->off].e[i].ts < ts) {
                    ts = la[me->ID][me->off].e[i % RING_SIZE].ts;
                    auto tid = la[me->ID][me->off].e[i % RING_SIZE].tid;
                    while (txn_tb[tid] == NULL) ; 
                    txn = txn_tb[tid];
                }
            }
        }
    }
    return txn;
}

#endif