#include "row.h"
#include "txn.h"
#include "row_olock.h"
#include "mem_alloc.h"
#include "manager.h"

extern txn_man *txn_tb[];

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

RC Row_olock::lock_release(txn_man *txn) {
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

void Row_dlock::init(row_t *row) {
    _row = row;
    owner.owner = NULL;
    bmp = new BitMap(THREAD_CNT);
}

RC Row_dlock::lock_get(lock_t type, txn_man *txn) {
    this->type = type;
    if (type == LOCK_SH)
        return RCOK;
    txn->lock_ready = false;
    // 2. Check if owner is NULL, become the owner if true.
    Owner o, o0, o1;
    o = o0 = o1 = owner;
    o0.wound = 0;
    o1.wound = 1;
    if (o0.owner == NULL) {
        // Try to become the owner.
        if (__sync_bool_compare_and_swap(&owner.owner, (uint64_t)NULL, (uint64_t)txn)) {
            txn->lock_ready = true;
            return RCOK;
        }
    } else {
        // Try if I can wound it.
        if (o0.owner->get_ts() > txn->get_ts() && !o.wound) {
            if (__sync_bool_compare_and_swap(&owner.owner, (uint64_t)o0.owner, (uint64_t)o1.owner)) {
                o0.owner->wound = true;
            }
        }
    }
    // Add myself to the bmp and wait.
    bmp->Set(txn->get_thd_id());
    return WAIT;
}

RC Row_dlock::lock_release(txn_man *txn) {
    if (bmp->isSet(txn->get_thd_id()))
        bmp->Unset(txn->get_thd_id());
    Owner o, o0, o1;

_start:
    o = o0 = o1 = owner;
    o0.wound = 0;
    o1.wound = 1;
    if (o0.owner == txn && o.wound) {
        // Someone else wound me, so I need to wait until wound arrive.
        while (txn->wound == false) {
            PAUSE
            asm volatile ("lfence" ::: "memory");
        }
    }

    // find the oldest and make him the owner.
    asm volatile ("lfence" ::: "memory");
    txn_man *txn_old = find_oldest(); // return NULL if empty.

    if (!__sync_bool_compare_and_swap(&owner.owner, (uint64_t)o.owner, (uint64_t)txn_old)) {
        // Someone is wounding me!
        goto _start;
    } else if (txn_old != NULL) {
        txn_old->lock_ready = true;
    }
    return RCOK;
}

void Row_dlock::poll_lock_state(txn_man *txn) {
    // Check if the owner is empty so someone can become the new owner.
    PAUSE
    asm volatile ("lfence" ::: "memory");
    Owner o, o0, o1;
    o = o0 = o1 = owner;
    o0.wound = 0;
    o1.wound = 1;
    if (o0.owner == NULL) {
        assert(o.wound != 1);
        txn_man *txn_old = find_oldest(); // return NULL if empty.
        if (__sync_bool_compare_and_swap(&owner.owner, (uint64_t)NULL, (uint64_t)txn_old)) {
            if (txn_old != NULL)
                txn_old->lock_ready = true;
        }
    }
    if (o0.owner != NULL && !o.wound && o0.owner->get_ts() > txn->get_ts()) { // FIXME.
        // wound it.
        if (__sync_bool_compare_and_swap(&owner.owner, (uint64_t)o0.owner, (uint64_t)o1.owner)) {
            // I can make sure it is still that owner.
            o0.owner->wound = true;
        }
    }
}

txn_man* Row_dlock::find_oldest() {
    txn_man *txn = NULL;
    uint ts = -1;
    if (bmp->isEmpty())
        return txn;
    for (int i = 0; i < THREAD_CNT; ++i) {
        if (bmp->isSet(i)) {
            while (txn_tb[i] == NULL) ;
            if (txn_tb[i]->get_ts() < ts) {
                ts = txn_tb[i]->get_ts();
                txn = txn_tb[i];
            }
        }
    }
    return txn;
}