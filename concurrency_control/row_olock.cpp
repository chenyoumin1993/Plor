#include "row.h"
#include "txn.h"
#include "row_olock.h"
#include "mem_alloc.h"
#include "manager.h"

extern txn_man *txn_tb[];

RingBuffer la[THREAD_CNT][BUFFER_LEN]; // Per-thread locking table.

// thread_local int lock_cnt = 0;


#if CC_ALG == PLOR

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

    mtx_get();

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
            mtx_release();
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
                mtx_release();
                return Abort;
            }
        }
    }
    // if (do_print) {
    //     printf("lock_wait, txn=%d, row=%p\n", txn->get_thd_id(), _row);
    // }
    // mtx.unlock();
    mtx_release();
    txn->conflict_type = 2;
    return WAIT;
}

RC Row_dlock::lock_get_sh(lock_t type, txn_man *txn) {
    Owner o;
    txn_man *cur_owner;
    uint64_t temp;
_start:
    // mtx_get();
    o._owner = owner._owner;
    temp = o.owner;
    cur_owner = (txn_man *)temp;
    if (!txn->readonly) {
        bmpRd->Set(txn->get_thd_id());
    }
    if (bmpRd->isSet(63) && cur_owner != NULL) {
        // mtx_release();
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
    } else {
        // Successfully get the shared lock.
        // mtx_release();
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
    // mtx_get();
    bmpRd->Set(63);
    // mtx_release();
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

    mtx_get();
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
        mtx_release();
        return RCOK;
    }

    if (bmpRd->isSet(63))
        bmpRd->Unset(63);

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


    if (txn_old != NULL) {
        // asm volatile ("sfence" ::: "memory");
        assert(txn_old != txn);
        asm volatile ("sfence" ::: "memory");
        txn_old->lock_ready = true;
    }
    mtx_release();
    return RCOK;
}

RC Row_dlock::lock_release_sh(lock_t type, txn_man *txn) {
    // Unset myself is enough.
    if (txn->readonly)
        return RCOK;
    
    // mtx_get();
    
    if (THREAD_CNT <= MAX_THREAD_ATOMIC) {
        bmpRd->Unset(txn->get_thd_id());
    } else {
        if (!dirLock.unlock((uint8_t)txn->get_thd_id(), type)) {
            // No available slots in dirLock.
            assert(readLock.unlock((uint8_t)txn->get_thd_id()) == true);
        }
    }
    // lock_cnt -= 1;

    // mtx_release();
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
        assert(bmpWr->isSet(txn->get_thd_id()));
        txn_man *txn_old = find_oldest(); // return NULL if empty. TBD...
        if (txn_old != txn) {
            // not myself.
            return;
        }
        // mtx_get();
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
        // mtx_release();
    } else if (!o.wound && cur_owner->get_ts() > txn->get_ts()) { // FIXME.
        // wound it.
        // mtx_get();
        o_new.wound = 1;
        if (__sync_bool_compare_and_swap((uint64_t *)&owner._owner, (uint64_t)o._owner, (uint64_t)o_new._owner)) {
            // I can make sure it is still that owner.
            asm volatile ("sfence" ::: "memory");
            cur_owner->wound = true;
        }
        // mtx_release();
    }
}

txn_man* Row_dlock::find_oldest() {
    txn_man *txn = NULL;
    uint64_t ts = 0;
    if (THREAD_CNT <= MAX_THREAD_ATOMIC) {
        if (bmpWr->isEmpty())
            return txn;
        for (int i = 0; i < THREAD_CNT; ++i) {
            if (bmpWr->isSet(i)) {
                while (txn_tb[i] == NULL) ;
                if ((txn_tb[i]->get_ts() < ts) || (ts == 0)) {
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