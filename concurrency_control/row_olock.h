#ifndef ROW_OLOCK_H
#define ROW_OLOCK_H

/*********************************
 Two types of implementation:
 1) maintain a lockfree list.
 2) using a distributed lock.
 ********************************/

#include <city.h>
#include <mutex>

struct OLockEntry {
	txn_man *txn;
	union {
		struct {
			uint64_t deleted : 1;
			uint64_t _next : 63;
		};
		OLockEntry *next;
	};
};

#if CC_ALG == OLOCK
struct Owner {
    union {
        struct {
            uint64_t _owner:63;
            uint64_t wound:1;
        };
        txn_man *owner;
    };
};

#elif CC_ALG == DLOCK

struct __attribute__((packed)) Owner {
    union {
        uint64_t _owner;
        struct {
        	uint8_t tid;
        	union {
                uint8_t pad;
                struct {
                    uint8_t _pad : 7;
                    uint8_t wound : 1;
                };
            };
            uint64_t owner : 48;
        };
    };
};


struct BitMap {
    uint32_t _size;
    uint32_t alloc_size;
    uint8_t *arr;
  
    BitMap(uint32_t size) {
        _size = size;
        alloc_size = (_size - 1) / 8 + 1; // In bytes
        alloc_size = ((alloc_size - 1) / 8 + 1) * 8;  // 8-byte aligned.
        arr = (uint8_t *)malloc(sizeof(uint8_t) * alloc_size);
    }

    ~BitMap() {
        free(arr);
    }

    bool isEmpty() {
        uint64_t *val;
        for (uint i = 0; i < alloc_size / 8; ++i) {
            val = (uint64_t *)&arr[i * 8];
            if (*val != 0 && *val != (0x1ull << 63))
                return false;
        }
        return true;
    }

    bool isSet(uint off) {
        uint x = off >> 3;
        uint y = (off & 0x7u);
        return (arr[x] & (0x1u << y)) != 0;
    }

    BitMap& operator=(BitMap &src) {
        memcpy((void *)arr, (void *)src.arr, src.alloc_size);
        _size = src._size;
        alloc_size = src.alloc_size;
        return *this;
    }

    void Set(uint off) { // Atomically
        // uint8_t _old, _new;
        uint x, y;
        x = off >> 3;
        y = off & 0x7u;
    // _start:
        // _old = _new = arr[x];
        // _new |= (0x1u << y);
        // if (!__sync_bool_compare_and_swap(&arr[x], _old, _new)) {
        __sync_fetch_and_add(&arr[x], (0x1u << y));
        //     asm volatile ("lfence" ::: "memory");
        //     goto _start;
        // }
    }

    void Unset(uint off) { // Atomically
        // uint8_t _old, _new;
        uint x, y;
        x = off >> 3;
        y = off & 0x7u;
    // _start:
        // _old = _new = arr[x];
        // _new &= ~(0x1u << y);
        // if (!__sync_bool_compare_and_swap(&arr[x], _old, _new)) {
        __sync_fetch_and_add(&arr[x], -(0x1u << y));
        //     asm volatile ("lfence" ::: "memory");
        //     goto _start;
        // }
    }
};

#endif

struct DirectLockItem {
    uint8_t _lt[8]; // 64-bit
    DirectLockItem() {
        uint64_t *lt = (uint64_t *)_lt;
        *lt = 0;
    };

    bool lock(uint8_t thd_id, lock_t lt) {
        int off = (lt == LOCK_SH) ? 0 : 4;
        for (int i = off; i < (off + 4); ++i) {
            uint8_t old = _lt[i];
            if (old == 0) {
                // reserve this lock.
                if (__sync_bool_compare_and_swap(&_lt[i], old, thd_id))
                    return true;
            }
            // Check more lock states.
        }
        return false;
    }

    bool unlock(uint8_t thd_id, lock_t lt) {
        int off = (lt == LOCK_SH) ? 0 : 4;
        for (int i = off; i < (off + 4); ++i) {
            if (_lt[i] == thd_id) {
                __sync_bool_compare_and_swap(&_lt[i], thd_id, 0);
                return true;
            }
        }
        return false;
    }
};

#define BUFFER_LEN 16  // How many buffer slots each thread owns.
#define RING_SIZE 128 // Must be a factor of 0x8000

#ifdef __GNUC__
#define likely(x)       __builtin_expect(!!(x), 1)
#define unlikely(x)     __builtin_expect(!!(x), 0)
#else
#define likely(x)       (x)
#define unlikely(x)     (x)
#endif

struct SlotItem {
    uint8_t tid;
    uint64_t ts;
} __attribute__((packed));

struct RingBuffer {
    uint64_t handler; //  point to the current row.
    SlotItem e[RING_SIZE];
    RingBuffer() { handler = 0; }
} __attribute__((aligned(64)));

extern RingBuffer la[][BUFFER_LEN]; // Per-thread locking table.

struct LockItem {
    union {
        struct {
            uint16_t off; // Offset in the table, default -1.
            uint16_t ID;  // Thread ID, default -1.
            uint16_t head;
            uint16_t tail;
        };
        uint64_t _slot;
    };

    LockItem() {
        _slot = 0;
        off = ID = (uint16_t)-1;
    }

    // tid: thread ID; ts: timpstamp; hdl: current row.
    uint16_t lock(uint8_t tid, uint64_t ts, uint64_t hdl) {
        // Reserve a lock item.
        uint64_t temp;
        LockItem *me = new (&temp) LockItem();
        uint16_t fetched;

    _start:
        // Read a snapshot.
        temp = _slot;
        if (me->off == (uint16_t)-1 && me->ID == (uint16_t)-1) {
            // Allocate a lock item from local lock table.
            // Two alternative slots.
            uint64_t t = (uint64_t)hdl; // FIXME: should be hdl->primary_key;
            // First check the handler, then see the row context.
            uint16_t new_off = CityHash32((char *)&t, 4) % BUFFER_LEN;
            auto e1 = &la[tid][new_off];
            bool find_slot = false;

            // see if e1 is available.
            if (!find_slot && e1->handler == 0) {
                e1->handler = hdl;
                find_slot = true;
            } else {
                // see if e2 is available.
                t += 0x10000000; // FIXME: should be hdl->primary_key;
                new_off = CityHash32((char *)&t, 4) % BUFFER_LEN;
                auto e2 = &la[tid][new_off];
                if (e2->handler == 0) {
                    e2->handler = hdl;
                    find_slot = true;
                } else {
                    // see if e1 and e2 are potentially available.
                    // TBD.
                }
            }

            if (find_slot) {
                // Safely use it. No others will actively grabe it.
                asm volatile ("sfence" ::: "memory");
                LockItem item;
                item.ID = tid;
                item.off = new_off;
                item.head = item.tail = 0;
                if (!__sync_bool_compare_and_swap(&_slot, temp, item._slot)) {
                    // Someone may already assign it a lock.
                    goto _start;
                }
            }

            // Cannot find a available slot, abort the current TX.
            if (!find_slot) {
                return -1;
            }
        }

        // Test first. If has bug, turn to use CAS instead.
        fetched = __sync_fetch_and_add(&head, 1);

        temp = _slot;  // Read again.
        if (unlikely(me->off == (uint16_t)-1 && me->ID == (uint16_t)-1)) {
            // The lock item has been relaimed.
            goto _start;
        }

        // Avoid wraparound.
        if (fetched == 0x4000) {
            // This logic is dangerous: what if this thread is delayed somehow?
            __sync_fetch_and_add(&head, -0x4000);
        }

        // The owner is in exclusive mode, wait instead.
        if (unlikely(fetched >= 0x8000)) {
            while (head >= 0x8000) {
                asm volatile ("lfence" ::: "memory");
            }
            // we don't need to add this line: fetched -= 0x8000.
        }

        la[ID][off].e[fetched % RING_SIZE].tid = tid;
        la[ID][off].e[fetched % RING_SIZE].ts = ts;
        return fetched;
    }

    bool unlock(uint16_t tid) {
        // Release my self.
        // Things we don't need to worry about: 
            // 1) off, ID are valid since the lock is not empty.
            // 2) Tail is valid since the owner doesn't modify it.
        // Set my slot to zero.
        int loc;
        for (loc = tail; loc < head; ++loc)
            if (la[ID][off].e[loc % RING_SIZE].tid == tid) {
                la[ID][off].e[loc % RING_SIZE].tid = 0;
                la[ID][off].e[loc % RING_SIZE].ts = 0;
            }

        if (loc == head) { // Cannot find myself. Possible.
            return false;
        }

        asm volatile ("sfence" ::: "memory");

        uint64_t temp; // Read a snapshot.
        LockItem *me = (LockItem *)&temp;
        temp = _slot;
        uint16_t new_tail;
        bool change = false;

        // Update the tail.
        if ((me->tail == loc)) {
            change = true;
            new_tail = ((me->tail + 1) >= 0x4000) ? (me->tail + 1 - 0x4000) : (me->tail + 1);
        } else if (la[ID][off].e[me->tail % RING_SIZE].tid == 0 && la[ID][off].e[me->tail % RING_SIZE].ts == 0) {
            int temp = me->tail;
            while (la[ID][off].e[temp % RING_SIZE].tid == 0 && la[ID][off].e[temp % RING_SIZE].ts == 0 )
                temp += 1;
            new_tail = (temp >= 0x4000) ? (temp - 0x4000) : temp;
            change = true;
        }
        if (change) {
            // No need to guarantee success once. Others can help.
            __sync_bool_compare_and_swap(&tail, me->tail, new_tail);
        }
        // Note: unlock doesn't need to return back the lock item. Lazy release.
        return true;
    }

    void exMode() {
        __sync_fetch_and_add(&head, 0x8000);
    }

    void unExMode() {
        __sync_fetch_and_add(&head, -0x8000);
    }
};



/*
struct LockBuffer __attribute__((aligned(64))) {
    uint64_t items[THREAD_CNT];
    bool lock(int off, uint64_t lt) {
        // For sure, no overlapping.
        items[off % THREAD_CNT] = lt;
    }
    bool unlock() {}
};
*/

inline uint countSetBits(int n) {
	uint count = 0; 
	while (n) { 
		n &= (n - 1); 
		count ++;
	}
	return count;
}

inline bool is_mark_set(uint64_t addr) {
	return (addr & 0x1ull) == 1;
}

inline void set_mark(uint64_t *addr) {
	*addr |= 0x1ull;
}

inline void unset_mark(uint64_t *addr) {
	if (is_mark_set(*addr))
		*addr -= 0x1ull;
}

#if CC_ALG == OLOCK

class Row_olock {
public:
    void init(row_t *row);
    RC lock_get(lock_t type, txn_man *txn);
    RC lock_release(lock_t type, txn_man *txn);
    void poll_lock_state(txn_man *txn);

private:
    // ...
    row_t *_row;
    Owner owner; // Only the owner can remove itself.
    OLockEntry *waiters;
    uint8_t wound;
    txn_man *last_unset;
    void insert(OLockEntry *);
    void remove(txn_man *);
    txn_man* find_oldest();
    // boost::lockfree::queue<uint64_t, boost::lockfree::capacity<400>> queue;
};

#elif CC_ALG == DLOCK

class Row_dlock {
public:
    void init(row_t *row);
    RC lock_get(lock_t type, txn_man *txn);
    RC lock_release(lock_t type, txn_man *txn);
    void poll_lock_state(txn_man *txn);

    RC validate(txn_man *txn);
    // int path = 0;

    // std::mutex mtx;
    // bool do_print;

    int cnt = 0;

private:
    // ...
    row_t *_row;
    Owner owner; // Only the owner can remove itself.

    // Solution for cores less than 64 (using a 64-bit bitmap).
    BitMap *bmpWr;
    BitMap *bmpRd;
    uint8_t s_lock;
    uint64_t readers;

    std::mutex mtx; // Used in legacy mode (w/o the lock-free optimizations)

    // Larger than 64 cores.
    DirectLockItem dirLock; // Including both read/write lock slots.
    LockItem readLock;  // 64-bit, managing a ringbuffer for readers.
    LockItem writeLock; // 64-bit, managing a ringbuffer for  writers.

    uint8_t _pad[64];
    txn_man* find_oldest();
    RC lock_get_sh(lock_t type, txn_man *txn);
    RC lock_get_ex(lock_t type, txn_man *txn);
    RC lock_release_sh(lock_t type, txn_man *txn);
    RC lock_release_ex(lock_t type, txn_man *txn);

    void mtx_get() {
    #if DLOCK_LOCKFREE == 0
        mtx.lock();
    #endif
    }
    
    void mtx_release() {
    #if DLOCK_LOCKFREE == 0
        mtx.unlock();
    #endif
    }
};
#endif

#endif
