#ifndef ROW_OLOCK_H
#define ROW_OLOCK_H

/*********************************
 Two types of implementation:
 1) maintain a lockfree list.
 2) using a distributed lock.
 ********************************/
#include <boost/lockfree/queue.hpp>

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
        txn_man *_owner;
        struct {
        	uint8_t cnt;
        	union {
                uint8_t _cnt;
                struct {
                    uint8_t cnt_bak : 7;
                    uint8_t wound : 1;
                };
            };
            uint64_t owner : 48;
        };
        struct {
            uint64_t _pad : 16;
            uint64_t ex_mode : 32;
            uint16_t thd_id : 16;
        };
    };
};

#endif

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
            if (*val != 0)
                return false;
        }
        return true;
    }

    bool isSet(uint off) {
        uint x = off >> 3;
        uint y = (off & 0x7u);
        return (arr[x] & (0x1u << y)) != 0;
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

private:
    // ...
    row_t *_row;
    Owner owner; // Only the owner can remove itself.
    BitMap *bmpWr;
    BitMap *bmpRd;
    uint8_t s_lock;
    uint64_t readers;
    txn_man* find_oldest();
    RC lock_get_sh(lock_t type, txn_man *txn);
    RC lock_get_ex(lock_t type, txn_man *txn);
    RC lock_release_sh(lock_t type, txn_man *txn);
    RC lock_release_ex(lock_t type, txn_man *txn);
};
#endif

#endif
