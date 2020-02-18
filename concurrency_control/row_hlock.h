#pragma once 

class table_t;
class Catalog;
class txn_man;

#if CC_ALG==HLOCK
#define LOCK_BIT (1UL << 63)

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

struct WrLockItem {
	union {
		struct {
			uint64_t _ts   : 55;
			uint64_t wound : 1;
			uint64_t _tid  : 8;
		};
		uint64_t l_wr = 0;
	};
};

class Row_hlock {
public:
	void 				init(row_t * row);
	RC 					access(txn_man * txn, TsType type, row_t * local_row);
	
	void				write(row_t * data, uint64_t tid);
	
	int 				lock_wr(txn_man *txn);
	void 				unlock_wr(txn_man *txn);
	void 				unlock_wr_(txn_man *txn);
	bool				try_lock_wr();

	bool				lock_rd(txn_man *txn);
	void				unlock_rd(txn_man *txn);
	bool				is_locked_rd(txn_man *txn);
	uint64_t 			get_tid();
	void 				assert_lock() {/*assert(_tid_word & LOCK_BIT); */}
private:
	// volatile uint64_t	_tid_word;
	WrLockItem *lockWr;
	BitMap				*bmpRd;
	row_t * 			_row;
	uint64_t owner_ts;
};

#endif
