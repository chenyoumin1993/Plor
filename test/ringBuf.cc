#include <stdint.h>
#include <stdio.h>
#include <thread>
#include <city.h>

#define RING_SIZE 128 // Must be a factor of 0x8000
#define THREAD_CNT 10 // The total number of threads.
#define BUFFER_LEN 16  // How many buffer slots each thread owns.

#ifdef __GNUC__
#define likely(x)       __builtin_expect(!!(x), 1)
#define unlikely(x)     __builtin_expect(!!(x), 0)
#else
#define likely(x)       (x)
#define unlikely(x)     (x)
#endif

struct Item {
    uint16_t tid;
    uint64_t ts;
} __attribute__((packed));

struct RingBuffer {
    uint64_t handler; //  point to the current row.
    Item e[RING_SIZE];
    RingBuffer() { handler = 0; }
} __attribute__((aligned(64)));

RingBuffer la[THREAD_CNT][BUFFER_LEN]; // Per-thread locking table.

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
    uint16_t lock(uint16_t tid, uint64_t ts, uint64_t hdl) {
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
                printf("------------\n");
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

    bool unlock(uint16_t loc) {
        // Release my self.
        // Things we don't need to worry about: 
            // 1) off, ID are valid since the lock is not empty.
            // 2) Tail is valid since the owner doesn't modify it.
        // Set my slot to zero.
        la[ID][off].e[loc % RING_SIZE].tid = 0;
        la[ID][off].e[loc % RING_SIZE].ts = 0;
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
    }

    void exMode() {
        __sync_fetch_and_add(&head, 0x8000);
    }

    void unExMode() {
        __sync_fetch_and_add(&head, -0x8000);
    }
};

int main() {
    LockItem item;
    uint16_t loc;

    for (int i = 0; i < 30; ++i) {
        loc = item.lock(0, 234242, 0x23423984);
        item.unlock(loc);
        printf("loc = %d\n", loc);
    }
    return 0;
}