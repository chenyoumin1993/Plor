#include <stdint.h>
#include <stdio.h>
#include <thread>

#define RING_SIZE 128 // Must be a factor of 0x80000000

struct RingBuffer {
    uint64_t items[RING_SIZE];
};

RingBuffer la[1][1]; // Per-thread locking table.

struct LockItem {
    union {
        struct {
            uint16_t off; // Offset in the table.
            uint16_t ID;  // Thread ID.
            uint16_t head;
            uint16_t tail;
        };
        uint64_t item;
    };

    LockItem() {
        item = 0;
    }
    uint16_t lock(uint64_t lt) {
        // Reserve a lock item.
        uint64_t temp;
        LockItem *me = (LockItem *)&temp;
        uint16_t fetched;

    _start:
        // Read a snapshot.
        temp = item;

        if (me->off == -1 && me->ID == -1) {
            // Allocate a lock item from local lock table.
            // Be careful of the logic.
        }

        fetched = __sync_fetch_and_add(&head, 1);

        temp = item;  // Read again.
        if (unlikely(me->off == -1 && me->ID == -1)) {
            // The lock item has been released.
            goto _start;
        }

        // Avoid wrap around.
        if (fetched == 0x80000000) { // 80 00 00 00
            __sync_fetch_and_add(&head, -0x80000000);
        }

        // The owner is in exclusive mode, wait instead.
        if (unlikely(fetched > 0xF0000000)) {
            while (head > 0xF0000000) {
                asm volatile ("lfence" ::: "memory");
            }
            fetched -= 0xF0000000;
        }

        la[ID][off].items[fetched % RING_SIZE] = lt;
        return fetched;
    }

    bool unlock(uint64_t lt, uint16_t loc) {
        // Release my self. 
        // For sure: 1) off, ID are valid since the lock is not empty.
        //           2) Tail is valid since the owner doesn't modify it.
        la[off][ID].items[loc % RING_SIZE] = 0;
        asm volatile ("sfence" ::: "memory");

        uint64_t temp; // Read a snapshot.
        LockItem *me = (LockItem *)&temp;
        temp = item;
        uint16_t new_tail;
        bool change = false;

        // Update the tail.
        if ((me->tail == loc)) {
            change = true;
            new_tail = ((me->tail + 1) >= 0x80000000) ? (me->tail + 1 - 0x80000000) : (me->tail + 1);
            __sync_bool_compare_and_swap(&head, me->tail, me->tail + 1);
        } else if (la[ID][off].items[me->tail % RING_SIZE] == 0) {
            int temp = me->tail;
            while (la[ID][off].items[temp % RING_SIZE] == 0)
                temp += 1;
            new_tail = (temp >= 0x80000000) ? (temp - 0x80000000) : temp;
            change = true;
        }
        if (change) {
            // No need to guarantee success once. Others can help.
            __sync_bool_compare_and_swap(&tail, me->tail, new_tail);
        }
        // Note: unlock doesn't need to return back the lock item. Lazy release.
    }
};