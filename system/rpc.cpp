#if 0

#ifndef _RPC_H_
#define _RPC_H_

#include <stdint.h>
#include <config.h>
#include <thread>
#include <assert.h>

/**
 *   +-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+
 *   | Buf 1 | Buf 2 | Buf 3 | Buf 4 | Buf 5 | Buf 6 | Buf 7 | Buf 8 | Buf 9 | Buf 0 |  ...  | Buf N |
 *   +-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+
 *   |<------- Core 1 ------>|<------- Core 2 ------>|<------- Core 3 ------>|<------- Core 4 ------>|
**/



#define REQ_STATE 1
#define RES_STATE 2

struct Message {
    uint64_t src;
    uint64_t des;
    uint64_t size;
    uint8_t flag;
} __attribute__((aligned(64)));

class Rpc {
public:
    Rpc() {
        if (THREAD_CNT >= STORAGE_WORKER_CNT) {
            assert(THREAD_CNT % STORAGE_WORKER_CNT == 0);
        }
    }

    ~Rpc() {
        for (int i = 0; i < thd_num; ++i) 
            wk[i].join();
    }

    bool rpcCopy(int core_id, void *des, void *src, uint64_t size);

    void start() {
        for (int i = 0; i < thd_num; ++i) 
            wk[i] = std::thread(&Rpc::worker, i);
    }

private:
    std::thread wk[STORAGE_WORKER_CNT]; // always larger than thd_num.
    static int thd_num;
    static void worker(int id);
};

#endif

#include "rpc.h"
#include "wl.h"
#include "helper.h"

extern workload *m_wl;

static Message slots[THREAD_CNT];

int Rpc::thd_num = (STORAGE_WORKER_CNT > THREAD_CNT) ? THREAD_CNT : STORAGE_WORKER_CNT;

// Post a rpc
bool Rpc::rpcCopy(int core_id, void *des, void *src, uint64_t size) {
    slots[core_id].des = (uint64_t)des;
    slots[core_id].src = (uint64_t)src;
    slots[core_id].size = (uint64_t)size;
    asm volatile ("sfence" ::: "memory");
    slots[core_id].flag = REQ_STATE;

    while (slots[core_id].flag != RES_STATE && !m_wl->sim_done) {
        asm volatile ("lfence" ::: "memory");
        __asm__ ( "pause;" );
    }
    return true;
}

void Rpc::worker(int id) {
    // printf("id = %d\n", id);
    // set_affinity(1);
    int scan_num = THREAD_CNT / thd_num;
    uint64_t cnt = 0;
    // printf("start = %d, end = %d\n", id * scan_num, (id + 1) * scan_num);
    while (!m_wl->sim_done) {
        for (int i = (id * scan_num); i < ((id + 1) * scan_num); ++i) {
            if (slots[i].flag == REQ_STATE) {
                memcpy((void *)slots[i].des, (void *)slots[i].src, slots[i].size);
                asm volatile ("sfence" ::: "memory");
                slots[i].flag = RES_STATE;
                cnt += 1;
            }
        }
        asm volatile ("lfence" ::: "memory");
    }
    // printf("cnt = %lld\n", (long long)cnt);
}

#endif