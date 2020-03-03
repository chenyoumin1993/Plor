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