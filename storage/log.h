#ifndef _LOG_H_
#define _LOG_H_

#include <stdint.h>

#define PER_LOG_SIZE (1024 * 1024 * 10) // 10 MB.

#ifdef __GNUC__
#define likely(x)       __builtin_expect(!!(x), 1)
#define unlikely(x)     __builtin_expect(!!(x), 0)
#else
#define likely(x)       (x)
#define unlikely(x)     (x)
#endif

struct LogMeta {
    uint64_t tx_id;
    uint64_t wr_cnt;
};

struct LogRecord {
    uint64_t primary_key;
    uint64_t record_size;
};

inline void clwb(void *des, uint64_t size) {
    size += (uint64_t)des & (64ULL - 1);
    uint64_t addr = (uint64_t)des >> 6 << 6;
    for (uint64_t i = 0; i < size; i += 64) {
        asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(addr + i)));
    };
}

class persistent_log {
public:
    void init(int thd_id);
    void log_tx_meta(uint64_t tx_id, uint64_t wr_cnt);
    void log_content(uint64_t primary_key, void *src, uint64_t size);
    void log_end();
private:
    void *log_buf;
    void *log;  // Available log space.
    void *v_tail; // a volatile pointer, incremented upon appending.
    void **p_tail; // located at the head of the log.
    void* append_data(void *src, uint64_t size);
    void persist_data(void *src, uint64_t size);
};

#endif