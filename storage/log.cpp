#include "log.h"
#include "config.h"

#include <numa.h>
#include <fcntl.h>
#include <time.h>
#include <stdint.h>
#include <sys/mman.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <sys/time.h>
#include <stdio.h>
#include <sched.h>
#include <assert.h>

void persistent_log::init(int thd_id) {
#if PERSISTENT_LOG == 1
    // current numa node count.
    int node = numa_node_of_cpu(sched_getcpu());
    char path[100];
    sprintf(path, "/dev/dax%d.0", node);
    int fd = open(path, O_RDWR);
    if (fd <= 0) {
        printf("Device %s not found.\n", path);
        assert(false);
    }
    log_buf = mmap(NULL, PER_LOG_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, PER_LOG_SIZE * thd_id);
    p_tail = (void **)log_buf;
    log = (uint64_t *)log_buf + 1;
    v_tail = log;
    *p_tail = log;
    persist_data(p_tail, sizeof(uint64_t));
#endif
}

void persistent_log::log_tx_meta(uint64_t tx_id, uint64_t wr_cnt) {
#if PERSISTENT_LOG == 1
    struct LogMeta meta = {tx_id, wr_cnt};
    void *log_before = append_data(&meta, sizeof(LogMeta));
    persist_data(log_before, sizeof(LogMeta));
#endif
}

void persistent_log::log_content(uint64_t primary_key, void *src, uint64_t size) {
#if PERSISTENT_LOG == 1
    struct LogRecord r = {primary_key, size};
    void *log_before = append_data(&r, sizeof(LogMeta));
    append_data(src, size);
    persist_data(log_before, sizeof(LogRecord) + size);
#endif
}

void persistent_log::log_end() {
#if PERSISTENT_LOG == 1
    // fence.
    asm volatile ("sfence" ::: "memory");
    *p_tail = v_tail;
    persist_data(p_tail, sizeof(uint64_t));
#endif
}

void* persistent_log::append_data(void *src, uint64_t size) {
    // Always update the volatile tail pointer.
    void *old_tail = v_tail;
    
    assert(size < PER_LOG_SIZE);
    assert(((uint64_t)log_buf + PER_LOG_SIZE) > (uint64_t)v_tail);

    if ((PER_LOG_SIZE - ((uint64_t)v_tail - (uint64_t)log_buf)) < size)
        v_tail = log;

    memcpy(v_tail, src, size);

    v_tail = (void *)((uint64_t)v_tail + size);

    return old_tail;
}

void persistent_log::persist_data(void *src, uint64_t size) {
    if (unlikely(v_tail < src)) {
        // wrap around.
        clwb(src, ((uint64_t)log_buf + PER_LOG_SIZE) - (uint64_t)src);
        clwb(log, ((uint64_t)v_tail - (uint64_t)log));
    } else {
        clwb(src, size);
    }
}