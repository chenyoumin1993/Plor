#include <stdint.h>
#include <thread>
#include <stdio.h>

struct Line {
    uint8_t cnt;
    uint8_t _pad[63];
};

Line arr[64];

#define MAX 1000000

int worker(int id) {
    for (int i = 0; i < MAX; ++i)
        __sync_fetch_and_add(&arr[id].cnt, 1);
}

int main(int argc, char **argv) {
    int n_thd = 1;
    if (argc > 1) n_thd = atoi(argv[1]);

    std::thread *t = new std::thread[n_thd];
    struct timespec T1, T2;
    double diff;
    
    clock_gettime(CLOCK_MONOTONIC, &T1);
    for (int i = 0; i < n_thd; ++i) {
        t[i] = std::thread(worker, i);
    }
    for (int i = 0; i < n_thd; ++i) {
        t[i].join();
    }
    clock_gettime(CLOCK_MONOTONIC, &T2);
    diff = (T2.tv_sec - T1.tv_sec) + (double)(T2.tv_nsec - T1.tv_nsec) / 1000000000;
    printf("%.2f\n", (double)MAX * n_thd / diff);
    return 0;
}