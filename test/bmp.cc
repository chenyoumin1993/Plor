#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <thread>

#define MAX 64

struct BitMap {
    uint32_t _size;
    uint8_t *arr;
  
    BitMap(uint32_t size) {
        _size = size;
        arr = (uint8_t *)malloc(sizeof(uint8_t) * ((_size - 1) / 8 + 1));
    }

    ~BitMap() {
        free(arr);
    }

    bool isSet(int off) {
        return (arr[off / 8] & (0x1u << (off % 8))) != 0;
    }

    void Set(int off) { // Atomically
        uint8_t _old, _new;
    _start:
        _old = _new = arr[off / 8];
        _new |= (0x1u << (off % 8));
        if (!__sync_bool_compare_and_swap(&arr[off / 8], _old, _new)) {
            asm volatile ("lfence" ::: "memory");
            goto _start;
        }
    }

    void Unset(int off) { // Atomically
                uint8_t _old, _new;
    _start:
        _old = _new = arr[off / 8];
        _new &= ~(0x1u << (off % 8));
        if (!__sync_bool_compare_and_swap(&arr[off / 8], _old, _new)) {
            asm volatile ("lfence" ::: "memory");
            goto _start;
        }
    }
};

BitMap *bmp;
int n_thd = 0;

void worker (int id) {
    int idx;
    for (int i = 0; i < 1000; ++i) 
        for (int j = 0; j < MAX; ++j) {
            idx = n_thd * j + id;
            bmp->Set(idx);
            if (!bmp->isSet(idx)) printf("Err.\n");
            bmp->Unset(idx);
            if (bmp->isSet(idx)) printf("Err.\n");
        }
}

int main(int argc, char **argv) {
    if (argc > 1) n_thd = atoi(argv[1]);

    bmp = new BitMap(MAX * n_thd);

    std::thread *t = new std::thread[n_thd];

    for (int i = 0; i < n_thd; ++i)
        t[i] = std::thread(worker, i);

    for (int i = 0; i < n_thd; ++i)
        t[i].join();

    uint x, y;
    uint c = 15;
    x = (c >> 3);
    y = c & 0x7u;
    printf("x = %d, y = %d\n", x, y);
    return 0;
}
