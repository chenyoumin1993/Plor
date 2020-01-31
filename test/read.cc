#include <stdint.h>
#include <stdio.h>

struct ReadLockEntry {
    union {
        uint32_t L;
        uint8_t _L[4];
    };

    ReadLockEntry() { L = 0; }

    bool reserve(uint8_t id) {
        for (int i = 0; i < 4; ++i) {
            if (_L[i] == 0) {
                if (__sync_bool_compare_and_swap(&_L[i], 0 , id)) {
                    return true;
                }
            }
        }
        return false;
    }

    void release(uint8_t id) {
        for (int i = 0; i < 4; ++i) {
            if (_L[i] == id) {
                __sync_bool_compare_and_swap(&_L[i], id, 0);
            }
        }
    }
};

int main() {
    printf("%lld\n", sizeof(ReadLockEntry));
    return 0;
}