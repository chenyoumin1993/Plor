#include <stdint.h>
#include <stdio.h>

int a = 1;

struct __attribute__((packed)) Owner {
    union {
        uint64_t _owner;
        struct {
        	uint8_t cnt;
        	union {
                uint8_t _cnt;
                struct {
                    uint8_t cnt_bak : 7;
                    uint8_t wound : 1;
                };
            };
            uint64_t owner : 48;
        };
        struct {
            uint64_t _pad : 16;
            uint64_t ex_mode : 32;
            uint16_t thd_id : 16;
        };
    };
};

int main() {
    printf("%d\n", sizeof(Owner));
}
