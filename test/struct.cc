#include <stdint.h>
#include <stdio.h>
#include <city.h>
#include <sys/time.h>
#include <time.h>


struct Item {
    uint16_t tid;
    uint64_t ts;
} __attribute__((packed));

struct Array {
    Item it[24];
} __attribute__((aligned(64)));

int main() {
    printf("%d\t%d\n", sizeof(Item), sizeof(Array));
    char buf[4];
    struct timespec T1, T2;
    double diff;
    clock_gettime(CLOCK_MONOTONIC, &T1);
    for (int i = 0; i < 1000000; ++i) {
        buf[0] = 'A' + (i % 36);
        CityHash32(buf, 4);
    }
    clock_gettime(CLOCK_MONOTONIC, &T2);
    diff = (double)(T2.tv_sec - T1.tv_sec) + (double)(T2.tv_nsec - T1.tv_nsec) / 1000000000;
    printf("%.2f\n", (double)1000000 / diff);
    return 0;
}