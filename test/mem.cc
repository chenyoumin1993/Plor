#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/time.h>
#include <stdio.h>
#include <random>

#define MAX (1024ULL * 1024 * 1024)
uint32_t pos[1000000];

int main() {
	uint64_t *p = (uint64_t *)malloc(MAX);
	memset((void *)p, 0, MAX);
	std::random_device rd;
	std::mt19937 mt(rd());
	std::uniform_real_distribution<double> dist(0, 1);
	for (int i = 0; i < 1000000; ++i)
		pos[i] = (uint32_t)(dist(mt) * (MAX / 64 / 2));
	struct timespec T1, T2;
	double diff;
	uint64_t cnt = 0, *cur;

	clock_gettime(CLOCK_MONOTONIC, &T1);
	for (int i = 0; i < 1000000; ++i) {
		for (int j = 0; j < 1024; ++j) {
			cur += *((uint64_t *)((uint8_t *)p + 64 * (pos[i] + j)));
		}
	}
	clock_gettime(CLOCK_MONOTONIC, &T2);
	diff = (double)(T2.tv_sec - T1.tv_sec) * 1000000 + (T2.tv_nsec - T1.tv_nsec) / 1000;
	printf("%.2f\n", diff / 1000000);
	return 0;
}
