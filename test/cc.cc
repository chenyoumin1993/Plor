#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/time.h>
#include <stdio.h>
#include <random>
#include <thread>
#include <atomic>

uint64_t cnt[64] = {0};

bool start1 = false;
std::atomic<int> c(0);

void add(int id) {
	while (!start1) ;
	for (int i = 0; i < 10000000; ++i)
		cnt[id] = i;
	c += 1;
}

int main(int argc, char **argv) {
	int n_thd = 1;
	if (argc > 1) n_thd = atoi(argv[1]);
	std::thread *t = new std::thread[n_thd];

	struct timespec T1, T2;
	double diff;

	for (int i = 0; i < n_thd; ++i)
		t[i] = std::thread(add, i);
	
	start1 = true;
	clock_gettime(CLOCK_MONOTONIC, &T1);
	while (c != n_thd) ;
	clock_gettime(CLOCK_MONOTONIC, &T2);
	for (int i = 0; i < n_thd; ++i)
		t[i].join();
	diff = (double)(T2.tv_sec - T1.tv_sec) * 1000000 + (T2.tv_nsec - T1.tv_nsec) / 1000;
	printf("%.2f\n", (double)(100000 * n_thd) / (diff / 10000000));
	for (int i = 0; i < 64; ++i)
		printf("%.0f\t", (double)cnt[i]);
	printf("\n");
	return 0;
}
