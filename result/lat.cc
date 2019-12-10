#include <stdio.h>
#include <stdint.h>
#include <sys/time.h>

uint8_t start = 0;
uint8_t stop = 0;

uint8_t epoch = 0;

uint8_t th_num = 0;

using namespace std;

void test()
{
	while (epoch == 0) ;
	__sync_fetch_and_add(&start, 1);
	while (start != th_num);

	__sync_fetch_and_add(&stop, 1);
	while (stop != th_num);
	return;
}
int main(int argc, char **argv)
{
	thread t[64];
	struct timespec T1, T2;
	if (argc > 1)
		th_num = atoi(argv[1]);
	for (uint i = 0; i < th_num; ++i)
		t[i] = thread(test);
	clock_gettime(T1, CLOCK_MONOTONIC);
	epoch = 1;
	while (stop != th_num);
	clock_gettime(T2, CLOCK_MONOTONIC);
	for (uint i = 0; i < th_num; ++i)
		t[i].join();
	printf("%.2f\n", (T2.tv_sec * 1000000000 + T2.tv_nsec) - 
		(T1.tv_sec * 1000000000 + T1.tv_nsec));
	return 0;
}
