#include <stdio.h>
#include <stdint.h>
#include <sys/time.h>
#include <thread>

uint8_t start = 0;
uint8_t stop = 0;

uint8_t epoch = 0;
uint8_t th_num = 1;

using namespace std;

void test()
{
	while ((__sync_fetch_and_add(&epoch, 0)) == 0) ;
	__sync_fetch_and_add(&start, 1);
	while ((__sync_fetch_and_add(&start, 0)) != th_num);

	__sync_fetch_and_add(&stop, 1);
	while ((__sync_fetch_and_add(&stop, 0)) != th_num);
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
	clock_gettime(CLOCK_MONOTONIC, &T1);
	epoch = 1;
	while ((__sync_fetch_and_add(&stop, 0)) != th_num);
	clock_gettime(CLOCK_MONOTONIC, &T2);
	for (uint i = 0; i < th_num; ++i)
		t[i].join();
	double diff;
	diff = (double)(T2.tv_sec - T1.tv_sec) * 1000000000 + (T2.tv_nsec - T1.tv_nsec);
	// printf("%.2f\n", ((double)T2.tv_sec * 1000000000 + T2.tv_nsec) - 
	//	((double)T1.tv_sec * 1000000000 + T1.tv_nsec));
	printf("%.2f\n", diff);
	return 0;
}
