#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

#include<boost/bind.hpp>
#include<boost/coroutine/all.hpp>

using namespace boost::coroutines;

struct timespec T1, T2;
double diff;
#define MAX 1000000

// Coroutines
typedef symmetric_coroutine<void>::call_type coro_call_t;
typedef symmetric_coroutine<void>::yield_type coro_yield_t;
typedef int coro_id_t;

coro_call_t *coro_arr;
int num_coro = 12;
int *next_coro;

uint64_t cnt = 0;

void slave_func(coro_yield_t &yield, int coro_id) {
_start:
	if (cnt == 0) {
		clock_gettime(CLOCK_MONOTONIC, &T1);
	} else if (cnt >= MAX) {
		clock_gettime(CLOCK_MONOTONIC, &T2);
		diff = (double)(T2.tv_sec - T1.tv_sec) + (double)(T2.tv_nsec - T1.tv_nsec) / 1000000000;
		double rate = MAX / diff;
		printf("%.2f\n", rate);
		return;
	}
	cnt += 1;
	// usleep(200000);
	// printf("yield to %d\n", next_coro[coro_id]);
	// yield(coro_arr[next_coro[coro_id]]);
	goto _start;
}

int main() {
	coro_arr = new coro_call_t[num_coro];
	next_coro = new int[num_coro];

	for (int i = 0; i < num_coro - 1; ++i) {
		next_coro[i] = i + 1;
		coro_arr[i] = coro_call_t(bind(slave_func, _1, i), attributes(fpu_not_preserved));
	}
	coro_arr[num_coro - 1] = coro_call_t(bind(slave_func, _1, num_coro - 1), attributes(fpu_not_preserved));
	next_coro[num_coro - 1] = 0;

	coro_arr[0]();

	return 0;
}
