#include <boost/thread/thread.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/atomic.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <mutex>
#include <queue>

#include <stdio.h>
#include <sys/time.h>

#define MAX 1000000

bool start1 = false, start2 = false;
boost::atomic_int cnt1(0), cnt2(0);
// std::queue<int> q;
boost::lockfree::queue<int> q(1);
// boost::lockfree::spsc_queue<int> q(1);

std::mutex mtx;
uint64_t cnt;

void producer(int id) {
    while (!start1) ;
    for (int i = 0; i < MAX; ++i) {
	// mtx.lock();
        q.push(id * MAX + i);
	// mtx.unlock();
    }
    
    cnt1 += 1;
    int ret;
    while (!start2) ;
    for (int i = 0; i < MAX; ++i) {
	// mtx.lock();
        // q.pop();
        q.pop(ret);
	// mtx.unlock();
    }
    cnt2 += 1;
}

int main(int argc, char **argv) {
    int n_thd = 1;
    struct timespec T1, T2, T3;
    double diff1, diff2;

    if (argc > 1)
        n_thd = atoi(argv[1]);
    
    boost::thread_group producers;

    for (int i = 0; i < n_thd; ++i)
        producers.create_thread(boost::bind(producer, i));

    clock_gettime(CLOCK_MONOTONIC, &T1);
    start1 = true;
    while (cnt1 != n_thd) ;
    clock_gettime(CLOCK_MONOTONIC, &T2);
    start2 = true;
    while (cnt2 != n_thd) ;
    clock_gettime(CLOCK_MONOTONIC, &T3);
    diff1 = (double)(T2.tv_sec - T1.tv_sec) * 1000000  + (double)(T2.tv_nsec - T1.tv_nsec) / 1000;
    diff2 = (double)(T3.tv_sec - T2.tv_sec) * 1000000  + (double)(T3.tv_nsec - T2.tv_nsec) / 1000;
    printf("%.4f\t%.4f\n", (double)(MAX * n_thd) / diff1, (double)(MAX * n_thd) / diff2);
    producers.join_all();
}
