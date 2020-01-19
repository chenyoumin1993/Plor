#include <stdint.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <thread>
#include <pthread.h>

#define g_synth_table_size 100

#define SPIN 1
int max_iter = 100000;
volatile bool start = false;
volatile bool stop = false;
uint64_t total_cnt[1048576];

static uint64_t the_n = 0;
static double denom = 0;
double zeta_2_theta;
double g_zipf_theta = 0.9;
__thread struct drand48_data *randBuffer;

struct atomic_lock {
	uint8_t _lock = 0;
	void lock() {
		while (!__sync_bool_compare_and_swap(&_lock, 0, 1))
			__asm__ ( "pause;" );
		// asm volatile ("mfence" ::: "memory");
	}
	void unlock() {
		if (_lock == 0)
			printf("error.");
		_lock = 0;
		// asm volatile ("mfence" ::: "memory");
	}
};

#ifdef MUTEX
pthread_mutex_t *latch[g_synth_table_size];
#elif SPIN
pthread_spinlock_t *latch[g_synth_table_size];
#elif ATMOIC
struct atomic_lock latch[g_synth_table_size];
#endif

static void calculateDenom();

static double zeta(uint64_t n, double theta);
uint64_t zipf(uint64_t n, double theta);
void calculateDenom();
void exec(int cnt);

int main(int argc, char **argv)
{
	if (argc > 1)
		g_zipf_theta = (double)atoi(argv[1]) / 100;
	randBuffer = (struct drand48_data*)malloc(sizeof(struct drand48_data));
	zeta_2_theta = zeta(2, g_zipf_theta);
	calculateDenom();

	int n_thd = 1;
	std::thread *t;

	struct timespec T1, T2;
	double diff, rate;
	
	// init lock
	for (int i = 0; i < g_synth_table_size; ++i) {
	#ifdef MUTEX
		latch[i] = new pthread_mutex_t;
		pthread_mutex_init(latch[i], NULL);
	#elif SPIN
		latch[i] = new pthread_spinlock_t;
		pthread_spin_init(latch[i], 0);
	#endif
	}

	// init threads
	if (argc > 2)
		n_thd = atoi(argv[2]);

	printf("%d\t%.2f\t", n_thd, g_zipf_theta);
	
	t = new std::thread[n_thd];
	for (int i = 0; i < n_thd; ++i)
		t[i] = std::thread(exec, i);
	
	clock_gettime(CLOCK_MONOTONIC, &T1);
	start = true;

	while (!stop);
	clock_gettime(CLOCK_MONOTONIC, &T2);

	for (int i = 0; i < n_thd; ++i)
		t[i].join();

	uint64_t cnt = 0;
	for (int i = 0; i < 100; ++i)
		cnt += total_cnt[i];

	diff = (double)(T2.tv_sec - T1.tv_sec) * 1000000 + (double)(T2.tv_nsec - T1.tv_nsec) / 1000;

	rate = cnt / (diff / 1000000);

	printf("%.2f\n", rate);

	return 0;
}

void exec(int cnt) {
	int iter = 0;
	int idx = 0;
	randBuffer = (struct drand48_data*)malloc(sizeof(struct drand48_data));
	while (!start);
	while (!stop) {
		idx = zipf(g_synth_table_size - 1, g_zipf_theta);
#ifdef MUTEX
		pthread_mutex_lock(latch[idx]);
#elif SPIN
		pthread_spin_lock( latch[idx] );
#elif ATMOIC
		latch[idx].lock();
#endif

#ifdef MUTEX
		pthread_mutex_unlock(latch[idx]);
#elif SPIN
		pthread_spin_unlock( latch[idx] );
#elif ATMOIC
		latch[idx].unlock();
#endif
		
		iter += 1;
		if (iter >= max_iter)
			stop = true;
		if (stop) {
			total_cnt[cnt] = iter;
			break;
		}
	}
}

void calculateDenom() {
        uint64_t table_size = g_synth_table_size; 
        the_n = table_size - 1;                                        
        denom = zeta(the_n, g_zipf_theta);                             
}

double zeta(uint64_t n, double theta) {                    
        double sum = 0;
        for (uint64_t i = 1; i <= n; i++)                              
                sum += pow(1.0 / i, theta);                            
        return sum;                                                    
}

uint64_t zipf(uint64_t n, double theta) {
        double alpha = 1 / (1 - theta);
        double zetan = denom;
        double eta = (1 - pow(2.0 / n, 1 - theta)) /
                (1 - zeta_2_theta / zetan);
        double u;
        drand48_r(randBuffer, &u);
        double uz = u * zetan;
        if (uz < 1) return 1;
        if (uz < 1 + pow(0.5, theta)) return 2;
        return 1 + (uint64_t)(n * pow(eta*u -eta + 1, alpha));
}
