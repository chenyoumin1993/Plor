#ifndef _SYNTH_BM_H_
#define _SYNTH_BM_H_

#include "wl.h"
#include "txn.h"
#include "global.h"
#include "helper.h"

inline void micro_sleep(int us) {
	struct timespec T1, T2;
	double diff = 0;
	clock_gettime(CLOCK_MONOTONIC, &T1);
	while (diff < us) {
		clock_gettime(CLOCK_MONOTONIC, &T2);
		diff = (T2.tv_sec - T1.tv_sec) * 1000000 + (T2.tv_nsec - T1.tv_nsec) / 1000;
	}
}

class ycsb_query;

class ycsb_wl : public workload {
public :
	RC init();
	RC init_table();
	RC init_schema(string schema_file);
	RC get_txn_man(txn_man *& txn_manager, thread_t * h_thd);
	int key_to_part(uint64_t key);
	INDEX * the_index;
	table_t * the_table;
private:
	void init_table_parallel();
	void * init_table_slice();
	static void * threadInitTable(void * This) {
		((ycsb_wl *)This)->init_table_slice(); 
		return NULL;
	}
	pthread_mutex_t insert_lock;
	//  For parallel initialization
	static int next_tid;
};

class ycsb_txn_man : public txn_man
{
public:
	void init(thread_t * h_thd, workload * h_wl, uint64_t part_id); 
	RC run_txn(base_query * query, coro_yield_t &yield, int coro_id);
private:
	uint64_t row_cnt;
	ycsb_wl * _wl;
};

#endif
