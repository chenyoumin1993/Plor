#pragma once 

#include "global.h"
#include "helper.h"

class workload;
class ycsb_query;
class tpcc_query;

class base_query {
public:
	virtual void init(uint64_t thd_id, workload * h_wl) = 0;
	uint64_t waiting_time;
	uint64_t part_num;
	uint64_t * part_to_access;
	ts_t start_time;
	ts_t stop_time;
	ts_t deadline_time = 0;
	ts_t timestamp = 0;
	uint64_t abort_cnt = 0;
	uint64_t exec_time = 0;
	uint64_t backoff = BACKOFF_CYCLE;
	uint64_t request_cnt = 0;
	bool readonly = false;
	bool read_committed = false;
	bool ro_print = false;
	TPCCTxnType type;
};

// All the querise for a particular thread.
class Query_thd {
public:
	void init(workload * h_wl, int thread_id);
	base_query * get_next_query(uint64_t thd_id); 
	int q_idx;
	int _request_cnt;
#if WORKLOAD == YCSB
	ycsb_query * queries;
#else 
	tpcc_query * queries;
#endif
	char pad[CL_SIZE - sizeof(void *) - sizeof(int)];
	drand48_data buffer;
};

// TODO we assume a separate task queue for each thread in order to avoid 
// contention in a centralized query queue. In reality, more sofisticated 
// queue model might be implemented.
class Query_queue {
public:
	void init(workload * h_wl);
	void init_per_thread(int thread_id);
	base_query * get_next_query(uint64_t thd_id); 
	
private:
	static void * threadInitQuery(void * This);

	Query_thd ** all_queries;
	workload * _wl;
	static int _next_tid;
};
