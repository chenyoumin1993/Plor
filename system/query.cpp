#include <sched.h>
#include <atomic>

#include "query.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"

/*************************************************/
//     class Query_queue
/*************************************************/
int Query_queue::_next_tid;

#if VALVE_ENABLED == 1
uint8_t sig_new_req[THREAD_CNT][1];

extern workload * m_wl;

void valve_f(int id) {
	// set_affinity(id + THREAD_CNT);
	double nap = (double)1000000000 / ((double)VALVE_TP / VALVE_CNT); // ns
	assert((THREAD_CNT % VALVE_CNT) == 0);
	int s_idx = id * (THREAD_CNT / VALVE_CNT);
	int e_idx = (id + 1) * (THREAD_CNT / VALVE_CNT);

	int i = s_idx;
	while (!m_wl->sim_done) {
		asm volatile ("lfence" ::: "memory");
		while (sig_new_req[i][0] == 1 && !m_wl->sim_done) {
			i = (i == (e_idx - 1)) ? s_idx : (i + 1);
		}
		__sync_fetch_and_add(&sig_new_req[i][0], 1);
		// nano_sleep(nap);
	}
}
#endif

void 
Query_queue::init(workload * h_wl) {
	all_queries = new Query_thd * [g_thread_cnt];
	_wl = h_wl;
	_next_tid = 0;

#if VALVE_ENABLED == 1
	for (int i = 0; i < THREAD_CNT; ++i)
		sig_new_req[i][0] = true;
#endif
	

#if WORKLOAD == YCSB	
	ycsb_query::calculateDenom();
#elif WORKLOAD == TPCC
	assert(tpcc_buffer != NULL);
#endif
	// int64_t begin = get_server_clock();
	pthread_t p_thds[CORE_CNT - 1];
	for (UInt32 i = 0; i < CORE_CNT - 1; i++) {
		pthread_create(&p_thds[i], NULL, threadInitQuery, this);
	}
	threadInitQuery(this);
	for (uint32_t i = 0; i < CORE_CNT - 1; i++) 
		pthread_join(p_thds[i], NULL);
	// int64_t end = get_server_clock();
	// printf("Query Queue Init Time %f\n", 1.0 * (end - begin) / 1000000000UL);
}

void 
Query_queue::init_per_thread(int thread_id) {	
	all_queries[thread_id] = (Query_thd *) _mm_malloc(sizeof(Query_thd), 64);
	all_queries[thread_id]->init(_wl, thread_id);
}

base_query * 
Query_queue::get_next_query(uint64_t thd_id) {
#if VALVE_ENABLED == 1
	while (!sig_new_req[thd_id][0] && !m_wl->sim_done) {
		PAUSE
		asm volatile ("lfence" ::: "memory");
	}
	__sync_fetch_and_add(&sig_new_req[thd_id][0], -1);

#endif
	base_query * query = all_queries[thd_id]->get_next_query(thd_id);
	query->abort_cnt = 0;
	return query;
}

void *
Query_queue::threadInitQuery(void * This) {
	Query_queue * query_queue = (Query_queue *)This;
	
	while (true) {
		uint32_t tid = ATOM_FETCH_ADD(_next_tid, 1);
		
		if (tid > (g_thread_cnt - 1))
			break;
		
		// set cpu affinity
		set_affinity(tid);

		query_queue->init_per_thread(tid);
	}
	return NULL;
}

/*************************************************/
//     class Query_thd
/*************************************************/

void 
Query_thd::init(workload * h_wl, int thread_id) {
	uint64_t request_cnt;
	q_idx = 0;
	// thd_id = thread_id;
	request_cnt = WARMUP / g_thread_cnt + MAX_TXN_PER_PART + 4;
#if ABORT_BUFFER_ENABLE
    request_cnt += ABORT_BUFFER_SIZE;
#endif
	_request_cnt = request_cnt;
#if WORKLOAD == YCSB	
	queries = (ycsb_query *) 
		mem_allocator.alloc(sizeof(ycsb_query) * request_cnt, thread_id);
	srand48_r(thread_id + 1, &buffer);
#elif WORKLOAD == TPCC
	queries = (tpcc_query *) _mm_malloc(sizeof(tpcc_query) * request_cnt, 64);
#endif
	for (UInt32 qid = 0; qid < request_cnt; qid ++) {
#if WORKLOAD == YCSB	
		new(&queries[qid]) ycsb_query();
		queries[qid].init(thread_id, h_wl, this);
#elif WORKLOAD == TPCC
		new(&queries[qid]) tpcc_query();
		queries[qid].init(thread_id, h_wl);
#endif
	}
}

base_query * 
Query_thd::get_next_query(uint64_t thd_id) {
	base_query * query = &queries[q_idx++];
	query->start_time = get_sys_clock();
	assert(query->request_cnt != 0);
	query->deadline_time = query->start_time + RS_FACTOR * query->request_cnt;
	query->deadline_time = (query->deadline_time << 6) + thd_id;
	if (q_idx >= _request_cnt) q_idx = 0;
	if (query == NULL) printf("no requests......\n");
	return query;
}
