#include "global.h"
#include "helper.h"
#include "stats.h"
#include "mem_alloc.h"
#include "wl.h"
#include <thread>

#define BILLION 1000000000UL

extern workload *m_wl;
extern std::thread perf;
extern bool start_perf;

#if INTERACTIVE_MODE == 1
extern bool is_storage_server;
#endif

void Stats_thd::init(uint64_t thd_id) {
	clear();
	all_debug1 = (uint64_t *)
		_mm_malloc(sizeof(uint64_t) * MAX_TXN_PER_PART, 64);
	all_debug2 = (uint64_t *)
		_mm_malloc(sizeof(uint64_t) * MAX_TXN_PER_PART, 64);
}

void Stats_thd::clear() {
	txn_cnt = 0;
	abort_cnt = 0;
	run_time = 0;
	time_man = 0;
	debug1 = 0;
	debug2 = 0;
	debug3 = 0;
	debug4 = 0;
	debug5 = 0;
	time_index = 0;
	time_abort = 0;
	time_cleanup = 0;
	time_wait = 0;
	time_ts_alloc = 0;
	latency = 0;
	time_query = 0;
}

void Stats_tmp::init() {
	clear();
}

void Stats_tmp::clear() {	
	time_man = 0;
	time_index = 0;
	time_wait = 0;
}

void Stats::init() {
	if (!STATS_ENABLE) 
		return;
	_stats = (Stats_thd**) 
			_mm_malloc(sizeof(Stats_thd*) * g_thread_cnt, 64);
	tmp_stats = (Stats_tmp**) 
			_mm_malloc(sizeof(Stats_tmp*) * g_thread_cnt, 64);
	dl_detect_time = 0;
	dl_wait_time = 0;
	deadlock = 0;
	cycle_detect = 0;
	perf = std::thread(&Stats::performance, this);
}

void Stats::init(uint64_t thread_id) {
	if (!STATS_ENABLE) 
		return;
	_stats[thread_id] = (Stats_thd *) 
		_mm_malloc(sizeof(Stats_thd), 64);
	tmp_stats[thread_id] = (Stats_tmp *)
		_mm_malloc(sizeof(Stats_tmp), 64);

	_stats[thread_id]->init(thread_id);
	tmp_stats[thread_id]->init();
}

void Stats::clear(uint64_t tid) {
	if (STATS_ENABLE) {
		_stats[tid]->clear();
		tmp_stats[tid]->clear();

		dl_detect_time = 0;
		dl_wait_time = 0;
		cycle_detect = 0;
		deadlock = 0;
	}
}

void Stats::add_debug(uint64_t thd_id, uint64_t value, uint32_t select) {
	if (g_prt_lat_distr && warmup_finish) {
		uint64_t tnum = _stats[thd_id]->txn_cnt;
		if (select == 1)
			_stats[thd_id]->all_debug1[tnum] = value;
		else if (select == 2)
			_stats[thd_id]->all_debug2[tnum] = value;
	}
}

void Stats::commit(uint64_t thd_id) {
	if (STATS_ENABLE) {
		_stats[thd_id]->time_man += tmp_stats[thd_id]->time_man;
		_stats[thd_id]->time_index += tmp_stats[thd_id]->time_index;
		_stats[thd_id]->time_wait += tmp_stats[thd_id]->time_wait;
		tmp_stats[thd_id]->init();
	}
}

void Stats::abort(uint64_t thd_id) {	
	if (STATS_ENABLE) 
		tmp_stats[thd_id]->init();
}

void Stats::print() {
	
	uint64_t total_txn_cnt = 0;
	uint64_t total_abort_cnt = 0;
	double total_run_time = 0;
	double total_time_man = 0;
	double total_debug1 = 0;
	double total_debug2 = 0;
	double total_debug3 = 0;
	double total_debug4 = 0;
	double total_debug5 = 0;
	double total_time_index = 0;
	double total_time_abort = 0;
	double total_time_cleanup = 0;
	double total_time_wait = 0;
	double total_time_ts_alloc = 0;
	double total_latency = 0;
	double total_time_query = 0;

	uint64_t total_tpcc_payment_commit = 0;
	uint64_t total_tpcc_new_order_commit = 0;
	uint64_t total_tpcc_order_status_commit = 0;
	uint64_t total_tpcc_delivery_commit = 0;
	uint64_t total_tpcc_stock_level_commit = 0;
	uint64_t total_tpcc_payment_abort = 0;
	uint64_t total_tpcc_new_order_abort = 0;
	uint64_t total_tpcc_order_status_abort = 0;
	uint64_t total_tpcc_delivery_abort = 0;
	uint64_t total_tpcc_stock_level_abort = 0;

	int actual_thd_cnt = 0;
	double rate = 0;

	// printf("\n>>>>>>>>>>>\n");
	for (uint64_t tid = 0; tid < g_thread_cnt; tid ++) {
		if (_stats[tid] == NULL) continue;
		actual_thd_cnt += 1;
		// printf("%d\t%p\n", tid, _stats[tid]);
		total_txn_cnt += _stats[tid]->txn_cnt;
		total_abort_cnt += _stats[tid]->abort_cnt;
		rate += (double)_stats[tid]->txn_cnt / ((double)_stats[tid]->run_time / 1000000000);
		total_run_time += _stats[tid]->run_time;
		total_time_man += _stats[tid]->time_man;
		total_debug1 += _stats[tid]->debug1;
		// printf("%d\n", tid);
		total_debug2 += _stats[tid]->debug2;
		total_debug3 += _stats[tid]->debug3;
		total_debug4 += _stats[tid]->debug4;
		total_debug5 += _stats[tid]->debug5;
		total_time_index += _stats[tid]->time_index;
		total_time_abort += _stats[tid]->time_abort;
		total_time_cleanup += _stats[tid]->time_cleanup;
		total_time_wait += _stats[tid]->time_wait;
		total_time_ts_alloc += _stats[tid]->time_ts_alloc;
		total_latency += _stats[tid]->latency;
		total_time_query += _stats[tid]->time_query;

		total_tpcc_payment_commit += _stats[tid]->tpcc_payment_commit;
		total_tpcc_new_order_commit += _stats[tid]->tpcc_new_order_commit;
		total_tpcc_order_status_commit += _stats[tid]->tpcc_order_status_commit;
		total_tpcc_delivery_commit += _stats[tid]->tpcc_delivery_commit;
		total_tpcc_stock_level_commit += _stats[tid]->tpcc_stock_level_commit;
		total_tpcc_payment_abort += _stats[tid]->tpcc_payment_abort;
		total_tpcc_new_order_abort += _stats[tid]->tpcc_new_order_abort;
		total_tpcc_order_status_abort += _stats[tid]->tpcc_order_status_abort;
		total_tpcc_delivery_abort += _stats[tid]->tpcc_delivery_abort;
		total_tpcc_stock_level_abort += _stats[tid]->tpcc_stock_level_abort;
		
		// printf("%lld\t%lld\t%lld\t%lld\t%lld\t%lld\t%lld\t%lld\t%lld\t%lld\n",
		// _stats[tid]->tpcc_payment_commit,
		// _stats[tid]->tpcc_new_order_commit,
		// _stats[tid]->tpcc_order_status_commit,
		// _stats[tid]->tpcc_delivery_commit,
		// _stats[tid]->tpcc_stock_level_commit,
		// _stats[tid]->tpcc_payment_abort,
		// _stats[tid]->tpcc_new_order_abort,
		// _stats[tid]->tpcc_order_status_abort,
		// _stats[tid]->tpcc_delivery_abort,
		// _stats[tid]->tpcc_stock_level_abort
		// );
		
		/*printf("[tid=%ld] txn_cnt=%ld,abort_cnt=%ld\n", 
			tid,
			_stats[tid]->txn_cnt,
			_stats[tid]->abort_cnt
		);*/
	}
	// printf("<<<<<<<<<<<\n");
	FILE * outf;
	if (output_file != NULL) {
		outf = fopen(output_file, "w");
		fprintf(outf, "[summary] txn_cnt=%ld, abort_cnt=%ld"
			", run_time=%f, rxn_rate=%.2f, time_wait=%f, time_ts_alloc=%f"
			", time_man=%f, time_index=%f, time_abort=%f, time_cleanup=%f, latency=%f"
			", deadlock_cnt=%ld, cycle_detect=%ld, dl_detect_time=%f, dl_wait_time=%f"
			", time_query=%f, debug1=%f, debug2=%f, debug3=%f, debug4=%f, debug5=%f\n",
			total_txn_cnt, 
			total_abort_cnt,
			total_run_time / BILLION,
			(double)(total_txn_cnt * g_thread_cnt) / (total_run_time / 1000),
			total_time_wait / BILLION,
			total_time_ts_alloc / BILLION,
			(total_time_man - total_time_wait) / BILLION,
			total_time_index / BILLION,
			total_time_abort / BILLION,
			total_time_cleanup / BILLION,
			total_latency / BILLION / total_txn_cnt,
			deadlock,
			cycle_detect,
			dl_detect_time / BILLION,
			dl_wait_time / BILLION,
			total_time_query / BILLION,
			total_debug1, // / BILLION,
			total_debug2, // / BILLION,
			total_debug3, // / BILLION,
			total_debug4, // / BILLION,
			total_debug5 / BILLION
		);
		fclose(outf);
	}
	/* printf("[summary] txn_cnt=%ld, abort_cnt=%ld"
		", run_time=%f, rxn_rate=%.2f, time_wait=%f, time_ts_alloc=%f"
		", time_man=%f, time_index=%f, time_abort=%f, time_cleanup=%f, latency=%f"
		", deadlock_cnt=%ld, cycle_detect=%ld, dl_detect_time=%f, dl_wait_time=%f"
		", time_query=%f, debug1=%f, debug2=%f, debug3=%f, debug4=%f, debug5=%f\n", 
		total_txn_cnt, 
		total_abort_cnt,
		total_run_time / BILLION,
		(double)(total_txn_cnt * g_thread_cnt) / (total_run_time / BILLION),
		total_time_wait / BILLION,
		total_time_ts_alloc / BILLION,
		(total_time_man - total_time_wait) / BILLION,
		total_time_index / BILLION,
		total_time_abort / BILLION,
		total_time_cleanup / BILLION,
		total_latency / BILLION / total_txn_cnt,
		deadlock,
		cycle_detect,
		dl_detect_time / BILLION,
		dl_wait_time / BILLION,
		total_time_query / BILLION,
		total_debug1 / BILLION,
		total_debug2, // / BILLION,
		total_debug3, // / BILLION,
		total_debug4, // / BILLION,
		total_debug5  // / BILLION 
	); */
	
	// printf("%.2f\t", (double)(total_txn_cnt * actual_thd_cnt) / (total_run_time / BILLION));
	
	// print_dis();

	if (g_prt_lat_distr)
		print_lat_distr(0);

	// printf("\n");
	// for (int i = 1; i <=5; ++i) {
	// 	print_lat_distr(i);
	// 	printf("\n");
	// }

	// printf("\n");
	uint64_t try_cnt = 0, abort_cnt1 = 0, abort_cnt2 = 0;
	for (uint  i = 0; i < g_thread_cnt; ++i) {
		if (_stats[i] == NULL) continue;
		try_cnt += _stats[i]->try_cnt;
		abort_cnt1 += _stats[i]->abort_cnt1;
		abort_cnt2 += _stats[i]->abort_cnt2;
		// printf("%lld\t%lld\n", _stats[i]->try_cnt, _stats[i]->abort_cnt1);
	}

	// printf("%.2f\t%.2f\t", (double)abort_cnt1 / try_cnt, (double)abort_cnt2 / total_txn_cnt);
	printf("%.2f\t", (double)abort_cnt1 / try_cnt);

	// printf("total_tpcc_payment_commit= %lld\n", total_tpcc_payment_commit);
	// printf("total_tpcc_new_order_commit= %lld\n", total_tpcc_new_order_commit);
	// printf("total_tpcc_order_status_commit= %lld\n", total_tpcc_order_status_commit);
	// printf("total_tpcc_delivery_commit= %lld\n", total_tpcc_delivery_commit);
	// printf("total_tpcc_stock_level_commit= %lld\n", total_tpcc_stock_level_commit);
	// printf("total_tpcc_payment_abort= %lld\n", total_tpcc_payment_abort);
	// printf("total_tpcc_new_order_abort= %lld\n", total_tpcc_new_order_abort);
	// printf("total_tpcc_order_status_abort= %lld\n", total_tpcc_order_status_abort);
	// printf("total_tpcc_delivery_abort= %lld\n", total_tpcc_delivery_abort);
	// printf("total_tpcc_stock_level_abort= %lld\n", total_tpcc_stock_level_abort);
}

void Stats::print_lat_distr(int off) {
	uint64_t total_lat_dis[MAX_LAT] = {0};
	double total_cnt = 0.001;

	uint64_t total_abt_dis[MAX_LAT] = {0};
	double total_abt = 0.001;


	for (uint i = 0; i < g_thread_cnt; ++i) {
		if (_stats[i] == NULL) continue;
		for (int j = 0; j < MAX_LAT; ++j) {
			total_lat_dis[j] += _stats[i]->lat_dis[off][j];
			total_cnt += (double)_stats[i]->lat_dis[off][j];

			total_abt_dis[j] += _stats[i]->abort_dis[off][j];
			total_abt += (double)_stats[i]->abort_dis[off][j];
		}
	}

	double tmp_cnt = 0;
	bool p_50 = false, /*p_90 = false, p_95 = false,*/ p_99 = false, p_999 = false, p_max = false;
	for (int i = 0; i < MAX_LAT; ++i) {
		tmp_cnt += (double)total_lat_dis[i];
		if (tmp_cnt / total_cnt > 0.5 && p_50 == false) {
			printf ("%d\t", i);
			p_50 = true;
		}
		// if (tmp_cnt / total_cnt > 0.9 && p_90 == false) {
		// 	printf ("%d\t", i);
		// 	p_90 = true;
		// }
		// if (tmp_cnt / total_cnt > 0.95 && p_95 == false) {
		// 	printf ("%d\t", i);
		// 	p_95 = true;
		// }
		if (tmp_cnt / total_cnt > 0.99 && p_99 == false) {
			printf ("%d\t", i);
			p_99 = true;
		}
		if (tmp_cnt / total_cnt > 0.999 && p_999 == false) {
			printf ("%d\t", i);
			p_999 = true;
		} 
		if (tmp_cnt / total_cnt >= 0.9999 && p_max == false) {
			printf ("%d\t", i);
			p_max = true;
		}
	}

	double abt_cnt = 0;
	p_50 = false;
	// p_90 = false;
	// p_95 = false;
	p_99 = false;
	p_999 = false;
	p_max = false;
	for (int i = 0; i < MAX_LAT; ++i) {
		abt_cnt += (double)total_abt_dis[i];
		if (abt_cnt / total_abt > 0.5 && p_50 == false) {
			printf ("%d\t", i);
			p_50 = true;
		}
		// if (abt_cnt / total_abt > 0.9 && p_90 == false) {
		// 	printf ("%d\t", i);
		// 	p_90 = true;
		// }
		// if (abt_cnt / total_abt > 0.95 && p_95 == false) {
		// 	printf ("%d\t", i);
		// 	p_95 = true;
		// }
		if (abt_cnt / total_abt > 0.99 && p_99 == false) {
			printf ("%d\t", i);
			p_99 = true;
		}
		if (abt_cnt / total_abt > 0.999 && p_999 == false) {
			printf ("%d\t", i);
			p_999 = true;
		} 
		if (abt_cnt / total_abt >= 0.9999 && p_max == false) {
			printf ("%d\t", i);
			p_max = true;
		} 
	}
}


void Stats::performance(){
#if INTERACTIVE_MODE == 1
	if (is_storage_server)
		return;
#endif
	while (!start_perf) usleep(10);
	// printf(".......%p\n", &(m_wl->sim_done));
	sleep(1);
	uint64_t old_total_cnt, new_total_cnt;
	ts_t start_time, end_time;
	double rate;
	old_total_cnt = new_total_cnt = 0;

	for (int i = 0; i < (int)g_thread_cnt; ++i)
		old_total_cnt += _stats[i]->txn_cnt;

// _start:
	// ProfilerStart("profile/prof");
	start_time = get_sys_clock();
	sleep(3);
	
	new_total_cnt = 0;

	for (int i = 0; i < (int)g_thread_cnt; ++i)
		new_total_cnt += _stats[i]->txn_cnt;
	
	end_time = get_sys_clock();
	rate = (new_total_cnt - old_total_cnt) / ((double)(end_time - start_time) / 1000000000);

	printf("%.2f\t", rate);
	old_total_cnt = new_total_cnt;
	// ProfilerStop();
	// goto _start;
	ATOM_CAS(m_wl->sim_done, false, true);
}