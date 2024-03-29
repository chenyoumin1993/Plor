#include <sched.h>
#include "global.h"
#include "manager.h"
#include "thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "test.h"
#include "tpcc.h"
#include <atomic>

#define CONFIG_H "silo/config/config-perf.h"
#include "../silo/rcu.h"

#if INTERACTIVE_MODE == 1
#include "rpc.h"
thread_local erpc::Rpc<erpc::CTransport> *rpc;
erpc::Nexus *nexus;
thread_local erpc::MsgBuffer req[N_REPLICAS];
thread_local erpc::MsgBuffer resp[N_REPLICAS];
thread_local int session_num[N_REPLICAS];
thread_local int outstanding_msg_cnt = 0;
bool is_storage_server = false;

void sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *) {}
#endif

extern __thread int *next_coro;
extern __thread coro_call_t *coro_arr;
char _pad1111[4096];
txn_man *txn_tb[THREAD_CNT];
__thread int mytid;
char _pad2222[4096];

std::atomic<int> start_perf(0);

// extern thread_local int lock_cnt;

void thread_t::init(uint64_t thd_id, workload * workload) {
	_thd_id = thd_id;
	_wl = workload;
	srand48_r((_thd_id + 1) * get_sys_clock(), &buffer);
	_abort_buffer_size = ABORT_BUFFER_SIZE;
	_abort_buffer = (AbortBufferEntry *) _mm_malloc(sizeof(AbortBufferEntry) * _abort_buffer_size, 64); 
	for (int i = 0; i < _abort_buffer_size; i++)
		_abort_buffer[i].query = NULL;
	_abort_buffer_empty_slots = _abort_buffer_size;
	_abort_buffer_enable = (g_params["abort_buffer_enable"] == "true");
#if PENALTY_POLICY == 2
	// _epoch_buffer.reserve(1024 * 1024);
	_epoch_buffer = new std::queue<base_query*>;
#endif
}

uint64_t thread_t::get_thd_id() { return _thd_id; }
uint64_t thread_t::get_host_cid() {	return _host_cid; }
void thread_t::set_host_cid(uint64_t cid) { _host_cid = cid; }
uint64_t thread_t::get_cur_cid() { return _cur_cid; }
void thread_t::set_cur_cid(uint64_t cid) {_cur_cid = cid; }

int64_t starttime1, endtime1;

RC thread_t::run() {
#if INTERACTIVE_MODE == 1
	if (!is_storage_server) {
		rpc = new erpc::Rpc<erpc::CTransport>(nexus, nullptr, _thd_id, sm_handler);

		int s_replicas = sizeof(replicanames) / sizeof(replicanames[0]);

		// Connect to the servers.
		for (int i = 0; i < s_replicas; ++i) {
			std::string server_uri = replicanames[i] + ":" + std::to_string(kUDPPortBase);
			session_num[i] = rpc->create_session(server_uri, _thd_id);
			// std::cout << server_uri << " : " << std::to_string(_thd_id) << std::endl;
			while (!rpc->is_connected(session_num[i])) rpc->run_event_loop_once();
			// Reserve message buffers.
			req[i] = rpc->alloc_msg_buffer_or_die(sizeof(WriteRowRequest));
			resp[i] = rpc->alloc_msg_buffer_or_die(sizeof(WriteRowRequest));
		}
	}
#endif
	// enable the performance monitor: 
#if !NOGRAPHITE
	_thd_id = CarbonGetTileId();
#endif
	if (warmup_finish) {
		mem_allocator.register_thread(_thd_id);
	}
	pthread_barrier_wait( &warmup_bar );
	stats.init(get_thd_id());
	pthread_barrier_wait( &warmup_bar );

	set_affinity(get_thd_id());

	myrand rdm;
	rdm.init(get_thd_id());
	RC rc = RCOK;
	txn_man * m_txn;
	rc = _wl->get_txn_man(m_txn, this);
	assert (rc == RCOK);
	glob_manager->set_txn_man(m_txn);
	txn_tb[_thd_id] = m_txn;
	mytid = _thd_id;
	// printf("mytid = %d\n", mytid);
	base_query * m_query = NULL;
	uint64_t thd_txn_id = 0;
	UInt64 txn_cnt = 0;

	start_perf ++;

	while (true) {
		ts_t starttime = get_sys_clock();
		if (WORKLOAD != TEST) {
#if PENALTY_POLICY == 0
			int trial = 0;
			if (_abort_buffer_enable) {
				m_query = NULL;
				while (trial < 2) {
					ts_t curr_time = get_sys_clock();
					ts_t min_ready_time = UINT64_MAX;
					if (_abort_buffer_empty_slots < _abort_buffer_size) {
						for (int i = 0; i < _abort_buffer_size; i++) {
							if (_abort_buffer[i].query != NULL && curr_time > _abort_buffer[i].ready_time) {
								m_query = _abort_buffer[i].query;
								// m_query->stop_time = get_sys_clock();
								// DIS_STATS(get_thd_id(), lat_dis, ((m_query->stop_time - m_query->start_time) / 1000));
								_abort_buffer[i].query = NULL;
								_abort_buffer_empty_slots ++;
								break;
							} else if (_abort_buffer_empty_slots == 0 
									  && _abort_buffer[i].ready_time < min_ready_time) 
								min_ready_time = _abort_buffer[i].ready_time;
						}
					}
					if (m_query == NULL && _abort_buffer_empty_slots == 0) {
						assert(trial == 0);
						// No avaiable slot, but too much aborted txs, sleep here.
						M_ASSERT(min_ready_time >= curr_time, "min_ready_time=%ld, curr_time=%ld\n", min_ready_time, curr_time);
						usleep((min_ready_time - curr_time) / 1000);
					} else if (m_query == NULL) {
						// Otherwise, get a new reuqest (have enough slot, and all )
						m_query = query_queue->get_next_query( _thd_id );
					}
					if (m_query != NULL)
						break;
				}
			} else {
				if (rc == RCOK)
					m_query = query_queue->get_next_query( _thd_id );
			}
#elif PENALTY_POLICY == 1  // ONLY HAS ONE SLOT
			// int trial = 0;
			if (_abort_buffer_enable) {
				m_query = NULL;
				if (_abort_buffer[0].query != NULL) {
					m_query = _abort_buffer[0].query;
					_abort_buffer[0].query = NULL;
					_abort_buffer_empty_slots ++;
				} else {
					m_query = query_queue->get_next_query( _thd_id );
				}
			} else {
				if (rc == RCOK)
					m_query = query_queue->get_next_query( _thd_id );
			}
#else
			m_query = NULL;
			if (local_epoch_cnt != epoch_cnt) {
				// epoch changes, try to fetch the aborted TXs from the _epoch_buffer first;
				int size = _epoch_buffer->size();
				if (size > 0) {
					m_query = _epoch_buffer->front();
					_epoch_buffer->pop();
				} else if (size == 0) {
					// No aborted TXs, update local epoch.
					local_epoch_cnt = epoch_cnt;
				} else {
					// printf("error %d - %d.\n", _thd_id, size);
				}
			}
			if (m_query == NULL) {
				m_query = query_queue->get_next_query( _thd_id );
			}
#endif
		}

		m_txn->readonly = m_txn->read_committed = false;

		if (m_query->readonly && m_query->abort_cnt <= 100) {
			m_txn->readonly = true;
		}
		
		if (m_query->read_committed) {
			m_txn->read_committed = true;
		}

		stats._stats[get_thd_id()]->try_cnt += 1;
		// INC_STATS(_thd_id, time_query, get_sys_clock() - starttime);
		m_txn->abort_cnt = 0;
		// if (m_txn->wound_cnt % 10000 == 0)
		// 	printf("%d - %d\n", m_txn->get_thd_id(), m_txn->wound_cnt);
		if (CC_ALG == WOUND_WAIT || CC_ALG == OLOCK || CC_ALG == PLOR || CC_ALG == HLOCK) {
		#ifdef DEBUG_WOUND
			if (m_txn->wound) m_txn->wound_cnt += 1;
			m_txn->last_wound = 0;
			m_txn->lock_cnt = 0;
			m_txn->cur_owner_id = 0;
		#endif
			m_txn->wound = false;
			m_txn->waiting = false;
			m_txn->ex_mode = false;  // for olock/dlock only.
			// if (m_txn->lock_holding != m_txn->lock_releasing) {
			// 	printf("locked = %d, released = %d\n", m_txn->lock_holding, m_txn->lock_releasing);
			// 	assert(m_txn->lock_holding == m_txn->lock_releasing);
			// }
			// m_txn->lock_holding = m_txn->lock_releasing = 0;
		}
//#if CC_ALG == VLL
//		_wl->get_txn_man(m_txn, this);
//#endif
		m_txn->set_txn_id(get_thd_id() + thd_txn_id * g_thread_cnt);
		thd_txn_id ++;

		if ((CC_ALG == HSTORE && !HSTORE_LOCAL_TS)
				|| CC_ALG == MVCC 
				|| CC_ALG == HEKATON
				|| CC_ALG == TIMESTAMP) 
			m_txn->set_ts(get_next_ts());
		if (CC_ALG == WAIT_DIE || CC_ALG == WOUND_WAIT || CC_ALG == OLOCK || CC_ALG == PLOR || CC_ALG == HLOCK) {
			if (m_query->timestamp != 0) {
				// This is an aborted TX.
			#if TS_OPT == 0
				// use old ts.
				m_txn->set_ts(m_query->timestamp);
			#else
				m_txn->set_ts(m_query->timestamp - TS_OPT * m_txn->row_num_last_tx);
			#endif
			} else {
				// New TX, acquire the ts first.
			#if RT_ENABLED == 1
				m_txn->set_ts(m_query->deadline_time);
			#else
				m_txn->set_ts(get_next_ts());
			#endif
				m_query->timestamp = m_txn->get_ts();
			}
			asm volatile ("sfence" ::: "memory");
		}

		rc = RCOK;
#if CC_ALG == HSTORE
		if (WORKLOAD == TEST) {
			uint64_t part_to_access[1] = {0};
			rc = part_lock_man.lock(m_txn, &part_to_access[0], 1);
		} else 
			rc = part_lock_man.lock(m_txn, m_query->part_to_access, m_query->part_num);
#elif CC_ALG == VLL
		vll_man.vllMainLoop(m_txn, m_query);
#elif CC_ALG == MVCC || CC_ALG == HEKATON
		glob_manager->add_ts(get_thd_id(), m_txn->get_ts());
#elif CC_ALG == OCC
		// In the original OCC paper, start_ts only reads the current ts without advancing it.
		// But we advance the global ts here to simplify the implementation. However, the final
		// results should be the same.
		m_txn->start_ts = get_next_ts(); 
#endif
		if (rc == RCOK) 
		{
			// scoped_rcu_region guard;
#if CC_ALG != VLL
			if (WORKLOAD == TEST) {
				rc = runTest(m_txn);
			} else {
				// starttime1 = get_server_clock();
				if (stats._stats[0]->txn_cnt == 100 && get_thd_id() == 0) {
					int sc;
					// _wl->print = true;
					// printf("thread 0 now run the 100th tx.\n");
					// scanf("%d", &sc);
					// _wl->print = false;
					// sleep(100000);
				}
				rc = m_txn->run_txn(m_query);

				// endtime1 = get_server_clock();
			}
#endif
#if CC_ALG == HSTORE
			if (WORKLOAD == TEST) {
				uint64_t part_to_access[1] = {0};
				part_lock_man.unlock(m_txn, &part_to_access[0], 1);
			} else 
				part_lock_man.unlock(m_txn, m_query->part_to_access, m_query->part_num);
#endif
		}
		if (rc == Abort) {
			m_query->abort_cnt += 1;
			stats._stats[get_thd_id()]->abort_cnt1 += 1;
			uint64_t penalty = 0;
#if PENALTY_POLICY == 0
			if (ABORT_PENALTY != 0)  {
				double r;
				drand48_r(&buffer, &r);
				penalty = r * ABORT_PENALTY;
			}
			if (!_abort_buffer_enable)
				usleep(penalty / 1000);
			else {
				assert(_abort_buffer_empty_slots > 0);
				for (int i = 0; i < _abort_buffer_size; i ++) {
					if (_abort_buffer[i].query == NULL) {
						// m_query->start_time = get_sys_clock();
						_abort_buffer[i].query = m_query;
						_abort_buffer[i].ready_time = get_sys_clock() + penalty;
						_abort_buffer_empty_slots --;
						break;
					}
				}
			}
#elif PENALTY_POLICY == 1
			if (!_abort_buffer_enable)
				usleep(penalty / 1000);
			else {
				assert(_abort_buffer_empty_slots > 0);
				assert(_abort_buffer[0].query == NULL);
				_abort_buffer[0].query = m_query;
				if (m_query->abort_cnt < 10)
					m_query->backoff <<= 1;
				// wait cycles.
				uint64_t cycles_to_wait = (m_query->backoff == 0) ? 0 : rand_r(&seed) % m_query->backoff;
				if (CC_ALG == PLOR || CC_ALG == HLOCK || CC_ALG == SILO || CC_ALG == MOCC)
					cycles_to_wait = (m_query->readonly) ? 100 : cycles_to_wait;
				// double r;
				// drand48_r(&buffer, &r);
				// uint64_t cycles_to_wait = r * m_query->backoff;
				ts_t wait_start = get_sys_clock();
				wait_cycles(cycles_to_wait);
				ts_t wait_end = get_sys_clock();
				uint64_t diff = (wait_end - wait_start) / 1000;
				if (PRINT_LAT_DEBUG && get_thd_id() == 0) {
					total_backoff_cnt += 1;
					total_backoff_time += wait_end - wait_start; // ns
					backoff_time_dis[diff >= 1000 ? 999 : diff] += 1; // us
				}
			}
#else
			// Put the aborted TX in _epoch_buffer.
			_epoch_buffer->push(m_query);
#endif
		}
		if (rc == RCOK){
			m_query->stop_time = get_sys_clock();
			// if (m_query->ro_print) {
				DIS_STATS(get_thd_id(), lat_dis[0], ((m_query->stop_time - m_query->start_time) / 1000));
				DIS_STATS(get_thd_id(), abort_dis[0], m_query->abort_cnt);
			// }
#if WORKLOAD == TPCC
			switch (((tpcc_query *)m_query)->type) {
				case TPCC_NEW_ORDER :
					DIS_STATS(get_thd_id(), lat_dis[1], ((m_query->stop_time - m_query->start_time) / 1000));
					DIS_STATS(get_thd_id(), abort_dis[1], m_query->abort_cnt);
					break;
				case TPCC_PAYMENT :
					DIS_STATS(get_thd_id(), lat_dis[2], ((m_query->stop_time - m_query->start_time) / 1000));
					DIS_STATS(get_thd_id(), abort_dis[2], m_query->abort_cnt);
					break;
				case TPCC_ORDER_STATUS :
					DIS_STATS(get_thd_id(), lat_dis[3], ((m_query->stop_time - m_query->start_time) / 1000));
					DIS_STATS(get_thd_id(), abort_dis[3], m_query->abort_cnt);
					break;
				case TPCC_DELIVERY :
					DIS_STATS(get_thd_id(), lat_dis[4], ((m_query->stop_time - m_query->start_time) / 1000));
					DIS_STATS(get_thd_id(), abort_dis[4], m_query->abort_cnt);
					break;
				case TPCC_STOCK_LEVEL :
					DIS_STATS(get_thd_id(), lat_dis[5], ((m_query->stop_time - m_query->start_time) / 1000));
					DIS_STATS(get_thd_id(), abort_dis[5], m_query->abort_cnt);
					break;
				default:
					break;
			}
#endif
			if (m_query->abort_cnt > 0)
				stats._stats[get_thd_id()]->abort_cnt2 += 1;
		}
		// DIS_STATS(get_thd_id(), lat_dis, ((endtime1 - starttime1) / 1000));
		ts_t endtime = get_sys_clock();
		uint64_t timespan = endtime - starttime;
		if (PRINT_LAT_DEBUG && get_thd_id() == 0) {
			if (rc == RCOK) {
				total_commit_cnt += 1;
				total_commit_time += timespan;
				total_waiting_2_commit_time_1 += last_waiting_time_1; // ns
				total_waiting_2_commit_time_2 += last_waiting_time_2; // ns
				total_try_exec_2_commit_time += last_try_exec_time;
				total_try_commit_2_commit_time += last_try_commit_time;
				// commit_time_dis[(timespan / 1000) >= 1000 ? 999 : (timespan / 1000)] += 1;
				// waiting_2_commit_time_dis[(last_waiting_time_1 / 1000) >= 1000 ? 999 : (last_waiting_time_1 / 1000)] += 1;
			} else {
				total_abort_cnt += 1;
				total_abort_time += timespan;
				total_waiting_2_abort_time_1 += last_waiting_time_1; // ns
				total_waiting_2_abort_time_2 += last_waiting_time_2; // ns
				total_try_exec_2_abort_time += last_try_exec_time;
				total_try_commit_2_abort_time += last_try_commit_time;
				// abort_time_dis[(timespan / 1000) >= 1000 ? 999 : (timespan / 1000)] += 1;
				// waiting_2_abort_time_dis[(last_waiting_time_1 / 1000) >= 1000 ? 999 : (last_waiting_time_1 / 1000)] += 1;
			}
			last_waiting_time_1 = 0;
			last_waiting_time_2 = 0;
			last_try_exec_time = 0;
			last_try_commit_time = 0;
		}
		INC_STATS(get_thd_id(), run_time, timespan);
		INC_STATS(get_thd_id(), latency, timespan);
		// DIS_STATS(get_thd_id(), lat_dis, timespan);
		//stats.add_lat(get_thd_id(), timespan);
		if (rc == RCOK) {
			stats.commit(get_thd_id());
			txn_cnt ++;
			
			stats._stats[get_thd_id()]->txn_cnt = txn_cnt;
			// if (txn_cnt % 100000 == 0)
			// 	printf("%lu, %lu, %p\n", txn_cnt, stats._stats[get_thd_id()]->txn_cnt, &(stats._stats[get_thd_id()]->txn_cnt));
		} else if (rc == Abort) {
			INC_STATS(get_thd_id(), time_abort, timespan);
			INC_STATS(get_thd_id(), abort_cnt, 1);
			stats.abort(get_thd_id());
			m_txn->abort_cnt ++;
		}

		if (rc == FINISH)
			goto _end;
		if (!warmup_finish && txn_cnt >= WARMUP / g_thread_cnt) 
		{
			stats.clear( get_thd_id() );
			goto _end;
		}

		// if (warmup_finish && txn_cnt >= MAX_TXN_PER_PART) {
		// 	assert(txn_cnt == MAX_TXN_PER_PART);
	    //     if( !ATOM_CAS(_wl->sim_done, false, true) )
		// 		assert( _wl->sim_done);
	    // }
		// if (_wl->print) {
		// 	while (_wl->print) ;
		// }
	    if (_wl->sim_done) { 
   		    goto _end;
   		}
		// if (next_coro[coro_id / CORE_CNT] != (coro_id / CORE_CNT))
		// 	yield(coro_arr[next_coro[coro_id / CORE_CNT]]);
	}
_end:
	// printf("lock_cnt = %d\n", lock_cnt);
	// assert(false);
	if (PRINT_LAT_DEBUG && get_thd_id() == 0) {
		printf("COMMIT@PERF\t%lld\t%lld\t%lld\t%lld\t%lld\t%lld\t", 
		(long long)total_commit_cnt, (long long)total_commit_time / 1000, 
		(long long)total_waiting_2_commit_time_1 / 1000, 
		(long long)total_waiting_2_commit_time_2 / 1000, 
		(long long)total_try_exec_2_commit_time / 1000, 
		(long long)total_try_commit_2_commit_time / 1000);
		printf("ABORT@PERF\t%lld\t%lld\t%lld\t%lld\t%lld\t%lld\t%lld\t", 
		(long long)total_abort_cnt, (long long)total_abort_time / 1000, 
		(long long)total_waiting_2_abort_time_1 / 1000, 
		(long long)total_waiting_2_abort_time_2 / 1000, 
		(long long)total_backoff_time / 1000, 
		(long long)total_try_exec_2_abort_time / 1000, 
		(long long)total_try_commit_2_abort_time / 1000);
	}
#if INTERACTIVE_MODE == 1
	delete rpc;
#endif
	// delete nexus;
	return rc;
}


ts_t
thread_t::get_next_ts() {
	if (g_ts_batch_alloc) {
		if (_curr_ts % g_ts_batch_num == 0) {
			_curr_ts = glob_manager->get_ts(get_thd_id());
			_curr_ts ++;
		} else {
			_curr_ts ++;
		}
		return _curr_ts - 1;
	} else {
		_curr_ts = glob_manager->get_ts(get_thd_id());
		return _curr_ts;
	}
}

RC thread_t::runTest(txn_man * txn)
{
	RC rc = RCOK;
	if (g_test_case == READ_WRITE) {
		rc = ((TestTxnMan *)txn)->run_txn(g_test_case, 0);
#if CC_ALG == OCC
		txn->start_ts = get_next_ts(); 
#endif
		rc = ((TestTxnMan *)txn)->run_txn(g_test_case, 1);
		// printf("READ_WRITE TEST PASSED\n");
		return FINISH;
	}
	else if (g_test_case == CONFLICT) {
		rc = ((TestTxnMan *)txn)->run_txn(g_test_case, 0);
		if (rc == RCOK)
			return FINISH;
		else 
			return rc;
	}
	assert(false);
	return RCOK;
}
