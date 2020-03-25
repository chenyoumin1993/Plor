#include <thread>
#include <execinfo.h>
#include <signal.h>
#include <unistd.h>

#include "global.h"
#include "ycsb.h"
#include "tpcc.h"
#include "test.h"
#include "thread.h"
#include "manager.h"
#include "mem_alloc.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"
#include "coro.h"

void * f(void *);
void exec(coro_yield_t &yield, int coro_id);

__thread coro_call_t *coro_arr;
__thread int *next_coro;

#if PENALTY_POLICY == 2
void * epoch(void *);
bool finished = false;
pthread_t e;
#endif

thread_t ** m_thds;

workload * m_wl;
thread perf;
bool start_perf = false;

#if INTERACTIVE_MODE == 1
#include "rpc.h"
void read_handler(erpc::ReqHandle *req_handle, void *);
void write_handler(erpc::ReqHandle *req_handle, void *);
void insert_handler(erpc::ReqHandle *req_handle, void *);
void remove_handler(erpc::ReqHandle *req_handle, void *);
void storage_service(int id, int replica_cnt);
extern thread_local erpc::Rpc<erpc::CTransport> *rpc;
extern erpc::Nexus *nexus;
extern bool is_storage_server;
int replica_cnt = 0;
#endif

#if VALVE_ENABLED == 1
std::thread valves[VALVE_CNT];
#endif

// defined in parser.cpp
void parser(int argc, char * argv[]);

void handler(int sig) {
  void *array[10];
  size_t size;

  // get void*'s for all entries on the stack
  size = backtrace(array, 10);

  // print out all the frames to stderr
  fprintf(stderr, "Error: signal %d:\n", sig);
  backtrace_symbols_fd(array, size, STDERR_FILENO);
  exit(1);
}

int main(int argc, char* argv[])
{
	parser(argc, argv);
	signal(SIGSEGV, handler);

#if INTERACTIVE_MODE == 1
	int s_replicas = sizeof(replicas) / sizeof(replicas[0]);
	char hostname[32];
	gethostname(hostname, 32);
	int i;
	for (i = 0; i < s_replicas; ++i)
		if (strcmp(hostname, replicas[i]) == 0)
			break;
	if (i < s_replicas) {
		// storage services.
		is_storage_server = true;
		std::string server_uri = replicanames[i] + ":" + std::to_string(kUDPPortBase);
		nexus = new erpc::Nexus(server_uri, 0, 0);
		nexus->register_req_func(kReadType, read_handler);
		nexus->register_req_func(kWriteType, write_handler);
		nexus->register_req_func(kInsertType, insert_handler);
		nexus->register_req_func(kRemoveType, remove_handler);
	} else {
		// TX Manager.
		std::string client_uri = clientname + ":" + std::to_string(kUDPPortBase);
		nexus = new erpc::Nexus(client_uri, 0, 0);
	}
#endif

	mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt); 
	stats.init();
	glob_manager = (Manager *) _mm_malloc(sizeof(Manager), 64);
	glob_manager->init();
	if (g_cc_alg == DL_DETECT) 
		dl_detector.init();
	// printf("mem_allocator initialized!\n");
	switch (WORKLOAD) {
		case YCSB :
			m_wl = new ycsb_wl; break;
		case TPCC :
			m_wl = new tpcc_wl; break;
		case TEST :
			m_wl = new TestWorkload; 
			((TestWorkload *)m_wl)->tick();
			break;
		default:
			assert(false);
	}
	m_wl->init();
	// printf("workload initialized!\n");
	
	uint64_t thd_cnt = g_thread_cnt;
	pthread_t p_thds[CORE_CNT - 1];
	m_thds = new thread_t * [thd_cnt];
	for (uint32_t i = 0; i < thd_cnt; i++)
		m_thds[i] = (thread_t *) _mm_malloc(sizeof(thread_t), 64);
	// query_queue should be the last one to be initialized!!!
	// because it collects txn latency
	query_queue = (Query_queue *) _mm_malloc(sizeof(Query_queue), 64);
	// Prepare requests for warmup.
	if (WORKLOAD != TEST)
		query_queue->init(m_wl);
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
	// printf("query_queue initialized!\n");
#if CC_ALG == HSTORE
	part_lock_man.init();
#elif CC_ALG == OCC
	occ_man.init();
#elif CC_ALG == VLL
	vll_man.init();
#endif
	for (uint32_t i = 0; i < thd_cnt; i++) 
		m_thds[i]->init(i, m_wl);
	
#if VALVE_ENABLED == 1
	for (int i = 0; i < VALVE_CNT; ++i)
		valves[i] = std::thread(valve_f, i);
#endif

	if (WARMUP > 0){
		// printf("WARMUP start!\n");
#if PENALTY_POLICY == 2
		pthread_create(&e, NULL, epoch, (void *)0);
#endif
		for (uint32_t i = 0; i < CORE_CNT - 1; i++) {
			uint64_t vid = i;
			pthread_create(&p_thds[i], NULL, f, (void *)vid);
		}
		f((void *)(CORE_CNT - 1)); // Er... the main thread also do warmup.
		for (uint32_t i = 0; i < CORE_CNT - 1; i++)
			pthread_join(p_thds[i], NULL);
		// printf("WARMUP finished!\n");
	}
#if PENALTY_POLICY == 2
	finished = true;
	pthread_join(e, NULL);
#endif

	warmup_finish = true;
	pthread_barrier_init( &warmup_bar, NULL, CORE_CNT );
#ifndef NOGRAPHITE
	CarbonBarrierInit(&enable_barrier, CORE_CNT);
#endif
	pthread_barrier_init( &warmup_bar, NULL, CORE_CNT );

	// spawn and run txns again.
	// int64_t starttime = get_server_clock();

#if PENALTY_POLICY == 2
	finished = false;
	pthread_create(&e, NULL, epoch, (void *)0);
#endif
	start_perf = true;
	for (uint32_t i = 0; i < CORE_CNT - 1; i++) {
		uint64_t vid = i;
		pthread_create(&p_thds[i], NULL, f, (void *)vid);
	}
	f((void *)(CORE_CNT - 1));
	for (uint32_t i = 0; i < CORE_CNT - 1; i++) 
		pthread_join(p_thds[i], NULL);
	// int64_t endtime = get_server_clock();
#if PENALTY_POLICY == 2
	finished = true;
	pthread_join(e, NULL);
#endif
	perf.join();

#if VALVE_ENABLED == 1
	for (int i = 0; i < VALVE_CNT; ++i)
		valves[i].join();
#endif

	if (WORKLOAD != TEST) {
		// printf("PASS! SimTime = %ld\n", endtime - starttime);
		if (STATS_ENABLE)
			stats.print();
	} else {
		((TestWorkload *)m_wl)->summarize();
	}

#if INTERACTIVE_MODE == 1
	delete nexus;
#endif
	return 0;
}

void * f(void * id) {
	uint64_t tid = (uint64_t)id;
	coro_arr = new coro_call_t[CORO_CNT];
	next_coro = new int[CORO_CNT];

#if INTERACTIVE_MODE == 1
	if (is_storage_server) {
		storage_service(tid, replica_cnt);
		return NULL;
	}
#endif

	for (int i = 0; i < CORO_CNT - 1; i++) {
		next_coro[i] = i + 1;
		// coro_arr[i] = coro_call_t(bind(exec, _1, i * CORE_CNT + tid), attributes(fpu_not_preserved));
		coro_arr[i] = coro_call_t(bind(exec, _1, i * CORE_CNT + tid));
		// coro_arr[i] = coro_call_t(bind(exec, _1, tid), attributes(fpu_not_preserved));
	}
	next_coro[CORO_CNT - 1] = 0;
	// coro_arr[CORO_CNT - 1] = coro_call_t(bind(exec, _1, (CORO_CNT - 1) * CORE_CNT + tid), attributes(fpu_not_preserved));
	coro_arr[CORO_CNT - 1] = coro_call_t(bind(exec, _1, (CORO_CNT - 1) * CORE_CNT + tid));
	coro_arr[0]();
	return NULL;
}

void exec(coro_yield_t &yield, int coro_id) {
	// First, just whether I'm a storage server, only valid in interactive mode.
	m_thds[coro_id]->run(yield, coro_id);
}

#if PENALTY_POLICY == 2
void * epoch(void * id) {
	while (!finished) {
		usleep(EPOCH_LENGTH);
		epoch_cnt += 1;
	}
}
#endif

#if INTERACTIVE_MODE == 1
void read_handler(erpc::ReqHandle *req_handle, void *) {
	auto req = req_handle->get_req_msgbuf();
	auto &resp = req_handle->pre_resp_msgbuf;
	// Process the requests.
	ReadRowRequest *r = reinterpret_cast<ReadRowRequest *>(req->buf);
	// printf("read request [%d] (index = %d, key = %lld)\n", req->get_data_size(), r->index_cnt, r->primary_key);
	rpc->resize_msg_buffer(&resp, MAX_TUPLE_SIZE);
	int tuple_size = m_wl->read_row_data(r->index_cnt, r->primary_key, reinterpret_cast<void *>(resp.buf));

	// Send response message.
	rpc->resize_msg_buffer(&resp, tuple_size);
	// sprintf(reinterpret_cast<char *>(resp.buf), "hello");
	rpc->enqueue_response(req_handle, &resp);
}

void write_handler(erpc::ReqHandle *req_handle, void *) {
	auto req = req_handle->get_req_msgbuf();
	auto &resp = req_handle->pre_resp_msgbuf;
	
	// Process the requests.
	WriteRowRequest *r = reinterpret_cast<WriteRowRequest *>(req->buf);
	// printf("write request [%d] (index = %d, key = %lld)\n", req->get_data_size(), r->index_cnt, r->primary_key);
	m_wl->write_row_data(r->index_cnt, r->primary_key, r->size, reinterpret_cast<void *>(r->buf));

	// Send response message.
	rpc->resize_msg_buffer(&resp, 0);
	rpc->enqueue_response(req_handle, &resp);
}

void insert_handler(erpc::ReqHandle *req_handle, void *) {
	auto req = req_handle->get_req_msgbuf();
	auto &resp = req_handle->pre_resp_msgbuf;
	
	// Process the requests.
	InsertRowRequest *r = reinterpret_cast<InsertRowRequest *>(req->buf);
	// printf("write request [%d] (index = %d, key = %lld)\n", req->get_data_size(), r->index_cnt, r->primary_key);
	m_wl->insert_row_data(r->index_cnt, r->primary_key, r->size, reinterpret_cast<void *>(r->buf));

	// Send response message.
	rpc->resize_msg_buffer(&resp, 0);
	rpc->enqueue_response(req_handle, &resp);
}

void remove_handler(erpc::ReqHandle *req_handle, void *) {
	auto req = req_handle->get_req_msgbuf();
	auto &resp = req_handle->pre_resp_msgbuf;
	
	// Process the requests.
	RemoveRowRequest *r = reinterpret_cast<RemoveRowRequest *>(req->buf);
	// printf("write request [%d] (index = %d, key = %lld)\n", req->get_data_size(), r->index_cnt, r->primary_key);
	m_wl->remove_row_data(r->index_cnt, r->primary_key);

	// Send response message.
	rpc->resize_msg_buffer(&resp, 0);
	rpc->enqueue_response(req_handle, &resp);
}

void storage_service(int id, int replica_cnt) {
	set_affinity(id);
	// printf("erpc deamon [%d] started.\n", id);
	rpc = new erpc::Rpc<erpc::CTransport>(nexus, nullptr, id, nullptr);
	rpc->run_event_loop(1000000000);
	printf("...\n");
	delete rpc;
}
#endif
