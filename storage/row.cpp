#include <mm_malloc.h>
#include "global.h"
#include "table.h"
#include "catalog.h"
#include "row.h"
#include "txn.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "row_hekaton.h"
#include "row_occ.h"
#include "row_tictoc.h"
#include "row_silo.h"
#include "row_vll.h"
#include "mem_alloc.h"
#include "manager.h"
#include "wl.h"
#include "row_olock.h"
#include "row_hlock.h"
#include "row_mocc.h"
#include <math.h>

#if INTERACTIVE_MODE == 1
#include "rpc.h"
extern thread_local erpc::Rpc<erpc::CTransport> *rpc;
extern thread_local erpc::MsgBuffer req[N_REPLICAS];
extern thread_local erpc::MsgBuffer resp[N_REPLICAS];
extern thread_local int session_num[N_REPLICAS];
extern thread_local int outstanding_msg_cnt;

// extern __thread int cur_index_cnt;

void cont_func(void *, void *) { outstanding_msg_cnt --; }
#endif


extern __thread int *next_coro;
extern __thread coro_call_t *coro_arr;
extern workload * m_wl;

RC 
row_t::init(table_t * host_table, uint64_t part_id, uint64_t row_id) {
	_row_id = row_id;
	_part_id = part_id;
	this->version = 0;
	this->table = host_table;
	this->is_deleted = false;
#if CC_ALG == MOCC
	this->temperature = 0;
#endif
	Catalog * schema = host_table->get_schema();
	int tuple_size = schema->get_tuple_size();
	data = (char *) _mm_malloc(sizeof(char) * tuple_size, 64);
	return RCOK;
}
void 
row_t::init(int size) 
{
	data = (char *) _mm_malloc(size, 64);
	this->table = NULL;
	this->is_deleted = false;
	this->version = 0;
}

RC 
row_t::switch_schema(table_t * host_table) {
	this->table = host_table;
	return RCOK;
}

void row_t::init_manager(row_t * row) {
#if CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == WOUND_WAIT
    manager = (Row_lock *) mem_allocator.alloc(sizeof(Row_lock), _part_id);
#elif CC_ALG == OLOCK
	manager = (Row_olock *) mem_allocator.alloc(sizeof(Row_dlock), _part_id);
#elif CC_ALG == PLOR
	manager = (Row_dlock *) mem_allocator.alloc(sizeof(Row_dlock), _part_id);
#elif CC_ALG == TIMESTAMP
    manager = (Row_ts *) mem_allocator.alloc(sizeof(Row_ts), _part_id);
#elif CC_ALG == MVCC
    manager = (Row_mvcc *) _mm_malloc(sizeof(Row_mvcc), 64);
#elif CC_ALG == HEKATON
    manager = (Row_hekaton *) _mm_malloc(sizeof(Row_hekaton), 64);
#elif CC_ALG == OCC
    manager = (Row_occ *) mem_allocator.alloc(sizeof(Row_occ), _part_id);
#elif CC_ALG == TICTOC
	manager = (Row_tictoc *) _mm_malloc(sizeof(Row_tictoc), 64);
#elif CC_ALG == SILO
	manager = (Row_silo *) _mm_malloc(sizeof(Row_silo), 64);
#elif CC_ALG == MOCC
	manager = (Row_mocc *)  _mm_malloc(sizeof(Row_mocc), 64);
#elif CC_ALG == HLOCK
	manager = (Row_hlock *) _mm_malloc(sizeof(Row_hlock), 64);
#elif CC_ALG == VLL
    manager = (Row_vll *) mem_allocator.alloc(sizeof(Row_vll), _part_id);
#endif

#if CC_ALG != HSTORE
	manager->init(this);
#endif
}

table_t * row_t::get_table() { 
	return table; 
}

Catalog * row_t::get_schema() { 
	return get_table()->get_schema(); 
}

const char * row_t::get_table_name() { 
	return get_table()->get_table_name(); 
};
uint64_t row_t::get_tuple_size() {
	return get_schema()->get_tuple_size();
}

uint64_t row_t::get_field_cnt() { 
	return get_schema()->field_cnt;
}

void row_t::set_value(int id, void * ptr) {
	int datasize = get_schema()->get_field_size(id);
	int pos = get_schema()->get_field_index(id);
	memcpy( &data[pos], ptr, datasize);
}

void row_t::set_value(int id, void * ptr, int size) {
	int pos = get_schema()->get_field_index(id);
	memcpy( &data[pos], ptr, size);
}

void row_t::set_value(const char * col_name, void * ptr) {
	uint64_t id = get_schema()->get_field_id(col_name);
	set_value(id, ptr);
}

SET_VALUE(uint64_t);
SET_VALUE(int64_t);
SET_VALUE(double);
SET_VALUE(UInt32);
SET_VALUE(SInt32);

GET_VALUE(uint64_t);
GET_VALUE(int64_t);
GET_VALUE(double);
GET_VALUE(UInt32);
GET_VALUE(SInt32);

void row_t::get_value_bak(int col_id, int64_t & value) {
		int pos = get_schema()->get_field_index(col_id);
		value = *(int64_t *)&data[pos];
}

char * row_t::get_value(int id) {
	int pos = get_schema()->get_field_index(id);
	return &data[pos];
}

char * row_t::get_value(char * col_name) {
	uint64_t pos = get_schema()->get_field_index(col_name);
	return &data[pos];
}

char * row_t::get_data() { return data; }

void row_t::set_data(row_t *src, uint64_t size) {
	char *data = src->get_data();
	// ASSERT(this->data != data);
#if INTERACTIVE_MODE == 1
	// Remote read ...
	ReadRowRequest *r =  reinterpret_cast<ReadRowRequest *>(req[0].buf);
	r->index_cnt = src->index_cnt;
	assert(r->index_cnt != -1);
	r->primary_key = src->get_primary_key();

	rpc->resize_msg_buffer(&req[0], sizeof(ReadRowRequest));

	rpc->enqueue_request(session_num[0], kReadType, &req[0], &resp[0], cont_func, nullptr);
	outstanding_msg_cnt += 1;

	while (outstanding_msg_cnt > 0) rpc->run_event_loop_once();

	// Copy to original place.
	// assert(resp[0].get_data_size() == size);
	// memcpy(this->data, (void *)resp[0].buf, size);
	memcpy(this->data, data, size);
#else
	memcpy(this->data, data, size);
#endif
}

// copy from the src to this
void row_t::copy(row_t * src) {
	assert(src->get_table() != NULL);
	assert(src->get_table()->get_schema() != NULL);
	set_data(src, src->get_tuple_size());
}

void row_t::remote_write(row_t * src) {
	// Remote write ...
#if INTERACTIVE_MODE == 1
	WriteRowRequest *r =  reinterpret_cast<WriteRowRequest *>(req[0].buf);
	r->index_cnt = this->index_cnt;
	assert(r->index_cnt != -1);
	r->size = this->get_tuple_size();
	r->primary_key = this->get_primary_key();
	memcpy(r->buf, src, this->get_tuple_size());

	rpc->resize_msg_buffer(&req[0], sizeof(WriteRowRequest) - (MAX_TUPLE_SIZE - this->get_tuple_size()));

	for (int i = 0; i < N_REPLICAS; ++i) {
		rpc->enqueue_request(session_num[i], kWriteType, &req[0], &resp[i], cont_func, nullptr);
		outstanding_msg_cnt += 1;
	}

	while (outstanding_msg_cnt > 0) rpc->run_event_loop_once();

	// Copy to original place.
	memcpy(this->data, src->get_data(), this->get_tuple_size());
#endif
}

void row_t::ref(row_t *src) {
	data_bak = data;
	data = src->get_data();
}

void row_t::unref() {
	data = data_bak;
	data_bak = NULL;
}

#if CC_ALG == HLOCK
void 
row_t::clean_hlock(txn_man *txn) {
	if (manager->is_locked_rd(txn)) {
		manager->unlock_rd(txn);
	}
}
#endif

void row_t::free_row() {
	free(data);
}

RC row_t::get_row(access_t type, txn_man * txn, row_t *& row) {
	RC rc = RCOK;
	txn->conflict_type = 0;
	if (txn->wound)
		return Abort;
#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT || CC_ALG == WOUND_WAIT || CC_ALG == OLOCK || CC_ALG == PLOR
	if (txn->read_committed)
		txn->wound = false; // I just read, you don't need to kill me.
	uint64_t thd_id = txn->get_thd_id();
	lock_t lt = (type == RD || type == SCAN) ? (lock_t)LOCK_SH : (lock_t)LOCK_EX;
	txn->lock_ready = false;
#if CC_ALG == DL_DETECT
	uint64_t * txnids;
	int txncnt; 
	rc = this->manager->lock_get(lt, txn, txnids, txncnt);	
#else
	rc = this->manager->lock_get(lt, txn);
#endif

	if (txn->read_committed && CC_ALG == PLOR && rc != RCOK) {
		std::cout << txn->wound << std::endl;
		assert(rc == RCOK);
	}

	if (rc == RCOK) {
	#if INTERACTIVE_MODE == 0
		if (CC_ALG != PLOR) {
			row = this;
		} else { // PLOR w/ one-shot TX mode
			if (type == WR) {
				row->copy(this);
			} else {
				row->ref(this); // only change the pointer.
			}
			row->table = this->get_table();
			row->set_primary_key(this->get_primary_key());
			row->index_cnt = this->index_cnt;
		}
	#else
		assert(row != NULL);
		row->table = this->get_table();
		row->set_primary_key(this->get_primary_key());
		row->index_cnt = this->index_cnt;
		row->copy(this);
	#endif
		if (txn->read_committed) {
			this->manager->lock_release(lt, txn);
		}
	} else if (rc == Abort) {} 
	else if (rc == WAIT) {
		ASSERT(CC_ALG == WAIT_DIE || CC_ALG == DL_DETECT || CC_ALG == WOUND_WAIT || CC_ALG == OLOCK || CC_ALG == PLOR);
		// if (CC_ALG == WOUND_WAIT || CC_ALG == WAIT_DIE) {
		// 	assert(txn->conflict_type != 0);
		if (CC_ALG == PLOR) {
			assert(txn->conflict_type == 2);
		}
		uint64_t starttime = get_sys_clock();
#if CC_ALG == DL_DETECT	
		bool dep_added = false;
#endif
		uint64_t endtime;
		txn->lock_abort = false;
		INC_STATS(txn->get_thd_id(), wait_cnt, 1);
		ts_t wait_start = get_sys_clock();
		while (!txn->lock_ready && !txn->lock_abort && !txn->wound && !this->is_deleted) {
#if CC_ALG == WAIT_DIE 
			continue;
#elif CC_ALG == WOUND_WAIT
			continue;
#elif CC_ALG == OLOCK || CC_ALG == PLOR
			this->manager->poll_lock_state(txn);
			continue;
#elif CC_ALG == DL_DETECT
			uint64_t last_detect = starttime;
			uint64_t last_try = starttime;

			uint64_t now = get_sys_clock();
			if (now - starttime > g_timeout ) {
				txn->lock_abort = true;
				break;
			}
			if (g_no_dl) {
				PAUSE
				continue;
			}
			int ok = 0;
			if ((now - last_detect > g_dl_loop_detect) && (now - last_try > DL_LOOP_TRIAL)) {
				if (!dep_added) {
					ok = dl_detector.add_dep(txn->get_txn_id(), txnids, txncnt, txn->row_cnt);
					if (ok == 0)
						dep_added = true;
					else if (ok == 16)
						last_try = now;
				}
				if (dep_added) {
					ok = dl_detector.detect_cycle(txn->get_txn_id());
					if (ok == 16)  // failed to lock the deadlock detector
						last_try = now;
					else if (ok == 0) 
						last_detect = now;
					else if (ok == 1) {
						last_detect = now;
					}
				}
			} else 
				PAUSE
#endif
		}

		ts_t wait_end = get_sys_clock();
		if (PRINT_LAT_DEBUG && txn->get_thd_id() == 0) {
			if (txn->conflict_type == 1) {
				last_waiting_time_1 += wait_end - wait_start; // ns
			} else if (txn->conflict_type == 2) {
				last_waiting_time_2 += wait_end - wait_start; // ns
			}
		}

		if (txn->lock_ready) {
			if (txn->read_committed) {
				this->manager->lock_release(lt, txn);
			}
			rc = RCOK;
		}
		else if (txn->lock_abort || this->is_deleted) {
			rc = Abort;
			if (txn->read_committed) {
				this->manager->lock_release(lt, txn);
			} else {
				return_row(type, txn, NULL);
			}
		} else if (txn->wound) {
		#ifdef DEBUG_WOUND
			txn->wound_cnt_discovered +=1;
			// printf("%d wounded cnt = %d. \n", (int)txn->get_thd_id(), (int)txn->wound_cnt);
		#endif
			rc = Abort;
			if (txn->read_committed) {
				this->manager->lock_release(lt, txn);
			} else {
				return_row(type, txn, NULL);
			}
		}
		endtime = get_sys_clock();
		INC_TMP_STATS(thd_id, time_wait, endtime - starttime);
	#if INTERACTIVE_MODE == 0
		if (CC_ALG != PLOR) {
			row = this;
		} else if (rc == RCOK) { // PLOR w/ one-shot mode.
			if (type == WR) {
				row->copy(this);
			} else {
				row->ref(this);
			}
			row->table = this->get_table();
			row->index_cnt = this->index_cnt;
			row->set_primary_key(this->get_primary_key());
		}
	#else
		if (rc == RCOK) {
			assert(row != NULL);
			row->table = this->get_table();
			row->set_primary_key(this->get_primary_key());
			row->index_cnt = this->index_cnt;
			row->copy(this);
		}
	#endif
	}
	return rc;
#elif CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == HEKATON 
	uint64_t thd_id = txn->get_thd_id();
	// For TIMESTAMP RD, a new copy of the row will be returned.
	// for MVCC RD, the version will be returned instead of a copy
	// So for MVCC RD-WR, the version should be explicitly copied.
	//row_t * newr = NULL;
  #if CC_ALG == TIMESTAMP
	// TODO. should not call malloc for each row read. Only need to call malloc once 
	// before simulation starts, like TicToc and Silo.
	txn->cur_row = (row_t *) mem_allocator.alloc(sizeof(row_t), this->get_part_id());
	txn->cur_row->init(get_table(), this->get_part_id());
  #endif

	// TODO need to initialize the table/catalog information.
	TsType ts_type = (type == RD)? R_REQ : P_REQ; 
	rc = this->manager->access(txn, ts_type, row);
	if (rc == RCOK ) {
		row = txn->cur_row;
	} else if (rc == WAIT) {
		uint64_t t1 = get_sys_clock();
		while (!txn->ts_ready)
			PAUSE
		uint64_t t2 = get_sys_clock();
		INC_TMP_STATS(thd_id, time_wait, t2 - t1);
		row = txn->cur_row;
	}
	if (rc != Abort) {
		row->table = get_table();
		assert(row->get_schema() == this->get_schema());
	}
	return rc;
#elif CC_ALG == OCC
	// OCC always make a local copy regardless of read or write, no need for locking.
	txn->cur_row = (row_t *) mem_allocator.alloc(sizeof(row_t), get_part_id());
	txn->cur_row->init(get_table(), get_part_id());
	// access makes a local copy (i.e., cur_row)
	// The manager is initialized when this row was created
	rc = this->manager->access(txn, R_REQ);
	// This is a new copy just made.
	row = txn->cur_row;
	return rc;
#elif CC_ALG == TICTOC || CC_ALG == SILO || CC_ALG == HLOCK || CC_ALG == MOCC
	// like OCC, tictoc also makes a local copy for each read/write
	row->table = get_table();
	row->set_primary_key(get_primary_key());
	row->index_cnt = this->index_cnt;
	TsType ts_type = (type == RD) ? R_REQ : ((type == RDWR) ? RW_REQ : P_REQ);
	rc = this->manager->access(txn, ts_type, row);
	return rc;
#elif CC_ALG == HSTORE || CC_ALG == VLL
	row = this;
	return rc;
#else
	assert(false);
#endif
}

// the "row" is the row read out in get_row(). 
// For locking based CC_ALG, the "row" is the same as "this". 
// For timestamp based CC_ALG, the "row" != "this", and the "row" must be freed.
// For MVCC, the row will simply serve as a version. The version will be 
// delete during history cleanup.
// For TIMESTAMP, the row will be explicity deleted at the end of access().
// (cf. row_ts.cpp)
void row_t::return_row(access_t type, txn_man * txn, row_t * row) {
	if (txn->read_committed) // Lock has already been released.
		return;
#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT || CC_ALG == WOUND_WAIT || CC_ALG == OLOCK
#if INTERACTIVE_MODE == 0
	assert (row == NULL || row == this || type == XP);
#endif
	if (ROLL_BACK && type == XP && INTERACTIVE_MODE == 0) {// recover from previous writes.
		wait_cycles(WAIT_CYCLE);
		this->copy(row);
	}

	if (INTERACTIVE_MODE == 1 && type == WR && row != NULL) {
		// commit.
		// this->copy(row);
		this->remote_write(row);
	}

	lock_t lt = (type == WR || type == XP) ? (lock_t)LOCK_EX : (lock_t)LOCK_SH;

	this->manager->lock_release(lt, txn);

#elif CC_ALG == PLOR
	lock_t lt = (type == WR || type == XP) ? (lock_t)LOCK_EX : (lock_t)LOCK_SH;
	if (type == WR && row) {
		// commit data.
	#if INTERACTIVE_MODE == 1
		this->remote_write(row);
	#else
		wait_cycles(WAIT_CYCLE);
		this->copy(row);
	#endif
		this->increase_version();
	}
	this->manager->lock_release(lt, txn);

#elif CC_ALG == TIMESTAMP || CC_ALG == MVCC
	// for RD or SCAN or XP, the row should be deleted.
	// because all WR should be companied by a RD
	// for MVCC RD, the row is not copied, so no need to free. 
  #if CC_ALG == TIMESTAMP
	if (type == RD || type == SCAN) {
		row->free_row();
		mem_allocator.free(row, sizeof(row_t));
	}
  #endif
	if (type == XP) {
		this->manager->access(txn, XP_REQ, row);
	} else if (type == WR) {
		assert (type == WR && row != NULL);
		assert (row->get_schema() == this->get_schema());
		RC rc = this->manager->access(txn, W_REQ, row);
		assert(rc == RCOK);
	}
#elif CC_ALG == OCC
	assert (row != NULL);
	if (type == WR)
		manager->write( row, txn->end_ts );
	row->free_row();
	mem_allocator.free(row, sizeof(row_t));
	return;
#elif CC_ALG == TICTOC || CC_ALG == SILO || CC_ALG == HLOCK || CC_ALG == MOCC
	assert (row != NULL);
	return;
#elif CC_ALG == HSTORE || CC_ALG == VLL
	return;
#else 
	assert(false);
#endif
}

#if CC_ALG == MOCC
void row_t::increase_temperature() {
	while (true) {
		double temp_old = temperature;
		double temp_new = temp_old + pow(2, -temp_old);
		if (temperature.compare_exchange_strong(temp_old, temp_new))
			break;
		asm volatile ("lfence" ::: "memory");
	};
}

double row_t::get_temperature() {
	return temperature;
}

#endif 