#include "txn.h"
#include "row.h"
#include "wl.h"
#include "ycsb.h"
#include "thread.h"
#include "mem_alloc.h"
#include "occ.h"
#include "table.h"
#include "catalog.h"
#include "index_btree.h"
#include "index_hash.h"
#include "index_mbtree.h"
#include "log.h"
#include "row_silo.h"
#include "row_lock.h"
#include "row_olock.h"
#include "row_hlock.h"

#define CONFIG_H "silo/config/config-perf.h"

#include <unordered_map>

extern thread_local std::unordered_map<void*, uint64_t>  node_map;

#if INTERACTIVE_MODE == 1
#include "rpc.h"
extern thread_local erpc::Rpc<erpc::CTransport> *rpc;
extern thread_local erpc::MsgBuffer req[N_REPLICAS];
extern thread_local erpc::MsgBuffer resp[N_REPLICAS];
extern thread_local int session_num[N_REPLICAS];
extern thread_local int outstanding_msg_cnt;

// extern __thread int cur_index_cnt;

void cont_func(void *, void *); //{ outstanding_msg_cnt --; }
#endif

void txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	this->h_thd = h_thd;
	this->h_wl = h_wl;

	log = new persistent_log();
	log->init(thd_id);

	node_map.clear();

	pthread_mutex_init(&txn_lock, NULL);
	lock_ready = false;
	ready_part = 0;
	row_cnt = 0;
	wr_cnt = 0;
	rdwr_cnt = 0;
	insert_cnt = 0;
	remove_cnt = 0;
	insert_idx_cnt = 0;
	remove_idx_cnt = 0;
	// inserted = 0;
	// inserted_total = 0;
	// locked = 0;
	// locked_total = 0;
	accesses = (Access **) _mm_malloc(sizeof(Access *) * MAX_ROW_PER_TXN, 64);
	for (int i = 0; i < MAX_ROW_PER_TXN; i++)
		accesses[i] = NULL;
	num_accesses_alloc = 0;
#if CC_ALG == TICTOC || CC_ALG == SILO || CC_ALG == HLOCK
	_pre_abort = (g_params["pre_abort"] == "true");
	if (g_params["validation_lock"] == "no-wait")
		_validation_no_wait = true;
	else if (g_params["validation_lock"] == "waiting")
		_validation_no_wait = false;
	else 
		assert(false);
#endif
#if CC_ALG == TICTOC
	_max_wts = 0;
	_write_copy_ptr = (g_params["write_copy_form"] == "ptr");
	_atomic_timestamp = (g_params["atomic_timestamp"] == "true");
#elif CC_ALG == SILO || CC_ALG == HLOCK
	_cur_tid = 0;
#endif
#if CC_ALG == OLOCK
	entries = malloc(64 * 128);
	n_entry = 0;
#endif
}


void* txn_man::reserve() {
#if CC_ALG == OLOCK
	if (n_entry == 128)
		n_entry = 0;
	n_entry += 1;
	return (void *)((uint64_t)entries + 64 * (n_entry - 1));
#else
	return NULL;
#endif
}

void txn_man::set_txn_id(txnid_t txn_id) {
	this->txn_id = txn_id;
}

txnid_t txn_man::get_txn_id() {
	return this->txn_id;
}

workload * txn_man::get_wl() {
	return h_wl;
}

uint64_t txn_man::get_thd_id() {
	return h_thd->get_thd_id();
}

void txn_man::set_ts(ts_t timestamp) {
	this->timestamp = timestamp;
}

ts_t txn_man::get_ts() {
	return this->timestamp;
}

RC txn_man::apply_index_changes(RC rc) {

	if (rc == RCOK) rc = validate();

	if (rc != RCOK) {
    	// Aborted, remove previously inserted placeholders.
    	for (size_t i = 0; i < insert_idx_cnt; i++) {
			auto idx = insert_idx_idx[i];
			auto key = insert_idx_key[i];
			// auto row = insert_idx_row[i];
			auto part_id = insert_idx_part_id[i];
			auto rc_remove = idx->index_remove(key, part_id);
			// at this time, we still hold the lock of the inserted rows. cleanup will delete these rows.
			assert(rc_remove == RCOK);
			// inserted -= 1;
		}
		insert_idx_cnt = 0;
		return rc;
	}

	// At this time, we can safely commit data.
#if INTERACTIVE_MODE == 1
	// Now we can push data to remote side if using interactive mode.
	for (size_t i = 0; i < insert_idx_cnt; i++) {
		// h_wl->update_index_accessed(insert_idx_idx[i]);
		InsertRowRequest *r =  reinterpret_cast<InsertRowRequest *>(req[0].buf);
		assert(r->index_cnt != -1);
		r->index_cnt = insert_idx_row[i]->index_cnt;
		r->size = insert_idx_row[i]->get_tuple_size();
		r->primary_key = insert_idx_key[i];
		memcpy(r->buf, insert_idx_row[i]->get_data(), insert_idx_row[i]->get_tuple_size());

		rpc->resize_msg_buffer(&req[0], sizeof(InsertRowRequest) - (MAX_TUPLE_SIZE - insert_idx_row[i]->get_tuple_size()));

		for (int i = 0; i < N_REPLICAS; ++i) {
			rpc->enqueue_request(session_num[i], kInsertType, &req[0], &resp[i], cont_func, nullptr);
			outstanding_msg_cnt += 1;
		}

		while (outstanding_msg_cnt > 0) rpc->run_event_loop_once();
	}

	for (size_t i = 0; i < remove_idx_cnt; i++) {
		// h_wl->update_index_accessed(remove_idx_idx[i]);
		RemoveRowRequest *r =  reinterpret_cast<RemoveRowRequest *>(req[0].buf);
		r->index_cnt = h_wl->get_index_cnt(remove_idx_idx[i]);
		assert(r->index_cnt != -1);
		r->primary_key = remove_idx_key[i];

		rpc->resize_msg_buffer(&req[0], sizeof(RemoveRowRequest));

		for (int i = 0; i < N_REPLICAS; ++i) {
			rpc->enqueue_request(session_num[i], kRemoveType, &req[0], &resp[i], cont_func, nullptr);
			outstanding_msg_cnt += 1;
		}

		while (outstanding_msg_cnt > 0) rpc->run_event_loop_once();
	}
#endif

	insert_idx_cnt = 0;

	for (size_t i = 0; i < remove_idx_cnt; i++) {
		auto idx = remove_idx_idx[i];
		auto key = remove_idx_key[i];
		auto part_id = remove_idx_part_id[i];
    // printf("remove_idx idx=%p key=%" PRIu64 " part_id=%d\n", idx, key, part_id);
		auto rc_remove = idx->index_remove(key, part_id);
		assert(rc_remove == RCOK);
	}

	remove_idx_cnt = 0;

	// Free deleted rows
	for (size_t i = 0; i < remove_cnt; i++) {
		auto row = remove_rows[i];
		assert(!row->is_deleted);
		row->is_deleted = 1;
		// We do this only when using RCU.
		// if (RCU_ALLOC) mem_allocator.free(row, row_t::alloc_size(row->get_table()));
	}
	remove_cnt = 0;

	return rc;
}

row_t* txn_man::search(index_base* index, uint64_t key, int part_id,
                        access_t type) {
	itemid_t * item = NULL;
	item = index_read(index, key, part_id);
	if (item == NULL) {
		assert(false);
		return NULL;
	}

	// ((row_t *)item->location)->index_cnt = h_wl->get_index_cnt(index);

	// printf("%lld, %lld\n", ((row_t *)item->location)->index_cnt, h_wl->get_index_cnt(index));
	assert(((row_t *)item->location)->index_cnt == h_wl->get_index_cnt(index));

	// cur_key = key;
	return get_row((row_t *)item->location, type);
}

void txn_man::cleanup(RC rc) {
#if CC_ALG == HEKATON
	row_cnt = 0;
	wr_cnt = 0;
	rdwr_cnt = 0;
	insert_cnt = 0;
	return;
#endif

#if PERSISTENT_LOG == 1 && (CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == WOUND_WAIT || CC_ALG == DLOCK || CC_ALG == OLOCK)
	if (rc != Abort && !readonly && !read_committed) {
		// Log TX Begin.
		log->log_tx_meta(get_txn_id(), wr_cnt + rdwr_cnt);

		for (int rid = row_cnt - 1; rid >= 0; rid --) {
			if (accesses[rid]->type == WR || accesses[rid]->type == RDWR)
				log->log_content(accesses[rid]->orig_row->get_primary_key(), 
					accesses[rid]->orig_row->get_data(), 
					accesses[rid]->orig_row->get_tuple_size());
		}
	}
#endif

	if (CC_ALG == OLOCK || CC_ALG == DLOCK)
		this->ex_mode = true;
	
	for (int rid = row_cnt - 1; rid >= 0; rid --) {
		row_t * orig_r = accesses[rid]->orig_row;
		access_t type = accesses[rid]->type;
		if (type == WR && rc == Abort)
			type = XP;
#if CC_ALG == HLOCK
		if ((type == RD || type == RDWR) && rc == Abort && !readonly && !read_committed) {
			orig_r->clean_hlock(this);
		}
#endif

#if (CC_ALG == NO_WAIT || CC_ALG == DL_DETECT) && ISOLATION_LEVEL == REPEATABLE_READ
		if (type == RD) {
			accesses[rid]->data = NULL;
			continue;
		}
#endif

		if (ROLL_BACK && type == XP &&
					(CC_ALG == DL_DETECT || 
					CC_ALG == NO_WAIT || 
					CC_ALG == WAIT_DIE || 
					CC_ALG == WOUND_WAIT ||
					CC_ALG == OLOCK)) {
			// Aborted TX with 2PL running at one-shot mode needs Roll-back.
			if (INTERACTIVE_MODE == 0) {
				orig_r->return_row(type, this, accesses[rid]->orig_data);
			} else {
				orig_r->return_row(type, this, NULL);
			}
		} else {
			assert(accesses[rid]->data->table != NULL);
			orig_r->return_row(type, this, accesses[rid]->data);
		}

#if CC_ALG != TICTOC && CC_ALG != SILO && CC_ALG != HLOCK && CC_ALG != DLOCK && INTERACTIVE_MODE == 0
		accesses[rid]->data = NULL;
#endif
	}

	if (rc == Abort && !readonly && read_committed) {
		for (UInt32 i = 0; i < insert_cnt; i ++) {
			row_t * row = insert_rows[i];
			row->is_deleted = 1;
		
			asm volatile ("sfence" ::: "memory");

		#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == WOUND_WAIT || CC_ALG == DLOCK
			auto rc = row->manager->lock_release((lock_t)LOCK_EX, this);
			assert(rc == RCOK);
		#elif CC_ALG == SILO
			row->manager->release();
		#elif CC_ALG == HLOCK
			row->manager->unlock_wr(this);
		#else
			assert(false);
		#endif
#if CC_ALG != HSTORE && CC_ALG != OCC
			mem_allocator.free(row->manager, 0);
#endif
			row->free_row();
			mem_allocator.free(row, sizeof(row));
		}
	} else if (!readonly && !read_committed) {
		for (UInt32 i = 0; i < insert_cnt; i ++) {
			row_t * row = insert_rows[i];
		#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == WOUND_WAIT || CC_ALG == DLOCK
      		auto rc = row->manager->lock_release((lock_t)LOCK_EX, this);
      		assert(rc == RCOK);
		#elif CC_ALG == SILO || CC_ALG == HLOCK
      		// Unlocking new rows is done in validate_*() to initialize row TID.
      		(void)row;
		#else
			// Not implemented.
			assert(false);
		#endif
    	}
	}

	if (PERSISTENT_LOG >= 1 && rc != Abort && !readonly && !read_committed)
		log->log_end();

	row_num_last_tx =  (rc == RCOK) ? 0 : row_cnt;

	row_cnt = 0;
	wr_cnt = 0;
	rdwr_cnt = 0;
	insert_cnt = 0;
	remove_cnt = 0;
	
	insert_idx_cnt = 0;
	remove_idx_cnt = 0;
	node_map.clear();

#if CC_ALG == DL_DETECT
	dl_detector.clear_dep(get_txn_id());
#endif
}

row_t * txn_man::get_row(row_t * row, access_t type) {
	// printf("key = %lld, idx_cnt = %d\n", row->get_primary_key(), row->index_cnt);
#if PERSISTENT_LOG == 2
	if (row_cnt == 0 && !readonly && !read_committed) {
		log->log_tx_meta(get_txn_id(), 0);
	}
#endif
	/* 
	Accessed items are warpped inside Access structure, 
	Some of them need to make a local copy (e.g., OCC, write-set in 2PL, etc), 
	while others can be directly refered to.
	*/
	if (wound) {
		// lock_cnt += 1;
		// wound_cnt_discovered1 += 1;
		// printf("%d wounded-1, cnt = %d\n", get_thd_id(), wound_cnt);
		// return NULL;
	}
	if (CC_ALG == HSTORE)
		return row;
	uint64_t starttime = get_sys_clock();
	RC rc = RCOK;
	assert(row_cnt < MAX_ROW_PER_TXN);
	if (accesses[row_cnt] == NULL) {
		Access * access = (Access *) _mm_malloc(sizeof(Access), 64);
		accesses[row_cnt] = access;
#if (CC_ALG == SILO || CC_ALG == TICTOC || CC_ALG == DLOCK || CC_ALG == HLOCK)
		access->data = (row_t *) _mm_malloc(sizeof(row_t), 64);
		access->data->init(MAX_TUPLE_SIZE);
		access->orig_data = (row_t *) _mm_malloc(sizeof(row_t), 64);
		access->orig_data->init(MAX_TUPLE_SIZE);
		access->data->data_bak = NULL;
#elif (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == WOUND_WAIT || CC_ALG == OLOCK)
#if INTERACTIVE_MODE == 1
		access->data = (row_t *) _mm_malloc(sizeof(row_t), 64);
		access->data->init(MAX_TUPLE_SIZE);
#endif
		access->orig_data = (row_t *) _mm_malloc(sizeof(row_t), 64);
		access->orig_data->init(MAX_TUPLE_SIZE);
#endif
		num_accesses_alloc ++;
	}
	/* 
	Create a local copy in *data* if necessary.
	Locks are acquired here, except OCC (validate in cleanup).
	*/
#if INTERACTIVE_MODE == 0 && (CC_ALG == DLOCK || CC_ALG == HLOCK)
	if (accesses[row_cnt]->data->data_bak != NULL) {
		// access has been used for read, *data point to a invalid location.
		accesses[row_cnt]->data->unref();
	}
#endif

	if (row->is_deleted) {
		return NULL;
	}

	// owner_before = owner_cur = 0;
	// lock_addr = 0;

	// if (lock_print) {
	// 	row->manager->do_print = true;
	// }
	
	rc = row->get_row(type, this, accesses[ row_cnt ]->data);

	if (read_committed && type == WR)
		assert(false);

	// if (accesses[row_cnt]->data != NULL && accesses[row_cnt]->data->table == NULL) {
	// 	// it can be NULL: for interactive mode or in DLOCK.
	// 	accesses[row_cnt]->data->set_primary_key(row->get_primary_key());
	// 	accesses[row_cnt]->data->table = row->get_table();
	// }

	wait_cycles(WAIT_CYCLE);

	if (rc == Abort) {
		return NULL;
	}
	if (row->is_deleted) { // safe: already deleted, lock is acquired but invalid.
		return NULL;
	}

	// if ((rc == RCOK && lock_print) || row->manager->do_print) {
	// 	lock_print = false;
	// 	if (type == RD) {
	// 		printf("RD  txn=%d get the lock of row=%p, path = %d, cur_owner = %d, (%d->%d, at %p).\n", get_thd_id(), 
	// 		row, row->manager->path, row->manager->owner.tid, owner_before, owner_cur, lock_addr);
	// 	} else {
	// 		printf("WR txn=%d get the lock of row=%p, path = %d, cur_owner = %d, (%d->%d, at %p).\n", get_thd_id(), 
	// 		row, row->manager->path, row->manager->owner.tid, owner_before, owner_cur, lock_addr);
	// 	}
	// }

	accesses[row_cnt]->type = type;
	accesses[row_cnt]->orig_row = row;
	// if (type == WR && accesses[row_cnt]->orig_row->get_primary_key() == 11) {
	// 	// printf("%d locked.\n", get_thd_id());
	// 	locked += 1;
	// 	locked_total += 1;
	// }
#if CC_ALG == TICTOC
	accesses[row_cnt]->wts = last_wts;
	accesses[row_cnt]->rts = last_rts;
#elif CC_ALG == SILO
	accesses[row_cnt]->tid = last_tid;
#elif CC_ALG == HEKATON
	accesses[row_cnt]->history_entry = history_entry;
#endif

#if CC_ALG == HLOCK || CC_ALG == DLOCK
	accesses[row_cnt]->tid = row->get_version();
#endif

#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == WOUND_WAIT || CC_ALG == OLOCK)
	if (type == WR && INTERACTIVE_MODE == 0) {
		accesses[row_cnt]->orig_data->table = row->get_table();
		accesses[row_cnt]->orig_data->copy(row);
	}
#endif

#if PERSISTENT_LOG == 2
	if ((type == WR || type == RDWR) && !readonly)
		log->log_content(accesses[row_cnt]->orig_row->get_primary_key(), 
					accesses[row_cnt]->orig_row->get_data(), 
					accesses[row_cnt]->orig_row->get_tuple_size());
#endif

#if (CC_ALG == NO_WAIT || CC_ALG == DL_DETECT) && ISOLATION_LEVEL == REPEATABLE_READ
	if (type == RD)
		row->return_row(type, this, accesses[ row_cnt ]->data);
#endif
	
	row_cnt ++;
	if (type == WR)
		wr_cnt ++;

	if (type == RDWR)
		rdwr_cnt ++;

	uint64_t timespan = get_sys_clock() - starttime;
	INC_TMP_STATS(get_thd_id(), time_man, timespan);
	return accesses[row_cnt - 1]->data;
}

bool txn_man::insert_row(row_t * &row, table_t * table, int part_id, uint64_t& out_row_id) {
	if (CC_ALG == HSTORE)
		return false;
	if (table->get_new_row(row, part_id, out_row_id) != RCOK) return false;
	assert(insert_cnt < MAX_ROW_PER_TXN);
	insert_rows[insert_cnt ++] = row;
#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == WOUND_WAIT || CC_ALG == DLOCK
	auto rc = row->manager->lock_get((lock_t)LOCK_EX, this);
  	assert(rc == RCOK);
#elif CC_ALG == SILO
	row->manager->lock();
#elif CC_ALG == HLOCK
	row->manager->lock_wr(this);
#else
  // Not implemented.
  assert(false);
#endif
  return true;
}

bool txn_man::remove_row(row_t* row) {
	remove_rows[remove_cnt++] = row;
	return true;
}

itemid_t *
txn_man::index_read(index_base * index, idx_key_t key, int part_id) {
	// h_wl->update_index_accessed(index);
	uint64_t starttime = get_sys_clock();
	itemid_t * item = NULL;
	index->index_read(key, item, part_id, get_thd_id());
	INC_TMP_STATS(get_thd_id(), time_index, get_sys_clock() - starttime);
	return item;
}

void 
txn_man::index_read(index_base * index, idx_key_t key, int part_id, itemid_t *& item) {
	// h_wl->update_index_accessed(index);
	uint64_t starttime = get_sys_clock();
	index->index_read(key, item, part_id, get_thd_id());
	INC_TMP_STATS(get_thd_id(), time_index, get_sys_clock() - starttime);
}

RC
txn_man::index_read_multiple(index_base* index, idx_key_t key, itemid_t** items, size_t& count, int part_id) {
	return index->index_read_multiple(key, items, count, part_id);
}

RC
txn_man::index_read_range(index_base* index, idx_key_t min_key, idx_key_t max_key, itemid_t** items, size_t& count, int part_id) {
	return index->index_read_range(min_key, max_key, items, count, part_id);
}

RC
txn_man::index_read_range_rev(index_base* index, idx_key_t min_key, idx_key_t max_key, itemid_t** items, size_t& count, int part_id) {
	return index->index_read_range_rev(min_key, max_key, items, count, part_id);
}

bool txn_man::insert_idx(index_base* index, uint64_t key, row_t* row, int part_id) {
	// h_wl->update_index_accessed(index);
	// Insert in advance, at this point, row has already been locked.
	
	row->index_cnt = h_wl->get_index_cnt(index);

	itemid_t * m_item =
		(itemid_t *) mem_allocator.alloc( sizeof(itemid_t), part_id);
	m_item->init();
	m_item->type = DT_row;
	m_item->location = row;
	m_item->valid = true;

	auto rc_insert = index->index_insert(key, m_item, part_id); // May fail if others also insert one.

	if (rc_insert != RCOK) {
    	return false;
	}

	assert(insert_idx_cnt < MAX_ROW_PER_TXN);

	insert_idx_idx[insert_idx_cnt] = index;
	insert_idx_key[insert_idx_cnt] = key;
	insert_idx_row[insert_idx_cnt] = row;
	insert_idx_part_id[insert_idx_cnt] = part_id;
	insert_idx_cnt++;
	// inserted += 1;
	// inserted_total += 1;
  return true;
}

bool txn_man::remove_idx(index_base* index, uint64_t key, row_t* row, int part_id) {
	(void)row;
	assert(remove_idx_cnt < MAX_ROW_PER_TXN);
	remove_idx_idx[remove_idx_cnt] = index;
	remove_idx_key[remove_idx_cnt] = key;
	remove_idx_part_id[remove_idx_cnt] = part_id;
	remove_idx_cnt++;
	return true;
}

RC txn_man::finish(RC rc) {
#if CC_ALG == HSTORE
	return RCOK;
#endif
	uint64_t starttime = get_sys_clock();
#if CC_ALG == OCC
	if (rc == RCOK)
		rc = occ_man.validate(this);
	else 
		cleanup(rc);
#elif CC_ALG == TICTOC
	if (rc == RCOK)
		rc = validate_tictoc();
	else 
		cleanup(rc);
#elif CC_ALG == SILO
	if (rc == RCOK) {
		rc = validate_silo();
	} else { 
		rc = apply_index_changes(rc);
		cleanup(rc);
	}
#elif CC_ALG == DLOCK
	if (rc == RCOK && !readonly && !read_committed) {
		ts_t wait_start = get_sys_clock();
		// validate all the rows with write locks.
		for (int rid = row_cnt - 1; rid >= 0; rid --) {
			if (accesses[rid]->type == WR) {
				rc = accesses[rid]->orig_row->manager->validate(this);
				if (rc == Abort)
					break;
			}
		}
		ts_t wait_end = get_sys_clock();
		if (PRINT_LAT_DEBUG && get_thd_id() == 0) {
			last_waiting_time += wait_end - wait_start; // ns
		}
	} else if (rc == RCOK && readonly && !read_committed) {
		for (int i = 0; i < row_cnt; i++) {
			if (accesses[i]->orig_row->get_version() != accesses[i]->tid) {
				rc = Abort;
				break;
			}
		}
	}
	if (!readonly && !read_committed)
		rc = apply_index_changes(rc);
	asm volatile ("sfence" ::: "memory");
	cleanup(rc);
#elif CC_ALG == HLOCK
	if (rc == RCOK) {
		rc = validate_hlock();
	} else {
		if (!readonly && !read_committed)
			rc = apply_index_changes(rc);
		cleanup(rc);
	}
#elif CC_ALG == HEKATON
	rc = validate_hekaton(rc);
	cleanup(rc);
#else 
	rc = apply_index_changes(rc);
	cleanup(rc);
#endif
	uint64_t timespan = get_sys_clock() - starttime;
	INC_TMP_STATS(get_thd_id(), time_man,  timespan);
	INC_STATS(get_thd_id(), time_cleanup,  timespan);
	return rc;
}

void
txn_man::release() {
	for (int i = 0; i < num_accesses_alloc; i++)
		mem_allocator.free(accesses[i], 0);
	mem_allocator.free(accesses, 0);
}

RC
txn_man::validate() {
  for (auto it : node_map) {
    if (IndexMBTree::extract_version(it.first) != it.second) {
      return Abort;
    }
  }
  return RCOK;
}