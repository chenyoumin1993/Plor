#include "global.h"
#include "helper.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"

// void micro_sleep(int us) {
// 	ts_t T1 = T2 = get_sys_clock();
// 	while (((T2 - T1) / 1000) < us)
// 		T2 = get_sys_clock();
// }

void ycsb_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	txn_man::init(h_thd, h_wl, thd_id);
	_wl = (ycsb_wl *) h_wl;
}

RC ycsb_txn_man::run_txn(base_query * query) {
	RC rc;
	ycsb_query * m_query = (ycsb_query *) query;
	ycsb_wl * wl = (ycsb_wl *) h_wl;
	itemid_t * m_item = NULL;
  	row_cnt = 0;

	ts_t starttime, endtime;

	starttime = get_sys_clock();

	for (uint32_t rid = 0; rid < m_query->request_cnt; rid ++) {
		ycsb_request * req = &m_query->requests[rid];
		int part_id = wl->key_to_part( req->key );
		bool finish_req = false;
		UInt32 iteration = 0;
		while ( !finish_req ) { 
			if (iteration == 0) { // To emulate scan (simply repeat for N times)
				// Find the item from the index
				m_item = index_read(_wl->the_index, req->key, part_id);
			}
#if INDEX_STRUCT == IDX_BTREE
			else {
				_wl->the_index->index_next(get_thd_id(), m_item);
				if (m_item == NULL)
					break;
			}
#endif
			row_t * row = ((row_t *)m_item->location);
			row_t * row_local; 
			access_t type = req->rtype;
			
			// Make a local copy and lock the item if necessary.
			row_local = get_row(row, type);
			if (row_local == NULL) {
				rc = Abort;
				goto final;
			}

			// Computation //
			// Only do computation when there are more than 1 requests.
            if (m_query->request_cnt > 1) {
                if (req->rtype == RD || req->rtype == SCAN) {
//                  for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
						int fid = 0;
						char * data = row_local->get_data();
						__attribute__((unused)) uint64_t fval = *(uint64_t *)(&data[fid * 10]);
//                  }
                } else {
					// printf("---%d\n", req->rtype);
                    assert(req->rtype == WR);
					// printf("+++%d\n", req->rtype);
					// int size = _wl->the_table->schema->get_tuple_size();
					// for (int off = 0; off < size; off += 64) {
					// 	char * data = row->get_data();
					// 	*(uint64_t *)(&data[off]) = get_thd_id();
					// }
					// for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
					int fid = 0;
					char * data = row->get_data();
					*(uint64_t *)(&data[fid * 100]) = 0;
					// }
                } 
            }
			iteration ++;
			if (req->rtype == RD || req->rtype == WR || iteration == req->scan_len)
				finish_req = true;
		}
	}

#ifdef LONG_TX_ENABLE
	if (m_query->exec_time > 0)
		micro_sleep(m_query->exec_time); // May be not accurate.
#endif
	rc = RCOK;
final:
	endtime = get_sys_clock();
	if (PRINT_LAT_DEBUG && get_thd_id() == 0)
		last_try_exec_time = endtime - starttime;
	// Execute the loose ends (validation in OCC, etc.)
	rc = finish(rc);
	
	starttime = get_sys_clock();
	if (PRINT_LAT_DEBUG && get_thd_id() == 0)
		last_try_commit_time = starttime - endtime;
	return rc;
}