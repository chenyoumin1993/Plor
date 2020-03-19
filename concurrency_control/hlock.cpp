#include "txn.h"
#include "row.h"
#include "row_hlock.h"
#include "log.h"

#if CC_ALG == HLOCK

RC
txn_man::validate_hlock()
{
	RC rc = RCOK;
	// lock write tuples in the primary key order.
	int write_set[wr_cnt];
	int cur_wr_idx = 0;
	int read_set[row_cnt - wr_cnt];
	int cur_rd_idx = 0;
	for (int rid = 0; rid < row_cnt; rid ++) {
		if (accesses[rid]->type == WR)
			write_set[cur_wr_idx ++] = rid;
		else 
			read_set[cur_rd_idx ++] = rid;
	}

	// bubble sort the write_set, in primary key order 
	for (int i = wr_cnt - 1; i >= 1; i--) {
		for (int j = 0; j < i; j++) {
			if (accesses[ write_set[j] ]->orig_row->get_primary_key() > 
				accesses[ write_set[j + 1] ]->orig_row->get_primary_key())
			{
				int tmp = write_set[j];
				write_set[j] = write_set[j+1];
				write_set[j+1] = tmp;
			}
		}
	}

	int num_locks = 0;
	ts_t max_tid = 0;
	// bool done = false;
	int ret = 0;

	// lock all rows in the write set.
	for (int i = 0; i < wr_cnt; i++) {
		row_t * row = accesses[ write_set[i] ]->orig_row;
		ret = row->manager->lock_wr(this);
		if (ret == 1) {
			num_locks++;
			rc = Abort;
			goto final;
		} else if (ret == 2) {
			rc = Abort;
			goto final;
		}
		num_locks++;
	}

	// unlock the rows in the read set
	// for repeatable_read, no need to validate the read set.
	for (int i = 0; i < row_cnt - wr_cnt; i ++) {
		Access * access = accesses[ read_set[i] ];
		access->orig_row->manager->unlock_rd(this);		
		if (access->tid > max_tid)
			max_tid = access->tid;
	}
	
	for (int i = 0; i < wr_cnt; i++) {
		Access * access = accesses[ write_set[i] ];
		if (access->tid > max_tid)
			max_tid = access->tid;
	}
	if (max_tid > _cur_tid)
		_cur_tid = max_tid + 1;
	else 
		_cur_tid ++;
final:
	if (rc == Abort) {
		for (int i = 0; i < num_locks; i++) 
			accesses[ write_set[i] ]->orig_row->manager->unlock_wr(this);
		asm volatile ("sfence" ::: "memory");
		cleanup(rc);
	} else {
	#if PERSISTENT_LOG == 1
		log->log_tx_meta(get_txn_id(), wr_cnt);
		for (int i = 0; i < wr_cnt; i++) {
			log->log_content(accesses[ write_set[i] ]->orig_row->get_primary_key(), 
				accesses[ write_set[i] ]->orig_row->get_data(), 
				accesses[ write_set[i] ]->orig_row->get_tuple_size());
		}
	#endif
		for (int i = 0; i < wr_cnt; i++) {
			Access * access = accesses[ write_set[i] ];
			access->orig_row->manager->write( 
				access->data, _cur_tid );
			asm volatile ("sfence" ::: "memory");
			accesses[ write_set[i] ]->orig_row->manager->unlock_wr(this);
		}
		cleanup(rc);
	}
	return rc;
}
#endif
