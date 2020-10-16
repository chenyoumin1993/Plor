#include "txn.h"
#include "row.h"
#include "row_mocc.h"
#include "log.h"

#if CC_ALG == MOCC

RC
txn_man::validate_mocc()
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

	// bubble sort the write_set, in primary key order [changed to decreasing order for TPCC, since TPCC access records in an increasing order] 
	for (int i = wr_cnt - 1; i >= 1; i--) {
		for (int j = 0; j < i; j++) {
			if (accesses[ write_set[j] ]->orig_row->get_primary_key() < 
				accesses[ write_set[j + 1] ]->orig_row->get_primary_key())
			{
				int tmp = write_set[j];
				write_set[j] = write_set[j+1];
				write_set[j+1] = tmp;
			}
		}
	}

    step = 1;
	int num_locks = 0;
	ts_t max_tid = 0;
	bool done = false;

    ts_t wait_start__ = 0, wait_end__ = 0;
	if (_pre_abort && !read_committed) { // Here, read_committed TXs are read-only TXs.
		for (int i = 0; i < wr_cnt; i++) {
			row_t * row = accesses[ write_set[i] ]->orig_row;
			if (row->manager->get_tid() != accesses[write_set[i]]->tid) {
                row->increase_temperature();
				rc = Abort;
				goto final;
			}
		}
		for (int i = 0; i < row_cnt - wr_cnt; i ++) {
			Access * access = accesses[ read_set[i] ];
			if (access->orig_row->manager->get_tid() != accesses[read_set[i]]->tid) {
                access->orig_row->increase_temperature();
				rc = Abort;
				goto final;
			}
		}
	}

    step = 2;
	// lock all rows in the write set.

    wait_start__ = get_sys_clock();
	if (_validation_no_wait) {
		while (!done) {
			num_locks = 0;
			for (int i = 0; i < wr_cnt; i++) {
				row_t * row = accesses[ write_set[i] ]->orig_row;
				if (!row->manager->try_lock(this, LOCK_EX, false))
					break;
				row->manager->assert_lock();
				num_locks ++;
				if (row->manager->get_tid() != accesses[write_set[i]]->tid)
				{
					rc = Abort;
					goto final;
				}
			}
			asm volatile ("sfence" ::: "memory");
			if (num_locks == wr_cnt)
				done = true;
			else {
				for (int i = 0; i < num_locks; i++)
					accesses[ write_set[i] ]->orig_row->manager->unlock(this, LOCK_EX);
				if (_pre_abort) {
					num_locks = 0;
					for (int i = 0; i < wr_cnt; i++) {
						row_t * row = accesses[ write_set[i] ]->orig_row;
						if (row->manager->get_tid() != accesses[write_set[i]]->tid) {
                            row->increase_temperature();
							rc = Abort;
							goto final;
						}
					}
					for (int i = 0; i < row_cnt - wr_cnt; i ++) {
						Access * access = accesses[ read_set[i] ];
						if (access->orig_row->manager->get_tid() != accesses[read_set[i]]->tid) {
                            access->orig_row->increase_temperature();
							rc = Abort;
							goto final;
						}
					}
				}
                PAUSE
			}
		}
	} else {
		for (int i = 0; i < wr_cnt; i++) {
			row_t * row = accesses[ write_set[i] ]->orig_row;
			row->manager->lock(this, LOCK_EX, false);
			num_locks++;
			if (row->manager->get_tid() != accesses[write_set[i]]->tid) {
                row->increase_temperature();
				rc = Abort;
				goto final;
			}
		}
	}

    step = 3;
	// validate rows in the read set
	// for repeatable_read, no need to validate the read set.
	if (!read_committed) {
		for (int i = 0; i < row_cnt - wr_cnt; i ++) {
			Access * access = accesses[ read_set[i] ];
			bool success = access->orig_row->manager->validate(access->tid, false);
			if (!success) {
                access->orig_row->increase_temperature();
				rc = Abort;
				goto final;
			}
			if (access->tid > max_tid)
				max_tid = access->tid;
		}
		// validate rows in the write set
		for (int i = 0; i < wr_cnt; i++) {
			Access * access = accesses[ write_set[i] ];
			bool success = access->orig_row->manager->validate(access->tid, true);
			if (!success) {
                access->orig_row->increase_temperature();
				rc = Abort;
				goto final;
			}
			if (access->tid > max_tid)
				max_tid = access->tid;
		}
	}
	if (max_tid > _cur_tid)
		_cur_tid = max_tid + 1;
	else 
		_cur_tid ++;
final:
    wait_end__ = get_sys_clock();

	if (PRINT_LAT_DEBUG && get_thd_id() == 0) {
        if (wait_start__ != 0 && wait_end__ != 0)
    		last_waiting_time_1 += (wait_end__ - wait_start__); // ns
	}

    // at this phase, we can unlock all read locks unconditionally (if they're hot when we accessed them).
    unlock_read_locks_all();

	rc = apply_index_changes(rc);
	if (rc == Abort) {
		for (int i = 0; i < num_locks; i++) 
			accesses[ write_set[i] ]->orig_row->manager->unlock(this, LOCK_EX);
		asm volatile ("sfence" ::: "memory");
		cleanup(rc);
	} else {
		// Log first.
	#if PERSISTENT_LOG == 1
		if (!readonly && !read_committed)
			log->log_tx_meta(get_txn_id(), wr_cnt);
		
		for (int i = 0; i < wr_cnt; i++) {
			log->log_content(accesses[ write_set[i] ]->orig_row->get_primary_key(), 
				accesses[ write_set[i] ]->orig_row->get_data(), 
				accesses[ write_set[i] ]->orig_row->get_tuple_size());
		}
	#endif
		for (UInt32 i = 0; i < insert_cnt; i++) {
			row_t * row = insert_rows[i];
      		row->manager->set_tid(_cur_tid);  // unlocking is done as well
		}
		
		for (int i = 0; i < wr_cnt; i++) {
			Access * access = accesses[ write_set[i] ];
			access->orig_row->manager->write( 
				access->data, _cur_tid );
			asm volatile ("sfence" ::: "memory");
			accesses[ write_set[i] ]->orig_row->manager->unlock(this, LOCK_EX);
		}
		cleanup(rc);
	}

    // clearnup my lockstates.
    clear_lock_state(rc);
	return rc;
}
#endif
