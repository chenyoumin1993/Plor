#pragma once 

#include "global.h"

class table_t;

class index_base {
public:
	virtual RC 			init(int part_cnt, table_t * table, uint64_t bucket_cnt) = 0;
	// virtual RC 			init(uint64_t size) { return RCOK; };

	virtual bool 		index_exist(idx_key_t key)=0; // check if the key exist.

	virtual RC 			index_insert(idx_key_t key, 
							itemid_t * item, 
							int part_id=-1)=0;

	virtual RC	 		index_read(idx_key_t key, 
							itemid_t * &item,
							int part_id=-1)=0;
	
	virtual RC	 		index_read(idx_key_t key, 
							itemid_t * &item,
							int part_id=-1, int thd_id=0)=0;
	
	virtual RC index_read_multiple(idx_key_t key, itemid_t** items,
                         size_t& count, int part_id=-1) = 0;
	virtual RC index_read_range(idx_key_t min_key, idx_key_t max_key,
                      itemid_t** items, size_t& count, int part_id) = 0;
	virtual RC index_read_range_rev(idx_key_t min_key, idx_key_t max_key,
                          itemid_t** items, size_t& count, int part_id) = 0;

	// TODO implement index_remove
	virtual RC 			index_remove(idx_key_t key, int part_id=-1) { return RCOK; };
	
	// the index in on "table". The key is the merged key of "fields"
	table_t * 			table;
};
