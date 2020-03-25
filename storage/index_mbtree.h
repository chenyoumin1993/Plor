#pragma once

// #include "global.h"
#include "helper.h"
#include "index_base.h"

class table_t;
class itemid_t;
class row_t;
class txn_man;

class IndexMBTree : public index_base {
 public:
  // RC init(uint64_t part_cnt, table_t* table);
  RC init(int part_cnt, table_t* table, uint64_t bucket_cnt);

  bool index_exist(idx_key_t key) {
      printf("Not implemented.\n");
      return true;
  }

  RC index_insert(idx_key_t key, itemid_t* item, int part_id=-1);
  // This method ignores the second row_t* argument.
  RC index_remove(idx_key_t key, int part_id=-1);

  RC index_read(idx_key_t key, itemid_t* &item, int part_id);
  RC index_read(idx_key_t key, itemid_t* &item, int part_id, int thd_id);

  RC index_read_multiple(idx_key_t key, itemid_t** items,
                         size_t& count, int part_id=-1);

  RC index_read_range(idx_key_t min_key, idx_key_t max_key,
                      itemid_t** items, size_t& count, int part_id);
  RC index_read_range_rev(idx_key_t min_key, idx_key_t max_key,
                          itemid_t** items, size_t& count, int part_id);

  static uint64_t extract_version(void *);

  table_t* table;
  std::vector<void*> btree_idx;
};
