// #define CONFIG_H "config/config-perf.h"

#define NDB_MASSTREE 1
#include "masstree/config.h"
#include "masstree_btree.h"

#ifdef NDEBUG
#undef NDEBUG
#endif

#include "index_mbtree.h"
#include "helper.h"
#include "row.h"
#include "mem_alloc.h"
#include "txn.h"

#include <unordered_map>

// #ifdef DEBUG
// #undef NDEBUG
// #endif

thread_local std::unordered_map<void*, uint64_t>  node_map;

struct mbtree_params : public Masstree::nodeparams<> {
  typedef itemid_t* value_type;
  typedef Masstree::value_print<value_type> value_print_type;
  typedef simple_threadinfo threadinfo_type;
  enum { RcuRespCaller = true };
};

typedef mbtree<mbtree_params> concurrent_mbtree;

class IndexMBTree_cb
    : public concurrent_mbtree::low_level_search_range_callback {
 public:
  IndexMBTree_cb(itemid_t** items, uint64_t count, uint64_t& i)
      : items_(items), count_(count), i_(i), abort_(false) {}

  void on_resp_node(const concurrent_mbtree::node_opaque_t* n,
                    uint64_t version) override {

    auto it = node_map.find((void*)n);
    if (it == node_map.end()) {
      // printf("index node seen: %p %" PRIu64 "\n", n, version);
      node_map.emplace_hint(it, (void*)n, version);
    } else if ((*it).second != version)
      abort_ = true;
  }

  bool invoke(const concurrent_mbtree::string_type& k,
              concurrent_mbtree::value_type v,
              const concurrent_mbtree::node_opaque_t* n,
              uint64_t version) override {
    (void)k;
    (void)n;
    (void)version;
    items_[i_++] = v;
    return i_ < count_;
  }

  bool need_to_abort() const { return abort_; }

 private:
  txn_man* txn_;
  itemid_t** items_;
  uint64_t count_;
  uint64_t& i_;
  bool abort_;
};

// RC IndexMBTree::init(uint64_t part_cnt, table_t* table) {
//   return init(part_cnt, table, 0);
// }

RC IndexMBTree::init(int part_cnt, table_t* table, uint64_t bucket_cnt) {
  (void)bucket_cnt;

  this->table = table;

  for (int part_id = 0; part_id < part_cnt; part_id++) {
    mem_allocator.register_thread(part_id % g_thread_cnt);

    auto t = (concurrent_mbtree*)mem_allocator.alloc(sizeof(concurrent_mbtree),
                                                     part_id);
    new (t) concurrent_mbtree;

    btree_idx.push_back(t);
  }

  return RCOK;
}

RC IndexMBTree::index_insert(idx_key_t key, itemid_t* item,
                             int part_id) {
  auto idx = reinterpret_cast<concurrent_mbtree*>(btree_idx[part_id]);

  u64_varkey mbtree_key(key);

  concurrent_mbtree::insert_info_t insert_info;
  if (!idx->insert_if_absent(mbtree_key, item, &insert_info)) return ERROR;


  auto it = node_map.find((void*)insert_info.node);

  if (it == node_map.end()) {
  } else if ((*it).second != insert_info.old_version) {
      return Abort;
  } else {
      (*it).second = insert_info.new_version;
  }
  return RCOK;
}

RC IndexMBTree::index_read(idx_key_t key, itemid_t* &item,
                           int part_id) {
  auto idx = reinterpret_cast<concurrent_mbtree*>(btree_idx[part_id]);

  u64_varkey mbtree_key(key);

  concurrent_mbtree::versioned_node_t search_info;
  if (!idx->search(mbtree_key, item, &search_info)) {

    auto it = node_map.find((void*)search_info.first);
    if (it == node_map.end()) {
      node_map.emplace_hint(it, (void*)search_info.first,
                                 search_info.second);
      // printf("index node seen: %p %" PRIu64 "\n", search_info.first,
      //        search_info.second);
    } else if ((*it).second != search_info.second)
      return Abort;

    return ERROR;
  }

  return RCOK;
}

RC IndexMBTree::index_read(idx_key_t key, itemid_t* &item, int part_id, int thd_id) {
  (void)thd_id;
  return index_read(key, item, part_id);
}

RC IndexMBTree::index_read_multiple(idx_key_t key, itemid_t** items,
                                    uint64_t& count, int part_id) {
  // Duplicate keys are currently not supported in IndexMBTree.
  assert(false);
  (void)key;
  (void)items;
  (void)count;
  (void)part_id;
  return ERROR;
}

RC IndexMBTree::index_read_range(idx_key_t min_key,
                                 idx_key_t max_key, itemid_t** items,
                                 uint64_t& count, int part_id) {
  if (count == 0) return RCOK;

  auto idx = reinterpret_cast<concurrent_mbtree*>(btree_idx[part_id]);

  u64_varkey mbtree_key_min(min_key);

  // mbtree's range is right-open.
  max_key++;
  assert(max_key != 0);
  u64_varkey mbtree_key_max(max_key);

  uint64_t i = 0;
  auto cb = IndexMBTree_cb(items, count, i);

  idx->search_range_call(mbtree_key_min, &mbtree_key_max, cb);
  if (cb.need_to_abort()) return Abort;

  count = i;

  return RCOK;
}

RC IndexMBTree::index_read_range_rev(idx_key_t min_key,
                                     idx_key_t max_key, itemid_t** items,
                                     uint64_t& count, int part_id) {
  if (count == 0) return RCOK;

  auto idx = reinterpret_cast<concurrent_mbtree*>(btree_idx[part_id]);

  // mbtree's range is left-open.
  assert(min_key != 0);
  min_key--;
  u64_varkey mbtree_key_min(min_key);

  u64_varkey mbtree_key_max(max_key);

  uint64_t i = 0;
  auto cb = IndexMBTree_cb(items, count, i);

  idx->rsearch_range_call(mbtree_key_max, &mbtree_key_min, cb);
  if (cb.need_to_abort()) return Abort;

  count = i;
  // printf("%" PRIu64 "\n", i);

  return RCOK;
}

RC IndexMBTree::index_remove(idx_key_t key, int part_id) {
  auto idx = reinterpret_cast<concurrent_mbtree*>(btree_idx[part_id]);

  u64_varkey mbtree_key(key);

  if (!idx->remove(mbtree_key, NULL)) return ERROR;

  return RCOK;
}

uint64_t IndexMBTree::extract_version(void *n) {
  return concurrent_mbtree::ExtractVersionNumber((concurrent_mbtree::node_opaque_t*)n);
}

// RC IndexMBTree::validate() {
//   for (auto it : node_map) {
//     auto n = (concurrent_mbtree::node_opaque_t*)it.first;
//     if (concurrent_mbtree::ExtractVersionNumber(n) != it.second) {
//       // printf("node wts validation failure!\n");
//       return Abort;
//     }
//   }

// // printf("node validation succeeded\n");
//   return RCOK;
// }
