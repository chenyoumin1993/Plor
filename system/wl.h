#pragma once 

#include "global.h"

class row_t;
class table_t;
class IndexHash;
class index_btree;
class Catalog;
class lock_man;
class txn_man;
class thread_t;
class index_base;
class Timestamp;
class Mvcc;

// this is the base class for all workload
class workload
{
public:
	// tables indexed by table name
	std::map<std::string, table_t *> tables;
	std::map<std::string, index_base *> indexes;

	table_t *tables_[32];
	index_base *indexes_[32]; // 32 indexes at most
	int index_2_table_[32];

	
	// initialize the tables and indexes.
	virtual RC init();
	virtual RC init_schema(std::string schema_file);
	virtual RC init_table()=0;
	virtual RC get_txn_man(txn_man *& txn_manager, thread_t * h_thd)=0;

	int read_row_data(int index_cnt, uint64_t primary_key, void *buf);
	void write_row_data(int index_cnt, uint64_t primary_key, int size, void *buf);
	void insert_row_data(int index_cnt, uint64_t primary_key, int size, void *buf);
	void remove_row_data(int index_cnt, uint64_t primary_key);

	int get_index_cnt(index_base *index);
	
	bool sim_done;
	bool print = false;
protected:
	void index_insert(std::string index_name, uint64_t key, row_t * row);
	void index_insert(index_base * index, uint64_t key, row_t * row, int64_t part_id = -1);
};

