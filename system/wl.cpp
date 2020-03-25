#include "global.h"
#include "helper.h"
#include "wl.h"
#include "row.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "index_mbtree.h"
#include "catalog.h"
#include "mem_alloc.h"

__thread int cur_index_cnt = 0;

RC workload::init() {
	sim_done = false;
	return RCOK;
}

RC workload::init_schema(string schema_file) {
    assert(sizeof(uint64_t) == 8);
    assert(sizeof(double) == 8);
	string line;
	ifstream fin(schema_file);
    Catalog * schema;
    while (getline(fin, line)) {
		if (line.compare(0, 6, "TABLE=") == 0) {
			string tname;
			tname = &line[6];
			schema = (Catalog *) _mm_malloc(sizeof(Catalog), CL_SIZE);
			getline(fin, line);
			int col_count = 0;
			// Read all fields for this table.
			vector<string> lines;
			while (line.length() > 1) {
				lines.push_back(line);
				getline(fin, line);
			}
			schema->init( tname.c_str(), lines.size() );
			for (UInt32 i = 0; i < lines.size(); i++) {
				string line = lines[i];
			    size_t pos = 0;
				string token;
				int elem_num = 0;
				int size = 0;
				string type;
				string name;
				while (line.length() != 0) {
					pos = line.find(",");
					if (pos == string::npos)
						pos = line.length();
	    			token = line.substr(0, pos);
			    	line.erase(0, pos + 1);
					switch (elem_num) {
					case 0: size = atoi(token.c_str()); break;
					case 1: type = token; break;
					case 2: name = token; break;
					default: assert(false);
					}
					elem_num ++;
				}
				assert(elem_num == 3);
                schema->add_col((char *)name.c_str(), size, (char *)type.c_str());
				col_count ++;
			}
			table_t * cur_tab = (table_t *) _mm_malloc(sizeof(table_t), CL_SIZE);
			cur_tab->init(schema);
			tables[tname] = cur_tab;
        } else if (!line.compare(0, 6, "INDEX=")) {
			string iname;
			iname = &line[6];
			getline(fin, line);

			vector<string> items;
			string token;
			size_t pos;
			while (line.length() != 0) {
				pos = line.find(",");
				if (pos == string::npos)
					pos = line.length();
	    		token = line.substr(0, pos);
				items.push_back(token);
		    	line.erase(0, pos + 1);
			}

			string tname(items[0]);
			index_base * index = (index_base *) _mm_malloc(sizeof(INDEX), 64);
			if (items.size() > 2) { // Index type has been specified in the schema.
				string index_type(items[2]);
				if (index_type == "HASH") {
					new(index) IndexHash();
				} else if (index_type == "BTREE") {
					new(index) IndexMBTree();
				} else {
					printf("Unknown index type.\n");
					assert(false);
				}
			} else {
				new(index) INDEX(); // Determined by the config.h
			}
			int part_cnt = (CENTRAL_INDEX)? 1 : g_part_cnt;
			if (tname == "ITEM")
				part_cnt = 1;

	#if WORKLOAD == YCSB
			index->init(part_cnt, tables[tname], g_synth_table_size * 2);
	#elif WORKLOAD == TPCC
			assert(tables[tname] != NULL);
			index->init(part_cnt, tables[tname], stoi( items[1] ) * part_cnt);
	#endif

			indexes[iname] = index;
		}
    }
	fin.close();
	return RCOK;
}



void workload::index_insert(string index_name, uint64_t key, row_t * row) {
	assert(false);
	INDEX * index = (INDEX *) indexes[index_name];
	index_insert(index, key, row);
}

void workload::index_insert(index_base * index, uint64_t key, row_t * row, int64_t part_id) {
	uint64_t pid = part_id;
	if (part_id == -1)
		pid = get_part_id(row);
	itemid_t * m_item =
		(itemid_t *) mem_allocator.alloc( sizeof(itemid_t), pid );
	m_item->init();
	m_item->type = DT_row;
	m_item->location = row;
	m_item->valid = true;

    assert( index->index_insert(key, m_item, pid) == RCOK );
}

int workload::read_row_data(int index_cnt, uint64_t primary_key, void *buf) {
	itemid_t * item;
	indexes_[index_cnt]->index_read(primary_key, item, 0, 0); // assume one partition, FIXME.
	row_t * row = ((row_t *)item->location);
	memcpy(buf, row->get_data(), row->get_tuple_size());
	return row->get_tuple_size();
}

void workload::write_row_data(int index_cnt, uint64_t primary_key, int size, void *buf) {
	itemid_t * item;
	indexes_[index_cnt]->index_read(primary_key, item, 0, 0); // assume one partition, FIXME.
	row_t * row = ((row_t *)item->location);
	memcpy(row->get_data(), buf, row->get_tuple_size());
}

void workload::insert_row_data(int index_cnt, uint64_t primary_key, int size, void *buf) {
	row_t *row;
	uint64_t row_id;

	tables_[index_cnt]->get_new_row(row, 0, row_id);

	row->set_primary_key(primary_key);

	itemid_t * m_item =
		(itemid_t *) mem_allocator.alloc( sizeof(itemid_t), 0);

	m_item->init();
	m_item->type = DT_row;
	m_item->location = row;
	m_item->valid = true;

	indexes_[index_cnt]->index_insert(primary_key, m_item, 0);
}

void workload::remove_row_data(int index_cnt, uint64_t primary_key) {
	// Not implemented.
	return;
}

void workload::update_index_accessed(index_base *index) {
	int i = 0;
	for (i = 0; i < 32; ++i) 
		if (indexes_[i] == index)
			break;
	cur_index_cnt = i;
}