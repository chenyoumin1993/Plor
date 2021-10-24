#include "global.h"
#include "helper.h"
#include "tpcc.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "tpcc_helper.h"
#include "row.h"
#include "query.h"
#include "txn.h"
#include "mem_alloc.h"
#include "tpcc_const.h"

RC tpcc_wl::init() {
	workload::init();
	std::string path = "./benchmarks/";
#if TPCC_SMALL
	path += "TPCC_short_schema.txt";
#else
	path += "TPCC_full_schema.txt";
#endif
	// cout << "reading schema file: " << path << endl;
	init_schema( path.c_str() );
	// cout << "TPCC schema initialized" << endl;
	init_table();
	next_tid = 0;
	return RCOK;
}

RC tpcc_wl::init_schema(const char * schema_file) {
	workload::init_schema(schema_file);
	t_warehouse = tables["WAREHOUSE"];
	t_district = tables["DISTRICT"];
	t_district_ext = tables["DISTRICT-EXT"];
	t_customer = tables["CUSTOMER"];
	t_history = tables["HISTORY"];
	t_neworder = tables["NEW-ORDER"];
	t_order = tables["ORDER"];
	t_orderline = tables["ORDER-LINE"];
	t_item = tables["ITEM"];
	t_stock = tables["STOCK"];

	i_item = indexes["ITEM_IDX"];
	i_warehouse = indexes["WAREHOUSE_IDX"];
	i_district = indexes["DISTRICT_IDX"];
	i_district_ext = indexes["DISTRICT_EXT_IDX"];
	i_customer_id = indexes["CUSTOMER_ID_IDX"];
	i_customer_last = indexes["CUSTOMER_LAST_IDX"];
	i_stock = indexes["STOCK_IDX"];
	i_order = indexes["ORDER_IDX"];
	i_order_cust = indexes["ORDER_CUST_IDX"];
	i_neworder = indexes["NEWORDER_IDX"];
	i_orderline = indexes["ORDERLINE_IDX"];

	tables_[0] = t_item;
	tables_[1] = t_warehouse;
	tables_[2] = t_district;
	tables_[3] = t_district_ext;
	tables_[4] = t_customer;
	tables_[5] = t_history;
	tables_[6] = t_stock;
	tables_[7] = t_order;
	tables_[8] = t_orderline;
	tables_[9] = t_neworder;
	tables_[10] = NULL;

	indexes_[0] = i_item;
	indexes_[1] = i_warehouse;
	indexes_[2] = i_district;
	indexes_[3] = i_district_ext;
	indexes_[4] = i_customer_id;
	indexes_[5] = i_customer_last;
	indexes_[6] = i_stock;
	indexes_[7] = i_order;
	indexes_[8] = i_order_cust;
	indexes_[9] = i_neworder;
	indexes_[10] = i_orderline;
	indexes_[11] = NULL;
	index_2_table_[0] = 0;
	index_2_table_[1] = 1;
	index_2_table_[2] = 2;
	index_2_table_[3] = 3;
	index_2_table_[4] = 4;
	index_2_table_[5] = 4;
	index_2_table_[6] = 6;
	index_2_table_[7] = 7;
	index_2_table_[8] = 7;
	index_2_table_[9] = 9;
	index_2_table_[10] = 8;
	return RCOK;
}

RC tpcc_wl::init_table() {
	num_wh = g_num_wh;

/******** fill in data ************/
// data filling process:
//- item
//- wh
//	- stock
// 	- dist
//  	- cust
//	  	- hist
//		- order 
//		- new order
//		- order line
/**********************************/
	int buf_cnt = (num_wh > g_thread_cnt) ? num_wh : g_thread_cnt;
	tpcc_buffer = new drand48_data * [buf_cnt];
	for (uint32_t i = 0; i < buf_cnt; ++i) {
		// printf("%d\n", g_thread_cnt);
		tpcc_buffer[i] = (drand48_data *) _mm_malloc(sizeof(drand48_data), 64);
		srand48_r(i + 1, tpcc_buffer[i]);
	}
	pthread_t * p_thds = new pthread_t[g_num_wh - 1];
	for (uint32_t i = 0; i < g_num_wh - 1; i++) 
		pthread_create(&p_thds[i], NULL, threadInitWarehouse, this);
	threadInitWarehouse(this);
	for (uint32_t i = 0; i < g_num_wh - 1; i++) 
		pthread_join(p_thds[i], NULL);

	// printf("TPCC Data Initialization Complete!\n");
	return RCOK;
}

RC tpcc_wl::get_txn_man(txn_man *& txn_manager, thread_t * h_thd) {
	txn_manager = (tpcc_txn_man *) _mm_malloc( sizeof(tpcc_txn_man), 64);
	new(txn_manager) tpcc_txn_man();
	txn_manager->init(h_thd, this, h_thd->get_thd_id());
	return RCOK;
}

// TODO ITEM table is assumed to be in partition 0
void tpcc_wl::init_tab_item() {
	for (UInt32 i = 1; i <= g_max_items; i++) {
		row_t * row;
		uint64_t row_id;
		t_item->get_new_row(row, 0, row_id);
		row->set_primary_key(i);
		row->set_value(I_ID, i);
		row->set_value(I_IM_ID, URand(1L,10000L, 0));
		char name[24];
		MakeAlphaString(14, 24, name, 0);
		row->set_value(I_NAME, name);
		row->set_value(I_PRICE, URand(1, 100, 0));
		char data[50];
    	int len = MakeAlphaString(26, 50, data, 0);
		// TODO in TPCC, "original" should start at a random position
		if (RAND(10, 0) == 0) {
			uint64_t startORIGINAL = URand(2, (len - 8), 0);
			strcpy(data + startORIGINAL, "original");		
		}
		row->set_value(I_DATA, data);
		
		index_insert(i_item, i, row, 0);
	}
}

void tpcc_wl::init_tab_wh(uint32_t wid) {
	assert(wid >= 1 && wid <= g_num_wh);
	row_t * row;
	uint64_t row_id;
	t_warehouse->get_new_row(row, 0, row_id);
	row->set_primary_key(wid);

	row->set_value(W_ID, wid);
	char name[10];
    MakeAlphaString(6, 10, name, wid-1);
	row->set_value(W_NAME, name);
	char street[20];
    MakeAlphaString(10, 20, street, wid-1);
	row->set_value(W_STREET_1, street);
    MakeAlphaString(10, 20, street, wid-1);
	row->set_value(W_STREET_2, street);
    MakeAlphaString(10, 20, street, wid-1);
	row->set_value(W_CITY, street);
	char state[2];
	MakeAlphaString(2, 2, state, wid-1); /* State */
	row->set_value(W_STATE, state);
	char zip[9];
   	MakeNumberString(9, 9, zip, wid-1); /* Zip */
	row->set_value(W_ZIP, zip);
   	double tax = (double)URand(0L,200L,wid-1)/1000.0;
   	double w_ytd=300000.00;
	row->set_value(W_TAX, tax);
	row->set_value(W_YTD, w_ytd);
	
	index_insert(i_warehouse, wid, row, wh_to_part(wid));
	return;
}

void tpcc_wl::init_tab_dist(uint64_t wid) {
	for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
		row_t * row;
		uint64_t row_id;
		t_district->get_new_row(row, 0, row_id);
		row->set_primary_key(distKey(did, wid));
		
		row->set_value(D_ID, did);
		row->set_value(D_W_ID, wid);
		char name[10];
		MakeAlphaString(6, 10, name, wid-1);
		row->set_value(D_NAME, name);
		char street[20];
        MakeAlphaString(10, 20, street, wid-1);
		row->set_value(D_STREET_1, street);
        MakeAlphaString(10, 20, street, wid-1);
		row->set_value(D_STREET_2, street);
        MakeAlphaString(10, 20, street, wid-1);
		row->set_value(D_CITY, street);
		char state[2];
		MakeAlphaString(2, 2, state, wid-1); /* State */
		row->set_value(D_STATE, state);
		char zip[9];
    	MakeNumberString(9, 9, zip, wid-1); /* Zip */
		row->set_value(D_ZIP, zip);
    	double tax = (double)URand(0L,200L,wid-1)/1000.0;
    	double w_ytd=30000.00;
		row->set_value(D_TAX, tax);
		row->set_value(D_YTD, w_ytd);
		row->set_value(D_NEXT_O_ID, 3001);
		index_insert(i_district, distKey(did, wid), row, wh_to_part(wid));

		// A mirror table.
		// row_t * row_new;
		// uint64_t row_new_id;
		// t_district_ext->get_new_row(row_new, 0, row_new_id);
		// assert(row != row_ext);
		// row_new->set_primary_key(distKey(did, wid));
		// memcpy(row_new->get_data(), row->get_data(), row->get_tuple_size());
		// index_insert(i_district_ext, distKey(did, wid), row_new, wh_to_part(wid));
		// printf("...\n");
	}
}

void tpcc_wl::init_tab_stock(uint64_t wid) {
	
	for (UInt32 sid = 1; sid <= g_max_items; sid++) {
		row_t * row;
		uint64_t row_id;
		t_stock->get_new_row(row, 0, row_id);
		row->set_primary_key(stockKey(sid, wid));
		row->set_value(S_I_ID, sid);
		row->set_value(S_W_ID, wid);
		row->set_value(S_QUANTITY, URand(10, 100, wid-1));
		row->set_value(S_REMOTE_CNT, 0);
#if !TPCC_SMALL
		char s_dist[25];
		char row_name[10] = "S_DIST_";
		for (int i = 1; i <= 10; i++) {
			if (i < 10) {
				row_name[7] = '0';
				row_name[8] = i + '0';
			} else {
				row_name[7] = '1';
				row_name[8] = '0';
			}
			row_name[9] = '\0';
			MakeAlphaString(24, 24, s_dist, wid-1);
			row->set_value(row_name, s_dist);
		}
		row->set_value(S_YTD, 0);
		row->set_value(S_ORDER_CNT, 0);
		char s_data[50];
		int len = MakeAlphaString(26, 50, s_data, wid-1);
		if (rand() % 100 < 10) {
			int idx = URand(0, len - 8, wid-1);
			strcpy(&s_data[idx], "original");
		}
		row->set_value(S_DATA, s_data);
#endif
		index_insert(i_stock, stockKey(sid, wid), row, wh_to_part(wid));
	}
}

void tpcc_wl::init_tab_cust(uint64_t did, uint64_t wid) {
	assert(g_cust_per_dist >= 1000);
	for (UInt32 cid = 1; cid <= g_cust_per_dist; cid++) {
		row_t * row;
		uint64_t row_id;
		t_customer->get_new_row(row, 0, row_id);

		row->set_value(C_ID, cid);		
		row->set_value(C_D_ID, did);
		row->set_value(C_W_ID, wid);
		char c_last[LASTNAME_LEN];
		if (cid <= 1000)
			Lastname(cid - 1, c_last);
		else
			Lastname(NURand(255,0,999,wid-1), c_last);
		row->set_value(C_LAST, c_last);
#if !TPCC_SMALL
		char tmp[3] = "OE";
		row->set_value(C_MIDDLE, tmp);
		char c_first[FIRSTNAME_LEN];
		MakeAlphaString(FIRSTNAME_MINLEN, sizeof(c_first), c_first, wid-1);
		row->set_value(C_FIRST, c_first);
		char street[20];
        MakeAlphaString(10, 20, street, wid-1);
		row->set_value(C_STREET_1, street);
        MakeAlphaString(10, 20, street, wid-1);
		row->set_value(C_STREET_2, street);
        MakeAlphaString(10, 20, street, wid-1);
		row->set_value(C_CITY, street); 
		char state[2];
		MakeAlphaString(2, 2, state, wid-1); /* State */
		row->set_value(C_STATE, state);
		char zip[9];
    	MakeNumberString(9, 9, zip, wid-1); /* Zip */
		row->set_value(C_ZIP, zip);
		char phone[16];
  		MakeNumberString(16, 16, phone, wid-1); /* Zip */
		row->set_value(C_PHONE, phone);
		row->set_value(C_SINCE, 0);
		row->set_value(C_CREDIT_LIM, 50000);
		row->set_value(C_DELIVERY_CNT, 0);
		char c_data[500];
        MakeAlphaString(300, 500, c_data, wid-1);
		row->set_value(C_DATA, c_data);
#endif
		if (RAND(10, wid-1) == 0) {
			char tmp[] = "GC";
			row->set_value(C_CREDIT, tmp);
		} else {
			char tmp[] = "BC";
			row->set_value(C_CREDIT, tmp);
		}
		row->set_value(C_DISCOUNT, (double)RAND(5000,wid-1) / 10000);
		row->set_value(C_BALANCE, -10.0);
		row->set_value(C_YTD_PAYMENT, 10.0);
		row->set_value(C_PAYMENT_CNT, 1);
		uint64_t key;
		key = custNPKey(did, wid, c_last);
		row->set_primary_key(key);
		index_insert(i_customer_last, key, row, wh_to_part(wid));

		key = custKey(cid, did, wid);
		row_t *row_new;
		t_customer->get_new_row(row_new, 0, row_id);
		row_new->set_primary_key(key);
		memcpy(row_new->get_data(), row->get_data(), row->get_tuple_size());
		index_insert(i_customer_id, key, row_new, wh_to_part(wid));
	}
}

void tpcc_wl::init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id) {
	row_t * row;
	uint64_t row_id;
	t_history->get_new_row(row, 0, row_id);
	row->set_primary_key(0);
	row->set_value(H_C_ID, c_id);
	row->set_value(H_C_D_ID, d_id);
	row->set_value(H_D_ID, d_id);
	row->set_value(H_C_W_ID, w_id);
	row->set_value(H_W_ID, w_id);
	row->set_value(H_DATE, 0);
	row->set_value(H_AMOUNT, 10.0);
#if !TPCC_SMALL
	char h_data[24];
	MakeAlphaString(12, 24, h_data, w_id-1);
	row->set_value(H_DATA, h_data);
#endif

}

void tpcc_wl::init_tab_order(uint64_t did, uint64_t wid) {
	uint64_t perm[g_cust_per_dist]; 
	init_permutation(perm, wid); /* initialize permutation of customer numbers */
	for (UInt32 oid = 1; oid <= g_cust_per_dist; oid++) {
		row_t * row;
		uint64_t row_id;
		t_order->get_new_row(row, 0, row_id);
		row->set_primary_key(orderKey(oid, did, wid));
		uint64_t o_ol_cnt = 1;
		uint64_t cid = perm[oid - 1]; //get_permutation();
		row->set_value(O_ID, oid);
		row->set_value(O_C_ID, cid);
		row->set_value(O_D_ID, did);
		row->set_value(O_W_ID, wid);
		uint64_t o_entry = 2013;
		row->set_value(O_ENTRY_D, o_entry);
		if (oid < 2101)
			row->set_value(O_CARRIER_ID, URand(1, 10, wid-1));
		else 
			row->set_value(O_CARRIER_ID, 0);
		o_ol_cnt = URand(5, 15, wid-1);
		row->set_value(O_OL_CNT, o_ol_cnt);
		row->set_value(O_ALL_LOCAL, 1);
		// index_insert(i_customer_id, key, row, wh_to_part(wid));
		index_insert(i_order, orderKey(oid, did, wid), row, wh_to_part(wid));

		row_t *row_cust;
		t_order->get_new_row(row_cust, 0, row_id);
		row_cust->set_primary_key(orderCustKey(oid, cid, did, wid));
		memcpy(row_cust->get_data(), row->get_data(), row->get_tuple_size());
    	index_insert(i_order_cust, orderCustKey(oid, cid, did, wid), row_cust,
                 wh_to_part(wid));
		// ORDER-LINE	
#if !TPCC_SMALL
		for (uint32_t ol = 1; ol <= o_ol_cnt; ol++) {
			t_orderline->get_new_row(row, 0, row_id);
			row->set_primary_key(orderlineKey(ol, oid, did, wid));
			row->set_value(OL_O_ID, oid);
			row->set_value(OL_D_ID, did);
			row->set_value(OL_W_ID, wid);
			row->set_value(OL_NUMBER, ol);
			row->set_value(OL_I_ID, URand(1, 100000, wid-1));
			row->set_value(OL_SUPPLY_W_ID, wid);
			if (oid < 2101) {
				row->set_value(OL_DELIVERY_D, o_entry);
				row->set_value(OL_AMOUNT, 0);
			} else {
				row->set_value(OL_DELIVERY_D, 0);
				row->set_value(OL_AMOUNT, (double)URand(1, 999999, wid-1)/100);
			}
			row->set_value(OL_QUANTITY, 5);
			char ol_dist_info[24];
	        MakeAlphaString(24, 24, ol_dist_info, wid-1);
			row->set_value(OL_DIST_INFO, ol_dist_info);
			index_insert(i_orderline, orderlineKey(ol, oid, did, wid), row,
                   wh_to_part(wid));
		}
#endif
		// NEW ORDER
		if (oid > 2100) {
			t_neworder->get_new_row(row, 0, row_id);
			row->set_primary_key(neworderKey(oid, did, wid));
			row->set_value(NO_O_ID, (int64_t)oid);
			row->set_value(NO_D_ID, did);
			row->set_value(NO_W_ID, wid);
			index_insert(i_neworder, neworderKey(oid, did, wid), row,
                   wh_to_part(wid));
		}
	}

	// for (int oid = 2101; oid <= g_cust_per_dist; ++oid) {
	// 		itemid_t *item = NULL;
	// 		i_neworder->index_read(neworderKey(oid, did, wid), item, wh_to_part(wid), 0);
	// 		// assert((void *)row == item->location);
	// 		row_t *r = (row_t *)item->location;
	// 		int64_t ooo_id;
	// 		r->get_value(NO_O_ID, ooo_id);
	// 		printf("%d\t", ooo_id);
	// 		assert(item != NULL);
	// }
	// printf("\n");
}

/*==================================================================+
| ROUTINE NAME
| InitPermutation
+==================================================================*/

void 
tpcc_wl::init_permutation(uint64_t * perm_c_id, uint64_t wid) {
	uint32_t i;
	// Init with consecutive values
	for(i = 0; i < g_cust_per_dist; i++) 
		perm_c_id[i] = i+1;

	// shuffle
	for(i=0; i < g_cust_per_dist-1; i++) {
		uint64_t j = URand(i+1, g_cust_per_dist-1, wid-1);
		uint64_t tmp = perm_c_id[i];
		perm_c_id[i] = perm_c_id[j];
		perm_c_id[j] = tmp;
	}
}


/*==================================================================+
| ROUTINE NAME
| GetPermutation
+==================================================================*/

void * tpcc_wl::threadInitWarehouse(void * This) {
	tpcc_wl * wl = (tpcc_wl *) This;
	int tid = ATOM_FETCH_ADD(wl->next_tid, 1);
	uint32_t wid = tid + 1;
	assert((uint64_t)tid < g_num_wh);
	
	if (tid == 0)
		wl->init_tab_item();
	wl->init_tab_wh( wid );
	wl->init_tab_dist( wid );
	wl->init_tab_stock( wid );
	for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
		wl->init_tab_cust(did, wid);
		wl->init_tab_order(did, wid);
		for (uint64_t cid = 1; cid <= g_cust_per_dist; cid++) 
			wl->init_tab_hist(cid, did, wid);
	}
	return NULL;
}

int64_t tpcc_wl::print() {
//   auto index = this->i_neworder;
//   auto key = neworderKey(g_max_orderline, 1, 1);
//   auto max_key = neworderKey(2101, 1, 1);  // Use key ">= 0" for "> -1"
//   auto part_id = wh_to_part(1);

//   itemid_t* items[1];
//   uint64_t count = 1;

//   auto idx_rc = index->index_read_range(key, max_key, items, count, part_id);
//   row_t *r = (row_t *)items[0]->location;
//   printf("%lld\n", r->get_primary_key());

//   for (int i = 0; i < 24; ++i) {
// 	  printf("%d ", (uint8_t)r->data[i]);
//   }
//   printf("\n");

//   int64_t o_id = 0;
//   r->get_value_bak(NO_O_ID, o_id);

//   printf("(1) o_id = %lld\n", o_id);
// //   r->get_value(NO_O_ID, o_id);
// //   printf("(2) o_id = %lld\n", o_id);
//   o_id = o_id;
  return 0;
}