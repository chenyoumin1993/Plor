#ifndef _CONFIG_H_
#define _CONFIG_H_
#define CORE_CNT 36
#define CC_ALG MOCC
#define ZIPF_THETA 0.5
#define READ_PERC 0.95
#define WRITE_PERC .05
#define USE_SPINLOCK 0
#define ATOMIC_WORD true
#define WORKLOAD YCSB

#define CORO_CNT 1
#define MAX_THREAD_ATOMIC 64 // help to choose the right solution in DLOCK.

#define PENALTY_POLICY 1
#define EPOCH_LENGTH 1000

#define LONG_TX_ENABLE 1
#define LONG_TX_PERC 1//0.00125
#define LONG_TX_EXEC_TIME 0

#define THREAD_CNT (CORO_CNT * CORE_CNT)

#define WAIT_CYCLE 0

#define BACKOFF_CYCLE 3750

#define VARY_REQ_CNT 1

#define INTERACTIVE_MODE 0
#define N_REPLICAS 1

#define VALVE_ENABLED 0
#define VALVE_CNT 1 // equal to the number of sockets.
// How fast to issue requests.
#define VALVE_TP 10000000

#define PERSISTENT_LOG 0

#define TS_OPT 0

#define YCSB_RO_TEST 0
#define YCSB_RO_RATIO 0.1

// #define DEBUG_WOUND 1

// 
/***********************************************/
// Simulation + Hardware
/***********************************************/
#define PART_CNT					1
// each transaction only accesses 1 virtual partition. But the lock/ts manager and index are not aware of such partitioning. VIRTUAL_PART_CNT describes the request distribution and is only used to generate queries. For HSTORE, VIRTUAL_PART_CNT should be the same as PART_CNT.
#define VIRTUAL_PART_CNT			1
#define PAGE_SIZE					4096 
#define CL_SIZE						64
// CPU_FREQ is used to get accurate timing info 
#define CPU_FREQ 					2.4 	// in GHz/s

// # of transactions to run for warmup
#define WARMUP						0
// YCSB or TPCC
// print the transaction latency distribution
#define PRT_LAT_DISTR					true	
#define STATS_ENABLE					true
#define TIME_ENABLE					true
#define MAX_LAT						100000

#define MEM_ALLIGN					8

// [THREAD_ALLOC]
#define THREAD_ALLOC				false
#define THREAD_ARENA_SIZE			(1UL << 22) 
#define MEM_PAD 					true

// [PART_ALLOC] 
#define PART_ALLOC 					false
#define MEM_SIZE					(1UL << 30) 
#define NO_FREE						false

/***********************************************/
// Concurrency Control
/***********************************************/
// WAIT_DIE, NO_WAIT, DL_DETECT, TIMESTAMP, MVCC, HEKATON, HSTORE, OCC, VLL, TICTOC, SILO, WOUND_WAIT
// TODO TIMESTAMP does not work at this moment
#define ISOLATION_LEVEL 			SERIALIZABLE

// all transactions acquire tuples according to the primary key order.
#define KEY_ORDER					false
// transaction roll back changes after abort
#define ROLL_BACK					true
// per-row lock/ts management or central lock/ts management
#define CENTRAL_MAN					false
#define BUCKET_CNT					31
#define ABORT_PENALTY 				100000
#define ABORT_BUFFER_SIZE			10
#define ABORT_BUFFER_ENABLE			true
// [ INDEX ]
#define ENABLE_LATCH				false
#define CENTRAL_INDEX				false
#define CENTRAL_MANAGER 			false
#define INDEX_STRUCT				IDX_MBTREE
#define BTREE_ORDER 				16

// [DL_DETECT] 
#define DL_LOOP_DETECT				1000 	// 100 us
#define DL_LOOP_TRIAL				100	// 1 us
#define NO_DL						KEY_ORDER
#define TIMEOUT						1000000 // 1ms
// [TIMESTAMP]
#define TS_TWR						false
#define TS_ALLOC					TS_CAS //TS_CLOCK
#define TS_BATCH_ALLOC				false
#define TS_BATCH_NUM				1
// [MVCC]
// when read/write history is longer than HIS_RECYCLE_LEN
// the history should be recycled.
//#define HIS_RECYCLE_LEN				10
//#define MAX_PRE_REQ					1024
//#define MAX_READ_REQ				1024
#define MIN_TS_INTVL				5000000 // 5 ms. In nanoseconds
// [OCC]
#define MAX_WRITE_SET				10
#define PER_ROW_VALID				true
// [TICTOC]
#define WRITE_COPY_FORM				"data" // ptr or data
#define TICTOC_MV					false
#define WR_VALIDATION_SEPARATE		true
#define WRITE_PERMISSION_LOCK		false
#define ATOMIC_TIMESTAMP			"false"
// [TICTOC, SILO]
#define VALIDATION_LOCK				"no-wait" // no-wait or waiting
#define PRE_ABORT					"true"
// [HSTORE]
// when set to true, hstore will not access the global timestamp.
// This is fine for single partition transactions. 
#define HSTORE_LOCAL_TS				false
// [VLL] 
#define TXN_QUEUE_SIZE_LIMIT		THREAD_CNT
// [DLOCK]
#define EX_MODE 0x1314

/***********************************************/
// Logging
/***********************************************/
#define LOG_COMMAND					false
#define LOG_REDO					false
#define LOG_BATCH_TIME				10 // in ms

/***********************************************/
// Benchmark
/***********************************************/
// max number of rows touched per transaction
#define MAX_ROW_PER_TXN				1024
#define QUERY_INTVL 				1UL
#define MAX_TXN_PER_PART 			100000
#define FIRST_PART_LOCAL 			true
#define MAX_TUPLE_SIZE				1024 // in bytes
// ==== [YCSB] ====
#define INIT_PARALLELISM			40
#define SYNTH_TABLE_SIZE 			(1024 * 1024 * 10)
#define SCAN_PERC 					0
#define SCAN_LEN					20
#define PART_PER_TXN 				1
#define PERC_MULTI_PART				1
#define REQ_PER_QUERY				16
#define FIELD_PER_TUPLE				10
// ==== [TPCC] ====
// For large warehouse count, the tables do not fit in memory
// small tpcc schemas shrink the table size.
#define TPCC_SMALL					false
// Some of the transactions read the data but never use them. 
// If TPCC_ACCESS_ALL == fales, then these parts of the transactions
// are not modeled.
#define TPCC_ACCESS_ALL 			false
#define WH_UPDATE					true
#define NUM_WH 1
#define TPCC_NP false

#define SMALL_RATIO 0.9
#define DLOCK_LOCKFREE 1
//
enum TPCCTxnType {TPCC_ALL, 
				TPCC_PAYMENT, 
				TPCC_NEW_ORDER, 
				TPCC_ORDER_STATUS, 
				TPCC_DELIVERY, 
				TPCC_STOCK_LEVEL};
extern TPCCTxnType 					g_tpcc_txn_type;

//#define TXN_TYPE					TPCC_ALL
#define PERC_PAYMENT 				0.5
#define FIRSTNAME_MINLEN 			8
#define FIRSTNAME_LEN 				16
#define LASTNAME_LEN 				16

#define DIST_PER_WARE				10

/***********************************************/
// TODO centralized CC management. 
/***********************************************/
#define MAX_LOCK_CNT				(20 * THREAD_CNT) 
#define TSTAB_SIZE                  50 * THREAD_CNT
#define TSTAB_FREE                  TSTAB_SIZE 
#define TSREQ_FREE                  4 * TSTAB_FREE
#define MVHIS_FREE                  4 * TSTAB_FREE
#define SPIN                        false

/***********************************************/
// Test cases
/***********************************************/
#define TEST_ALL					true
enum TestCases {
	READ_WRITE,
	CONFLICT
};
extern TestCases					g_test_case;
/***********************************************/
// DEBUG info
/***********************************************/
#define WL_VERB						true
#define IDX_VERB					false
#define VERB_ALLOC					true

#define DEBUG_LOCK					false
#define DEBUG_TIMESTAMP				false
#define DEBUG_SYNTH					false
#define DEBUG_ASSERT				false
#define DEBUG_CC					false //true

/***********************************************/
// Constant
/***********************************************/
// INDEX_STRUCT
#define IDX_HASH 					1
#define IDX_BTREE					2
#define IDX_MTREE					3
// WORKLOAD
#define YCSB						1
#define TPCC						2
#define TEST						3
// Concurrency Control Algorithm
#define NO_WAIT						1
#define WAIT_DIE					2
#define DL_DETECT					3
#define TIMESTAMP					4
#define MVCC						5
#define HSTORE						6
#define OCC							7
#define TICTOC						8
#define SILO						9
#define VLL							10
#define HEKATON 					11
#define WOUND_WAIT					12
#define OLOCK						13
#define DLOCK						14
#define HLOCK						15
#define MOCC						16

//Isolation Levels 
#define SERIALIZABLE				1
#define SNAPSHOT					2
#define REPEATABLE_READ				3
// TIMESTAMP allocation method.
#define TS_MUTEX					1
#define TS_CAS						2
#define TS_HW						3
#define TS_CLOCK					4


// Debug info
#define PRINT_LAT_DEBUG				1
#define BOOST_COROUTINES_NO_DEPRECATION_WARNING 1
#endif
