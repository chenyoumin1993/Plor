Plor (general transactions with predictable, low tail latency)
=======

Plor is based on DBx1000, which is a single node OLTP database management system (DBMS).

Dependencies
------------
	# for jemalloc 
	apt-get install libjemalloc-dev

	# for cityhash
	cd cityhash
	./configure
	make -j
	make install

	# for masstree
	cd silo/masstree
	./configure
	make -j

Build & Test
------------

To build the database.

    make -j

Configuration
-------------

DBMS configurations can be changed in the config.h file. Please refer to README for the meaning of each configuration. Here we only list several most important ones. 

    CORE_CNT        : Number of worker threads running in the database.
    WORKLOAD          : Supported workloads include YCSB and TPCC
    CC_ALG            : Concurrency control algorithm. Seven algorithms are supported 
                        (NO_WAIT WAIT_DIE WOUND_WAIT DLOCK HLOCK SILO MOCC) 
                        
Run
---

The DBMS can be run with 

    ./rundb


Output
-------------
	N(TP): throughput
	N(LAT@P99): 99th percentile latency
	N(ABT@P99): 99th percentile abort count
