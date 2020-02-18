#!/bin/bash
replace()
{
	sed -i "$1s/.*/$2/" $3
}

# CC
#CC_AGS=(WAIT_DIE NO_WAIT DL_DETECT TIMESTAMP MVCC HEKATON HSTORE OCC VLL TICTOC SILO)
#CC_AGS=(WAIT_DIE NO_WAIT DL_DETECT MVCC OCC) # HSTORE)
#CC_AGS=(WAIT_DIE NO_WAIT SILO TICTOC)
CC_AGS=(HLOCK)
# CC_AGS=(WOUND_WAIT DLOCK SILO)
#CC_AGS=(WAIT_DIE NO_WAIT)
MAX_THD=(1 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60 64)
#MAX_THD=(24 28 32 36 40 44 48 52 56 60 64)
#MAX_THD=(36)
#ZIPF=(0 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.75 0.8 0.85 0.9 0.95 0.99)
#ZIPF=(0.1 0.7 0.75 0.8 0.85 0.9)
#ZIPF=(0.8 0.85 0.9 0.99)
ZIPF=(0.99)
#READ=(0 0.5 0.9 1)
READ=(0.5)
EXEC_TIME=(0 1 10 100 1000)
EPOCH=(10 30 300 1000)
#CC_AGS=(WAIT_DIE)
#MAX_THD=1
#ZIPF=(0.5)
#READ=(0)

printf "Rd\tWt\tZip\tT\tCC\tTP\tP50\tP90\tP99\tP999\tAbt\n"
#for epo in ${EPOCH[@]}
#do
for zip in ${ZIPF[@]}
do
for rd in ${READ[@]}
do
for cc in ${CC_AGS[@]}
do
for t in ${MAX_THD[@]}
do
#for exec_t in ${EXEC_TIME[@]}
#do
	replace 3 "#define CORE_CNT $t" config.h
	replace 4 "#define CC_ALG $cc" config.h
	replace 5 "#define ZIPF_THETA $zip" config.h
	replace 6 "#define READ_PERC $rd" config.h
	replace 10 "#define WORKLOAD YCSB" config.h
	wt=`echo 1 - $rd | bc`
	replace 7 "#define WRITE_PERC $wt" config.h
	#replace 10 "#define EPOCH_LENGTH $epo" config.h
	#replace 14 "#define LONG_TX_EXEC_TIME $exec_t" config.h
	printf "%.2f\t%.2f\t%.2f\t%d\t%s\t%d\t" $rd $wt $zip $t $cc $exec_t
	make clean &> /dev/null
	make -j &> /dev/null
	sleep 2
	timeout 60 ./rundb
	printf "\n"
done
done
done
done
#done
#done
# replace 1 abc config.h
