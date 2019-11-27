#!/bin/bash
replace()
{
	sed -i "$1s/.*/$2/" $3
}

# CC
#CC_AGS=(WAIT_DIE NO_WAIT DL_DETECT TIMESTAMP MVCC HEKATON HSTORE OCC VLL TICTOC SILO)
CC_AGS=(WAIT_DIE NO_WAIT DL_DETECT MVCC OCC HEKATON HSTORE)
MAX_THD=(1 4 8 12 16 20 24 28 32 36) #40 44 48 52 56 60 64)
ZIPF=(0.7 0.9 0.99)
READ=(0 0.1 0.5 0.9 1)

#CC_AGS=(WAIT_DIE)
#MAX_THD=1
#ZIPF=(0.5)
#READ=(0)

printf "Rd\tWt\tZip\tT\tCC\tTP\tP50\tP90\tP99\tP999\tAbt\n"
for zip in ${ZIPF[@]}
do
for rd in ${READ[@]}
do
for cc in ${CC_AGS[@]}
do
for t in ${MAX_THD[@]}
do
	replace 3 "#define THREAD_CNT $t" config.h
	replace 4 "#define CC_ALG $cc" config.h
	replace 5 "#define ZIPF_THETA $zip" config.h
	replace 6 "#define READ_PERC $rd" config.h
	wt=`echo 1 - $rd | bc`
	replace 7 "#define WRITE_PERC $wt" config.h
	printf "%.2f\t%.2f\t%.2f\t%d\t%s\t" $rd $wt $zip $t $cc
	make clean &> /dev/null
	make -j &> /dev/null
	./rundb
done
done
done
done
# replace 1 abc config.h
