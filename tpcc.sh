#!/bin/bash
replace()
{
	sed -i "$1s/.*/$2/" $3
}

CC_AGS=(WAIT_DIE NO_WAIT)
#CC_AGS=(WOUND_WAIT DLOCK SILO)
MAX_THD=(1 4 8 12 16 20 24 28 32 36)
#MAX_THD=(1 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60 64)

printf "\tT\tCC\tTP\tP50\tP90\tP99\tP999\tAbt\n"
for cc in ${CC_AGS[@]}
do
for t in ${MAX_THD[@]}
do
	replace 3 "#define CORE_CNT $t" config.h
	replace 4 "#define CC_ALG $cc" config.h
	printf "TPCC\t%s\t%d\t" $cc $t
	make clean &> /dev/null
	make -j &> /dev/null
	sleep 2
	timeout 10 ./rundb
	printf "\n"
done
done
