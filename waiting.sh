#!/bin/bash
replace()
{
	sed -i "$1s/.*/$2/" $3
}

# CC_AGS=(WOUND_WAIT DLOCK SILO)
CC_AGS=(NO_WAIT WAIT_DIE WOUND_WAIT DLOCK HLOCK SILO MOCC)
# CC_AGS=(MOCC)
MAX_THD=(1 4 8 12 16 20 24 28 32 36)
ZIPF=(0.5)
READ=(0.95)
# WAIT=(1050 2100 4200 8400)
# LONG=(4 16 32 64 128)
printf "Rd\tWt\tZip\tT\tCC\tTP\tP50\tP90\tP99\tP999\tAbt\n"
# for l in ${LONG[@]}
# do
# printf "long tx size = %d\n" $l
for zip in ${ZIPF[@]}
do
for rd in ${READ[@]}
do
for cc in ${CC_AGS[@]}
do
for t in ${MAX_THD[@]}
do
	replace 3 "#define CORE_CNT $t" config.h
	replace 4 "#define CC_ALG $cc" config.h
	replace 5 "#define ZIPF_THETA $zip" config.h
	replace 6 "#define READ_PERC $rd" config.h
	replace 10 "#define WORKLOAD YCSB" config.h
	# replace 165 "#define REQ_PER_QUERY				$l" config.h
	# replace 24 "#define WAIT_CYCLE $w" config.h
	wt=`echo 1 - $rd | bc`
	replace 7 "#define WRITE_PERC $wt" config.h
	printf "%.2f\t%.2f\t%.2f\t%d\t%s\t%d\t" $rd $wt $zip $t $cc $exec_t
	make clean &> /dev/null
	make -j &> /dev/null
	timeout 60 numactl --membind=0 ./rundb
	printf "\n"
done
done
done
done
# done
