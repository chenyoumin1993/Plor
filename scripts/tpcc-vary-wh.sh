#!/bin/bash
replace()
{
	sed -i "$1s/.*/$2/" $3
}

# CC_AGS=(WAIT_DIE)
CC_AGS=(NO_WAIT WAIT_DIE WOUND_WAIT PLOR HLOCK SILO MOCC TICTOC)
# MAX_THD=(1 4 8 12 16 20 24 28 32 36)
# CC_AGS=(TICTOC)
#MAX_THD=(1 4 8 12 16 20 24 28 32 36)
thd_num=20
WH_NUM=(1 4 8 12 16 20)
#MAX_THD=(36 40 44 48 52 56 60 64)

replace 10 "#define WORKLOAD TPCC" config.h

replace 38 "#define PERSISTENT_LOG 0" config.h
replace 30 "#define INTERACTIVE_MODE 0" config.h
replace 3 "#define CORE_CNT ${thd_num}" config.h

# replace 171 "#define NUM_WH 1" config.h
for cc in ${CC_AGS[@]}
do
for wh in ${WH_NUM[@]}
do
	replace 4 "#define CC_ALG $cc" config.h
	replace 176 "#define NUM_WH $wh" config.h
	printf "TPCC\t%s\t%d\t" $cc $wh
	make clean &> /dev/null
	make -j &> /dev/null
	sleep 2
	timeout 60 numactl --membind=0 ./rundb
	printf "\n"
done
done
