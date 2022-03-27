#!/bin/bash
replace()
{
	sed -i "$1s/.*/$2/" $3
}

CC_AGS=(PLOR)
MAX_THD=(1 4 8 12 16 20 24 28 32 36)
RT=(5000)

replace 38 "#define PERSISTENT_LOG 0" config.h
replace 30 "#define INTERACTIVE_MODE 0" config.h

for rt in ${RT[@]}
do
	echo $rt
for cc in ${CC_AGS[@]}
do
for t in ${MAX_THD[@]}
do
	replace 3 "#define CORE_CNT $t" config.h
	replace 4 "#define CC_ALG $cc" config.h
	replace 44 "#define RS_FACTOR $rt" config.h
	printf "%d\t%s\t" $t $cc
	make clean &> /dev/null
	make -j &> /dev/null
	sleep 2
	timeout 60 numactl --membind=0 ./rundb
	printf "\n"
done
done
done
