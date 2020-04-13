#!/bin/bash
replace()
{
	sed -i "$1s/.*/$2/" $3
}

CC_AGS=(WAIT_DIE)
# CC_AGS=(NO_WAIT WAIT_DIE WOUND_WAIT DLOCK HLOCK SILO)
#MAX_THD=(1 4 8 12 16 20 24 28 32 36)
# CC_AGS=(SILO)
MAX_THD=(1 4 8 12 16 20 24 28 32 36)
#MAX_THD=(36 40 44 48 52 56 60 64)

replace 10 "#define WORKLOAD TPCC" config.h

replace 38 "#define PERSISTENT_LOG 0" config.h
echo "w/o log"
printf "\tT\tCC\tTP\tP50\tP90\tP99\tP999\tAbt\n"
# replace 171 "#define NUM_WH 1" config.h
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
	timeout 60 numactl --membind=0 ./rundb
	printf "\n"
done
done

# replace 38 "#define PERSISTENT_LOG 1" config.h
# echo "log 1"
# printf "\tT\tCC\tTP\tP50\tP90\tP99\tP999\tAbt\n"
# # replace 171 "#define NUM_WH 1" config.h
# for cc in ${CC_AGS[@]}
# do
# for t in ${MAX_THD[@]}
# do
# 	replace 3 "#define CORE_CNT $t" config.h
# 	replace 4 "#define CC_ALG $cc" config.h
# 	printf "TPCC\t%s\t%d\t" $cc $t
# 	make clean &> /dev/null
# 	make -j &> /dev/null
# 	sleep 2
# 	timeout 60 numactl --membind=0 ./rundb
# 	printf "\n"
# done
# done

# echo "log 2"
# replace 38 "#define PERSISTENT_LOG 2" config.h
# printf "\tT\tCC\tTP\tP50\tP90\tP99\tP999\tAbt\n"
# # replace 171 "#define NUM_WH 1" config.h
# for cc in ${CC_AGS[@]}
# do
# for t in ${MAX_THD[@]}
# do
# 	replace 3 "#define CORE_CNT $t" config.h
# 	replace 4 "#define CC_ALG $cc" config.h
# 	printf "TPCC\t%s\t%d\t" $cc $t
# 	make clean &> /dev/null
# 	make -j &> /dev/null
# 	sleep 2
# 	timeout 60 numactl --membind=0 ./rundb
# 	printf "\n"
# done
# done

# echo "4 WH"
# replace 171 "#define NUM_WH 4" config.h
# for cc in ${CC_AGS[@]}
# do
# for t in ${MAX_THD[@]}
# do
# 	replace 3 "#define CORE_CNT $t" config.h
# 	replace 4 "#define CC_ALG $cc" config.h
# 	printf "TPCC\t%s\t%d\t" $cc $t
# 	make clean &> /dev/null
# 	make -j &> /dev/null
# 	sleep 2
# 	timeout 60 numactl --membind=0 ./rundb
# 	printf "\n"
# done
# done

# echo "N WH"
# replace 171 "#define NUM_WH THREAD_CNT" config.h
# for cc in ${CC_AGS[@]}
# do
# for t in ${MAX_THD[@]}
# do
# 	replace 3 "#define CORE_CNT $t" config.h
# 	replace 4 "#define CC_ALG $cc" config.h
# 	printf "TPCC\t%s\t%d\t" $cc $t
# 	make clean &> /dev/null
# 	make -j &> /dev/null
# 	sleep 2
# 	timeout 60 numactl --membind=0 ./rundb
# 	printf "\n"
# done
# done