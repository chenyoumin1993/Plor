#!/bin/bash
replace()
{
	sed -i "$1s/.*/$2/" $3
}

# CC_AGS=(WOUND_WAIT DLOCK HLOCK SILO)
CC_AGS=(DLOCK)
# CC_AGS=(NO_WAIT WAIT_DIE WOUND_WAIT DLOCK HLOCK SILO)
MAX_THD=(1 2 4 8 12 16 20 24 28 32 36)
# MAX_THD=(1 2 5 9 13 17 21 25 29 33)
# MAX_THD=(20 36)
ZIPF=(0.99)
READ=(0.5)
RATIO=(0.5 0.7 0.9)

replace 28 "#define VARY_REQ_CNT 1" config.h

replace 38 "#define PERSISTENT_LOG 0" config.h
echo "bimodal, w/o log (stored procedure)"
printf "Rd\tWt\tZip\tT\tCC\tTP\tP50\tP90\tP99\tP999\tAbt\n"
# for ratio in ${RATIO[@]}
# do
for zip in ${ZIPF[@]}
do
for rd in ${READ[@]}
do
for cc in ${CC_AGS[@]}
do
for t in ${MAX_THD[@]}
do
	# replace 176 "#define SMALL_RATIO $ratio" config.h
	replace 3 "#define CORE_CNT $t" config.h
	replace 4 "#define CC_ALG $cc" config.h
	replace 5 "#define ZIPF_THETA $zip" config.h
	replace 6 "#define READ_PERC $rd" config.h
	replace 10 "#define WORKLOAD YCSB" config.h
	wt=`echo 1 - $rd | bc`
	replace 7 "#define WRITE_PERC $wt" config.h
	printf "%.2f\t%.2f\t%.2f\t%d\t%s\t%d\t" $rd $wt $zip $t $cc $exec_t
	make clean &> /dev/null
	make -j &> /dev/null
	timeout 60 numactl --membind=0 ./rundb
	printf "\n"
	# pprof --pdf ./rundb profile/prof > tmp/${cc}-${t}.pdf
done
done
done
done
# done

# replace 38 "#define PERSISTENT_LOG 1" config.h
# echo "bimodal, log1"
# printf "Rd\tWt\tZip\tT\tCC\tTP\tP50\tP90\tP99\tP999\tAbt\n"
# for zip in ${ZIPF[@]}
# do
# for rd in ${READ[@]}
# do
# for cc in ${CC_AGS[@]}
# do
# for t in ${MAX_THD[@]}
# do
# 	replace 3 "#define CORE_CNT $t" config.h
# 	replace 4 "#define CC_ALG $cc" config.h
# 	replace 5 "#define ZIPF_THETA $zip" config.h
# 	replace 6 "#define READ_PERC $rd" config.h
# 	replace 10 "#define WORKLOAD YCSB" config.h
# 	wt=`echo 1 - $rd | bc`
# 	replace 7 "#define WRITE_PERC $wt" config.h
# 	printf "%.2f\t%.2f\t%.2f\t%d\t%s\t%d\t" $rd $wt $zip $t $cc $exec_t
# 	make clean &> /dev/null
# 	make -j &> /dev/null
# 	timeout 60 numactl --membind=0 ./rundb
# 	printf "\n"
# done
# done
# done
# done

# echo "bimodal, log2"
# replace 38 "#define PERSISTENT_LOG 2" config.h
# printf "Rd\tWt\tZip\tT\tCC\tTP\tP50\tP90\tP99\tP999\tAbt\n"
# for zip in ${ZIPF[@]}
# do
# for rd in ${READ[@]}
# do
# for cc in ${CC_AGS[@]}
# do
# for t in ${MAX_THD[@]}
# do
# 	replace 3 "#define CORE_CNT $t" config.h
# 	replace 4 "#define CC_ALG $cc" config.h
# 	replace 5 "#define ZIPF_THETA $zip" config.h
# 	replace 6 "#define READ_PERC $rd" config.h
# 	replace 10 "#define WORKLOAD YCSB" config.h
# 	wt=`echo 1 - $rd | bc`
# 	replace 7 "#define WRITE_PERC $wt" config.h
# 	printf "%.2f\t%.2f\t%.2f\t%d\t%s\t%d\t" $rd $wt $zip $t $cc $exec_t
# 	make clean &> /dev/null
# 	make -j &> /dev/null
# 	timeout 60 numactl --membind=0 ./rundb
# 	printf "\n"
# done
# done
# done
# done

# echo "uniform, w log"
# replace 28 "#define VARY_REQ_CNT 0" config.h
# replace 38 "#define PERSISTENT_LOG 1" config.h
# printf "Rd\tWt\tZip\tT\tCC\tTP\tP50\tP90\tP99\tP999\tAbt\n"
# for zip in ${ZIPF[@]}
# do
# for rd in ${READ[@]}
# do
# for cc in ${CC_AGS[@]}
# do
# for t in ${MAX_THD[@]}
# do
# 	replace 3 "#define CORE_CNT $t" config.h
# 	replace 4 "#define CC_ALG $cc" config.h
# 	replace 5 "#define ZIPF_THETA $zip" config.h
# 	replace 6 "#define READ_PERC $rd" config.h
# 	replace 10 "#define WORKLOAD YCSB" config.h
# 	wt=`echo 1 - $rd | bc`
# 	replace 7 "#define WRITE_PERC $wt" config.h
# 	printf "%.2f\t%.2f\t%.2f\t%d\t%s\t%d\t" $rd $wt $zip $t $cc $exec_t
# 	make clean &> /dev/null
# 	make -j &> /dev/null
# 	timeout 60 numactl --membind=0 ./rundb
# 	printf "\n"
# done
# done
# done
# done

# echo "bimodal, w log"
# replace 28 "#define VARY_REQ_CNT 1" config.h
# printf "Rd\tWt\tZip\tT\tCC\tTP\tP50\tP90\tP99\tP999\tAbt\n"
# for zip in ${ZIPF[@]}
# do
# for rd in ${READ[@]}
# do
# for cc in ${CC_AGS[@]}
# do
# for t in ${MAX_THD[@]}
# do
# 	replace 3 "#define CORE_CNT $t" config.h
# 	replace 4 "#define CC_ALG $cc" config.h
# 	replace 5 "#define ZIPF_THETA $zip" config.h
# 	replace 6 "#define READ_PERC $rd" config.h
# 	replace 10 "#define WORKLOAD YCSB" config.h
# 	wt=`echo 1 - $rd | bc`
# 	replace 7 "#define WRITE_PERC $wt" config.h
# 	printf "%.2f\t%.2f\t%.2f\t%d\t%s\t%d\t" $rd $wt $zip $t $cc $exec_t
# 	make clean &> /dev/null
# 	make -j &> /dev/null
# 	timeout 60 numactl --membind=0 ./rundb
# 	printf "\n"
# done
# done
# done
# done

