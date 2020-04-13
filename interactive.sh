#!/bin/bash
replace()
{
	sed -i "$1s/.*/$2/" $3
}

CC_AGS=(WOUND_WAIT DLOCK HLOCK SILO)
# CC_AGS=(DLOCK)
MAX_THD=(1 4 8 12 16 20 24 28 32 36)
# MAX_THD=(36)
ZIPF=(0.99)
READ=(0.5)


# echo "uniform, w/o log"
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
# 	sleep 1
# 	ssh root@aep5 -f "cd /home/chenyoumin/workspace/Ltx; nohup ./rundb" &> /dev/null
# 	sleep 2
# 	timeout 60 numactl --membind=0 ./rundb
# 	ssh root@aep5 "cd /home/chenyoumin/workspace/Ltx; nohup ./kill.sh" &> /dev/null
# 	printf "\n"
# done
# done
# done
# done

echo "bimodal, w/o log"
replace 28 "#define VARY_REQ_CNT 1" config.h
printf "Rd\tWt\tZip\tT\tCC\tTP\tP50\tP90\tP99\tP999\tAbt\n"
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
	# replace 10 "#define WORKLOAD YCSB" config.h
	wt=`echo 1 - $rd | bc`
	replace 7 "#define WRITE_PERC $wt" config.h
	printf "%.2f\t%.2f\t%.2f\t%d\t%s\t%d\t" $rd $wt $zip $t $cc $exec_t
	make clean &> /dev/null
	make -j &> /dev/null
	sleep 1
	ssh root@aep5 -f "cd /home/chenyoumin/workspace/Ltx; nohup ./rundb" &> /dev/null
	sleep 2
	timeout 60 numactl --membind=0 ./rundb
	ssh root@aep5 "cd /home/chenyoumin/workspace/Ltx; nohup ./kill.sh" &> /dev/null
	printf "\n"
done
done
done
done
