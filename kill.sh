kill -9 `ps -aux | grep ycsb.sh | awk -F " " '{print $2}'`
kill -9 `ps -aux | grep tpcc.sh | awk -F " " '{print $2}'`
kill -9 `ps -aux | grep temp.sh | awk -F " " '{print $2}'`
kill -9 `ps -aux | grep rundb | awk -F " " '{print $2}'`
