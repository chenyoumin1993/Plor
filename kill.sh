kill -9 `ps -aux | grep run.sh | head -n 1 | awk -F " " '{print $2}'`
kill -9 `ps -aux | grep rundb | head -n 1 | awk -F " " '{print $2}'`
