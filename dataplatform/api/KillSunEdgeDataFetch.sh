#!/bin/bash
# Program: Reset SunEdgeDataFetch
# History:
# 2020/01/16 Henry
PATH=~/eco:/home/ecoetl:$PATH

pid=$(ps -ef | grep SunEdgeDataFetch.jar | awk '!/grep/{print $2}')

echo "   ***$(date "+%Y-%m-%d %k:%M:%S") Killing process: $pid"
kill -9 $pid

echo $pid Killed.

# sleep 10

# java -jar /home/ecoetl/api/SunEdgeDataFetch.jar &

exit 0

