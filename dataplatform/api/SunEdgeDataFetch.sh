#!/bin/bash
# Program:
#
# History:
# 2020/01/30  Henry start Sunedge Data Collection program
# Env Setup
PATH=~/bin:/usr/sbin:$PATH

sleep 30

while [ 1 -eq 1 ]
do
  n=`ps -ef | grep SunEdgeDataFetch.jar | awk '!/grep/'`
  if [ -z "$n" ]; then
    java -jar /home/ecoetl/api/SunEdgeDataFetch.jar &
  fi

  sleep 30

done
