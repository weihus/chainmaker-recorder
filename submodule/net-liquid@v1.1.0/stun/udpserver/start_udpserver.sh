#!/bin/bash
pid=`ps -ef | grep ./udpserver |grep -v grep | awk  '{print $2}'`
echo "pid: $pid"
if [ -n "$pid" ]; then
    kill -9 $pid
    echo "kill -9 pid: $pid"
fi

sleep 2

nohup ./udpserver &
