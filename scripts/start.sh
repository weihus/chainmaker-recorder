#! /usr/bin/env bash
cd param_adapter && nohup ./param_adapter > param_adapter.log &
PID=$!
echo "PID: $PID"
echo $PID >> ./pids

