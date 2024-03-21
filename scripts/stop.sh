#! /usr/bin/env bash
while read line; do
        echo "Kill process #$line"
        kill $line
done <./pids

rm ./pids
touch ./pids
