#!/bin/bash

while true; do
    kill -9 $(ps aux | grep "stats.sh" | grep -v grep | awk 'NR == 1' | awk '{print $2}')
    sleep 1 # You can adjust the sleep duration as needed.
    if [ $? -eq 0 ]; then
        echo "Kill stats.sh successfully."
        exit 0
    else
        echo "Not found stats.sh."
        exit 1
    fi
done
