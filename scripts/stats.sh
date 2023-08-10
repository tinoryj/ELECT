#!/bin/bash

func() {
    expName=$1
    CASSANDRA_PID=$(ps aux | grep CassandraDaemon | grep -v grep | awk '{print $2}')
    echo "Cassandra PID: $CASSANDRA_PID"

    SimpleBase=10

    OUTPUT_MEM="${expName}_memory_usage.txt"
    OUTPUT_CPU="${expName}_cpu_usage.txt"

    while true; do

        # If Cassandra is running, get its memory usage
        if [[ ! -z "$CASSANDRA_PID" ]]; then
            MEM_USAGE=$(ps -o rss= -p $CASSANDRA_PID)
            echo "$(date): Cassandra PID $CASSANDRA_PID is using $MEM_USAGE KiB" >>$OUTPUT_MEM

            CPU_LOAD=$(top -b -n 1 -p "$CASSANDRA_PID" | grep "$CASSANDRA_PID" | awk '{print $9}')
            echo "$(date): PID $CASSANDRA_PID CPU Load: $CPU_LOAD%" >>$OUTPUT_CPU
        else
            echo "$(date): Cassandra is not running" >>$OUTPUT_FILE
            exit
        fi

        # Wait for $SimpleBase seconds before the next check
        sleep $SimpleBase
    done
}

func "$1"
