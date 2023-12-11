# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#!/bin/bash

func() {

    expName=$1
    stage=$2
    CASSANDRA_PID=$(ps aux | grep CassandraDaemon | grep -v grep | awk '{print $2}')
    echo "Cassandra PID: $CASSANDRA_PID"

    SimpleBase=1

    OUTPUT_MEM="${PathToELECTLog}/${expName}_${stage}_memory_usage.txt"
    OUTPUT_CPU="${PathToELECTLog}/${expName}_${stage}_cpu_usage.txt"
    echo "Memory overhead for Cassandra PID: $CASSANDRA_PID, Exp = ${expName}, stage = $stage" >>"$OUTPUT_MEM"
    echo "CPU overhead for Cassandra PID: $CASSANDRA_PID, Exp = ${expName}, stage = $stage" >>"$OUTPUT_CPU"

    while true; do

        # If Cassandra is running, get its memory usage
        if [[ ! -z "$CASSANDRA_PID" ]]; then
            MEM_USAGE=$(ps -o rss= -p "$CASSANDRA_PID")
            echo "$(date): Cassandra PID $CASSANDRA_PID is using $MEM_USAGE KiB" >>"$OUTPUT_MEM"

            CPU_LOAD=$(top -b -n 1 -p "$CASSANDRA_PID" | grep "$CASSANDRA_PID" | awk '{print $9}')
            echo "$(date): PID $CASSANDRA_PID CPU Load: $CPU_LOAD%" >>"$OUTPUT_CPU"
        else
            echo "$(date): Cassandra is not running" >>"$OUTPUT_MEM"
            exit
        fi

        # Wait for $SimpleBase seconds before the next check
        sleep $SimpleBase
    done
}

func "$1" "$2"
