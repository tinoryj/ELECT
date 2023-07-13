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

. /etc/profile

func() {
    coordinator=$1
    operationcount=$2
    threads=$3
    consistency=$4
    file_name="$(date +%s)-${operationcount}-${threads}"
    cd ~/ycsb-0.17.0
    mkdir -p logs/run-log/
    
    sed -i "s/operationcount=.*$/operationcount=${operationcount}/" workloads/workload_template
    
    
    bin/ycsb run cassandra-cql -p hosts=$coordinator -p cassandra.readconsistencylevel="$consistency" -threads $threads -s -P workloads/workload_template > logs/run-log/${file_name}.log 2>&1
}

func "$1" "$2" "$3" "$4"


