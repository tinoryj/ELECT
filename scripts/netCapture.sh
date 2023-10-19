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
    # Network interface (change this to your interface, e.g., wlan0, ens33, etc.)
    INTERFACE="eth0"

    # File to store the results
    OUTPUT_FILE="${expName}_network_summary.txt"

    # Extract the received (RX) and transmitted (TX) bytes for the specified interface
    RX_BYTES=$(cat /proc/net/dev | grep $INTERFACE | awk '{print $2}')
    TX_BYTES=$(cat /proc/net/dev | grep $INTERFACE | awk '{print $10}')

    # Write the results to the file
    echo "Summary for interface: $INTERFACE" >$OUTPUT_FILE
    echo "Bytes received: $RX_BYTES" >>$OUTPUT_FILE
    echo "Bytes sent: $TX_BYTES" >>$OUTPUT_FILE

    echo "Results written to $OUTPUT_FILE"
}

func "$1"