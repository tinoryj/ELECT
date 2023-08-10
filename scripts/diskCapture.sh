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

    kill -9 $(ps aux | grep "diskCapture.sh" | grep -v grep | awk 'NR == 1'  | awk {'print $2'})
    expName=$1
    # Mount point
    MOUNT_POINT="/mnt/ssd"

    # File to store the results
    OUTPUT_FILE="${expName}_disk_io_total.txt"

    # Get the device associated with the mount point (e.g., sda, sdb)
    DEVICE=$(df --output=source "$MOUNT_POINT" | tail -1 | awk -F'/' '{print $NF}')

    # Extract the read and write statistics for the device from /proc/diskstats
    # Fields:
    # 3 - Device name
    # 6 - Number of sectors read
    # 10 - Number of sectors written
    SECTOR_SIZE=512 # Default sector size in bytes
    STATS=$(grep "$DEVICE " /proc/diskstats | awk -v size=$SECTOR_SIZE '{print $3 " " $6*size/1024 " " $10*size/1024}')

    DEVICE_NAME=$(echo "$STATS" | awk '{print $1}')
    TOTAL_KB_READ=$(echo "$STATS" | awk '{print $2}')
    TOTAL_KB_WRITTEN=$(echo "$STATS" | awk '{print $3}')

    # Write the results to the file
    echo "Summary for device: $DEVICE_NAME (mounted at $MOUNT_POINT)" >$OUTPUT_FILE
    echo "Total KiB read: $TOTAL_KB_READ" >>$OUTPUT_FILE
    echo "Total KiB written: $TOTAL_KB_WRITTEN" >>$OUTPUT_FILE

    echo "Results written to $OUTPUT_FILE"
}

func "$1"