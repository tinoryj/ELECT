#!/bin/bash

func() {
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