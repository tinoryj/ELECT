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

fun "$1"