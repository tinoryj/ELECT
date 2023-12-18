#!/bin/bash
. /etc/profile
func() {

    recordcount=$1
    operationcount=$2
    threads=$3
    workload=$4
    expName=$5
    keyspace=$6

    dirName="${expName}-${workload}-${keyspace}-${recordcount}-${operationcount}-${threads}-$(date +%s)-Log"

    cp -r ${PathToELECTPrototype}/logs ${PathToELECTLog}/$dirName
}

func "$1" "$2" "$3" "$4" "$5" "$6"
