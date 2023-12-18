#!/bin/bash
. /etc/profile
../Common.sh

func() {
    record_count=$1
    key_length=$2
    field_length=$3
    threads=$4
    workload=$5
    expName=$6
    keyspace=$7

    echo "Tracing is enabled"
    cd /home/elect/ELECTExp/YCSB || exit

    sed -i "s/recordcount=.*$/recordcount=${record_count}/" workloads/"${workload}"
    sed -i "s/keylength=.*$/keylength=${field_length}/" workloads/"${workload}"
    sed -i "s/fieldlength=.*$/fieldlength=${field_length}/" workloads/"${workload}"
    file_name="${expName}-${workload}-KVNumber-${record_count}-KeySize-${key_length}-ValueSize-${field_length}-ClientNumber-${threads}-$(date +%s)"

    nohup bin/ycsb load cassandra-cql -p hosts=${NodesList} -p cassandra.keyspace=${keyspace} -p cassandra.tracing="false" -threads $threads -s -P workloads/"${workload}" >${PathToELECTLog}/${file_name}.log 2>&1 &
}

func "$1" "$2" "$3" "$4" "$5" "$6" "$7"
