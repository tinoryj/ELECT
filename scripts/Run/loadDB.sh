#!/bin/bash
. /etc/profile
../Common.sh

func() {
    record_count=$1
    field_length=$2
    threads=$3
    workload=$4
    expName=$5
    keyspace=$6

    echo "Tracing is enabled"
    cd /home/elect/ELECTExp/YCSB || exit

    sed -i "s/recordcount=.*$/recordcount=${record_count}/" workloads/"${workload}"
    sed -i "s/fieldlength=.*$/fieldlength=${field_length}/" workloads/"${workload}"
    file_name="${expName}-${workload}-${record_count}-${field_length}-${threads}-$(date +%s)"
    # nohup bin/ycsb.sh load cassandra-cql -p hosts=$coordinator -threads $threads -s -P workloads/workload_template > logs/insert-log/${file_name}.log 2>&1 &
    bin/ycsb load cassandra-cql -p hosts=${NodesList} -p cassandra.keyspace=${keyspace} -p cassandra.tracing="false" -threads $threads -s -P workloads/"${workload}" >/home/elect/Results/${file_name}.log 2>&1
}

func "$1" "$2" "$3" "$4" "$5" "$6"
