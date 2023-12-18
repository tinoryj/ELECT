#!/bin/bash

. /etc/profile

recordcount=$1
operationcount=$2
threads=$3
workload=$4
expName=$5
keyspace=$6
consistency=$7

cd /home/elect/ELECTExp/YCSB || exit

sed -i "s/recordcount=.*$/recordcount=${recordcount}/" workloads/"${workload}"
sed -i "s/operationcount=.*$/operationcount=${operationcount}/" workloads/"${workload}"

file_name="${expName}-${workload}-${recordcount}-${operationcount}-${threads}-$(date +%s)"

# bin/ycsb run cassandra-cql -p hosts=${NodesList} -p cassandra.readconsistencylevel=${consistency} -p cassandra.keyspace=${keyspace} -p cassandra.tracing="true" -threads 1 -s -P workloads/"${workload}" >${PathToELECTLog}/"${file_name}".log 2>&1

bin/ycsb run cassandra-cql -p hosts=${NodesList} -p cassandra.readconsistencylevel=${consistency} -p cassandra.keyspace=${keyspace} -p cassandra.tracing="false" -threads $threads -s -P workloads/"${workload}" >${PathToELECTLog}/"${file_name}".log 2>&1

