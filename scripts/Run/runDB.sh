#!/bin/bash

. /etc/profile

nodes10="172.27.96.1,172.27.96.2,172.27.96.3,172.27.96.4,172.27.96.5,172.27.96.6,172.27.96.7,172.27.96.8,172.27.96.9,172.27.96.10"
nodes20="172.27.96.1,172.27.96.2,172.27.96.3,172.27.96.4,172.27.96.5,172.27.96.6,172.27.96.7,172.27.96.8,172.27.96.9,172.27.96.10,172.27.96.11,172.27.96.12,172.27.96.13,172.27.96.14,172.27.96.15,172.27.96.16,172.27.96.17,172.27.96.18,172.27.96.19,172.27.96.20"
nodes10New="172.27.96.11,172.27.96.12,172.27.96.13,172.27.96.14,172.27.96.15,172.27.96.16,172.27.96.17,172.27.96.18,172.27.96.19,172.27.96.20"
nodes30="172.27.96.1,172.27.96.2,172.27.96.3,172.27.96.4,172.27.96.5,172.27.96.6,172.27.96.7,172.27.96.8,172.27.96.9,172.27.96.10,172.27.96.11,172.27.96.12,172.27.96.13,172.27.96.14,172.27.96.15,172.27.96.16,172.27.96.17,172.27.96.18,172.27.96.19,172.27.96.20,172.27.96.21,172.27.96.22,172.27.96.23,172.27.96.24,172.27.96.25,172.27.96.26,172.27.96.27,172.27.96.28,172.27.96.29,172.27.96.30"
nodes40="172.27.96.1,172.27.96.2,172.27.96.3,172.27.96.4,172.27.96.5,172.27.96.6,172.27.96.7,172.27.96.8,172.27.96.9,172.27.96.10,172.27.96.11,172.27.96.12,172.27.96.13,172.27.96.14,172.27.96.15,172.27.96.16,172.27.96.17,172.27.96.18,172.27.96.19,172.27.96.20,172.27.96.21,172.27.96.22,172.27.96.23,172.27.96.24,172.27.96.25,172.27.96.26,172.27.96.27,172.27.96.28,172.27.96.29,172.27.96.30,172.27.96.31,172.27.96.32,172.27.96.33,172.27.96.34,172.27.96.35,172.27.96.36,172.27.96.37,172.27.96.38,172.27.96.39,172.27.96.40"

recordcount=$1
operationcount=$2
threads=$3
workload=$4
expName=$5
keyspace=$6
consistency=$7
trace=$8

NodesList=$nodes10
echo "${NodesList}"

cd /home/elect/ELECTExp/YCSB || exit

sed -i "s/recordcount=.*$/recordcount=${recordcount}/" workloads/"${workload}"
sed -i "s/operationcount=.*$/operationcount=${operationcount}/" workloads/"${workload}"

file_name="${expName}-${workload}-${recordcount}-${operationcount}-${threads}-$(date +%s)"

# bin/ycsb run cassandra-cql -p hosts=${NodesList} -p cassandra.readconsistencylevel=${consistency} -p cassandra.keyspace=${keyspace} -p cassandra.tracing="true" -threads 1 -s -P workloads/"${workload}" >/home/elect/Results/"${file_name}".log 2>&1

bin/ycsb run cassandra-cql -p hosts=${NodesList} -p cassandra.readconsistencylevel=${consistency} -p cassandra.keyspace=${keyspace} -p cassandra.tracing="false" -threads $threads -s -P workloads/"${workload}" >/home/elect/Results/"${file_name}".log 2>&1

if [ $trace == "on" ]; then
    echo "Tracing is enabled, copy trace log to NFS"
    mv /home/elect/Results/"${file_name}".log /mnt/nfs
fi
