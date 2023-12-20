#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"

recordcount=$1
operationcount=$2
key_length=$3
field_length=$4
threads=$5
workload=$6
expName=$7
keyspace=$8
consistency=$9

cd ${PathToYCSB} || exit

sed -i "s/recordcount=.*$/recordcount=${recordcount}/" workloads/"${workload}"
sed -i "s/operationcount=.*$/operationcount=${operationcount}/" workloads/"${workload}"
sed -i "s/keylength=.*$/keylength=${key_length}/" workloads/"${workload}"
sed -i "s/fieldlength=.*$/fieldlength=${field_length}/" workloads/"${workload}"

mode="cassandra"
if [ "$keyspace" == "ycsb" ]; then
    mode="elect"
fi

file_name="${expName}-Scheme-${mode}-${workload}-KVNumber-${recordcount}-OPNumber-${operationcount}-ClientNumber-${threads}-Time-$(date +%s)"

bin/ycsb.sh run cassandra-cql -p hosts=${NodesList} -p cassandra.readconsistencylevel=${consistency} -p cassandra.keyspace=${keyspace} -p cassandra.tracing="false" -threads $threads -s -P workloads/"${workload}" >${PathToELECTResultSummary}/"${file_name}".log 2>&1
