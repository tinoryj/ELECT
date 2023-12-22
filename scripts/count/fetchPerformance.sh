#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"

# Fetch performance data from log files.
expName=$1
runningMode=$2
workload=$3
recordcount=$4
operationcount=$5
key_length=$6
field_length=$7
threads=$8

fileNameForLoad="${expName}-Load-Scheme-${runningMode}-${workload}-KVNumber-${recordcount}-KeySize-${key_length}-ValueSize-${field_length}-ClientNumber-${threads}-Time-$(date +%s)"
fileNameForRun="${expName}-Run-Scheme-${runningMode}-${workload}-KVNumber-${recordcount}-OPNumber-${operationcount}-KeySize-${key_length}-ValueSize-${field_length}-ClientNumber-${threads}-Time-$(date +%s)"


file_list=()

for file in "$search_dir"/*; do
    if [[ $file =~ .*-Load-Scheme-.*-KVNumber-.*-KeySize-.*-ValueSize-.*-ClientNumber-.*-Time-.* ]]; then
        file_list+=("$file")
    elif [[ $file =~ .*-Run-Scheme-.*-KVNumber-.*-OPNumber-.*-KeySize-.*-ValueSize-.*-ClientNumber-.*-Time-.* ]]; then
        file_list+=("$file")
    fi
done


possibleOperationTypeSet=(READ INSERT SCAN UPDATE)
benchmarkTypeSet=("AverageLatency" "25thPercentileLatency" "50thPercentileLatency" "75thPercentileLatency" "90thPercentileLatency" "99thPercentileLatency")

if [ "${workload}" == "workloadReadKV" ]; then
    cat ${expName}-${type}-Run-${running}-*${additionalLabel}*-${workload}-* | grep "\[READ\], ${param}" | awk -F',' '{print $NF}'
elif [ "${workload}" == "workloadWriteKV" ]; then
    cat ${expName}-${type}-Run-${running}-*${additionalLabel}*-${workload}-* | grep "\[INSERT\], ${param}" | awk -F',' '{print $NF}'
elif [ "${workload}" == "workloadScan" ]; then
    cat ${expName}-${type}-Run-${running}-*${additionalLabel}*-${workload}-* | grep "\[SCAN\], ${param}" | awk -F',' '{print $NF}'
elif [ "${workload}" == "workloadUpdate" ]; then
    cat ${expName}-${type}-Run-${running}-*${additionalLabel}*-${workload}-* | grep "\[UPDATE\], ${param}" | awk -F',' '{print $NF}'
fi


types=("cassandra" "elect")
runnings=("normal" "degraded")
workloads=("workloadReadKV" "workloadWriteKV")
params=("AverageLatency" "25thPercentileLatency" "50thPercentileLatency" "75thPercentileLatency" "90thPercentileLatency" "99thPercentileLatency" "999thPercentileLatency")
additionalSections=("Key-8-Value-512" "Key-16-Value-512" "Key-32-Value-512" "Key-64-Value-512" "Key-128-Value-512" "Key-32-Value-32" "Key-32-Value-128" "Key-32-Value-2048" "Key-32-Value-8192")
expName="Exp8-VaryKV"

for type in "${types[@]}"; do
    for additionalLabel in "${additionalSections[@]}"; do
        for running in "${runnings[@]}"; do
            for workload in "${workloads[@]}"; do
                for param in "${params[@]}"; do
                    echo "Type: ${type}, Running: ${running}, Workload: ${workload}, Parameter: ${param}, Additional Label: ${additionalLabel}"
                    if [ "${workload}" == "workloadReadKV" ]; then
                        cat ${expName}-${type}-Run-${running}-*${additionalLabel}*-${workload}-* | grep "\[READ\], ${param}" | awk -F',' '{print $NF}'
                    elif [ "${workload}" == "workloadWriteKV" ]; then
                        cat ${expName}-${type}-Run-${running}-*${additionalLabel}*-${workload}-* | grep "\[INSERT\], ${param}" | awk -F',' '{print $NF}'
                    elif [ "${workload}" == "workloadScan" ]; then
                        cat ${expName}-${type}-Run-${running}-*${additionalLabel}*-${workload}-* | grep "\[SCAN\], ${param}" | awk -F',' '{print $NF}'
                    elif [ "${workload}" == "workloadUpdate" ]; then
                        cat ${expName}-${type}-Run-${running}-*${additionalLabel}*-${workload}-* | grep "\[UPDATE\], ${param}" | awk -F',' '{print $NF}'
                    fi
                done
            done
        done
    done
done
