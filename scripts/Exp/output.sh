#!/bin/bash

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
