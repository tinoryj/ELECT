#!/bin/bash

types=("cassandra" "elect" "mlsm")
runnings=("normal" "degraded")
workloads=("workloadRead" "workloadWrite" "workloadScan" "workloadUpdate")
params=("AverageLatency" "99thPercentileLatency")
expName="Exp4"

for type in "${types[@]}"; do
    for running in "${runnings[@]}"; do
        for workload in "${workloads[@]}"; do
            for param in "${params[@]}"; do
                echo "Type: ${type}, Running: ${running}, Workload: ${workload}, Parameter: ${param}"
                if [ "${workload}" == "workloadRead" ]; then
                    cat ${expName}-${type}-Run-${running}-*-${workload}-* | grep "\[READ\], ${param}" | awk -F',' '{print $NF}'
                elif [ "${workload}" == "workloadWrite" ]; then
                    cat ${expName}-${type}-Run-${running}-*-${workload}-* | grep "\[INSERT\], ${param}" | awk -F',' '{print $NF}'
                elif [ "${workload}" == "workloadScan" ]; then
                    cat ${expName}-${type}-Run-${running}-*-${workload}-* | grep "\[SCAN\], ${param}" | awk -F',' '{print $NF}'
                elif [ "${workload}" == "workloadUpdate" ]; then
                    cat ${expName}-${type}-Run-${running}-*-${workload}-* | grep "\[UPDATE\], ${param}" | awk -F',' '{print $NF}'
                fi
            done
        done
    done
done
