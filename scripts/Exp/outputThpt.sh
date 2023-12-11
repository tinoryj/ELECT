#!/bin/bash

types=("cassandra" "elect" "mlsm")
runnings=("normal")
workloads=("workloadRead" "workloadWrite")
expName="Exp12-clients"
params=(8 16 32 64 128)

for type in "${types[@]}"; do
    for running in "${runnings[@]}"; do
        for workload in "${workloads[@]}"; do
            for param in "${params[@]}"; do
                echo "Type: ${type}, Running: ${running}, Workload: ${workload}, Parameter: ${param}"
                if [ "${workload}" == "workloadRead" ]; then
                    cat ${expName}-${type}-Run-${running}-*-${workload}-*-$param-* | grep "Throughput" | awk -F',' '{print $NF}'
                elif [ "${workload}" == "workloadWrite" ]; then
                    cat ${expName}-${type}-Run-${running}-*-${workload}-*-$param-* | grep "Throughput" | awk -F',' '{print $NF}'
                fi
            done
        done
    done
done
