#!/bin/bash

types=("elect")
runnings=("normal" "degraded")
workloads=("workloadRead" "workloadWrite")
TSSs=("0.1" "0.2" "0.3" "0.4" "0.5" "0.7" "0.8" "0.9")
expName="Exp6"

for type in "${types[@]}"; do
    for running in "${runnings[@]}"; do
        for workload in "${workloads[@]}"; do
            for TSS in "${TSSs[@]}"; do
                echo "Type: ${type}, Running: ${running}, TSS: ${TSS} Workload: ${workload}"
                cat ${expName}-${type}-Run-${running}-TSS-${TSS}-*-${workload}-* | grep "Throughput" | awk -F',' '{print $NF}'
            done
        done
    done
done

TSSs=("0.1" "0.2" "0.3" "0.4" "0.5" "0.7" "0.8" "0.9")
nodes=("1" "2" "3" "4" "5" "6" "7" "8" "9" "10")

for TSS in "${TSSs[@]}"; do
    echo "TSS: ${TSS}"
    for node in "${nodes[@]}"; do
        ssh node${node} "du -s PATH_TO_DB_BACKUP/elect/Exp6-${TSS}"
    done
done
