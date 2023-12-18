#!/bin/bash
source Common.sh
# Exp1: YCSB core workloads, 3-way replication, (6,4) encoding, 60% target storage saving, 10M KV + 1M OP.

ExpName="Exp3-breakdown"
schemes=("cassandra" "elect")
workloads=("workloadRead" "workloadWrite" "workloadScan" "workloadUpdate")
runningTypes=("normal" "degraded")
KVNumber=10000000
keylength=24
fieldlength=1000
operationNumber=1000000
simulatedClientNumber=16
RunningRoundNumber=1

# Setup hosts
setupNodeInfo ./hosts.ini
# Run Exp
for scheme in "${schemes[@]}"; do
    echo "Start experiment of ${scheme}"
    # Load data for evaluation
    loadDataForEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${keylength}" "${fieldlength}" "${operationNumber}" "${simulatedClientNumber}"

    # Run experiment
    doEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${operationNumber}" "${simulatedClientNumber}" "${runningTypes[@]}" "${workloads[@]}" "${RunningRoundNumber}"
done
