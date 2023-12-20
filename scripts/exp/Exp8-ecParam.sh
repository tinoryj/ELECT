#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"
# Exp8: YCSB core workloads, 3-way replication, (K+2,k) encoding, 60% target storage saving, 10M KV + 1M OP.

ExpName="Exp8-ecParam"
schemes=("cassandra" "elect")
workloads=("workloadRead" "workloadWrite")
runningTypes=("normal" "degraded")
KVNumber=10000000
keyLength=24
valueLength=1000
operationNumber=1000000
simulatedClientNumber=32
RunningRoundNumber=1
erasureCodingKSet=(4 5 6 7 8)
storageSavingTarget=0.6

# Setup hosts
setupNodeInfo ./hosts.ini
# Run Exp
for scheme in "${schemes[@]}"; do
    for erasureCodingK in "${erasureCodingKSet[@]}"; do
        echo "Start experiment of ${scheme} with erasure coding K=${erasureCodingK}"
        # Load data for evaluation
        loadDataForEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${valueLength}" "${operationNumber}" "${simulatedClientNumber}" "${storageSavingTarget}" "${erasureCodingK}"

        # Run experiment
        for workload in "${workloads[@]}"; do
            for runningMode in "${runningTypes[@]}"; do
                # Run experiment
                doEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${valueLength}" "${operationNumber}" "${simulatedClientNumber}" "${RunningRoundNumber}" "${runningMode}" "${workload}"
            done
        done
    done
done
