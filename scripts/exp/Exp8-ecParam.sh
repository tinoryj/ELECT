#!/bin/bash
. /etc/profile
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/../common.sh"
# Exp8: YCSB core workloads, 3-way replication, (K+2,k) encoding, 60% target storage saving, 10M KV + 1M OP.

ExpName="Exp8-ecParam"
schemes=("cassandra" "elect")
workloads=("workloadRead" "workloadWrite")
runningTypes=("normal" "degraded")
KVNumber=10000000
keylength=24
fieldlength=1000
operationNumber=1000000
simulatedClientNumber=16
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
        loadDataForEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${keylength}" "${fieldlength}" "${operationNumber}" "${simulatedClientNumber}" "${storageSavingTarget}" "${erasureCodingK}"

        # Run experiment
        doEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${operationNumber}" "${simulatedClientNumber}" "${runningTypes[@]}" "${workloads[@]}" "${RunningRoundNumber}"
    done
done
