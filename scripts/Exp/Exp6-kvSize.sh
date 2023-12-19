#!/bin/bash
. /etc/profile
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/../Common.sh"
# Exp1: YCSB core workloads, 3-way replication, (6,4) encoding, 60% target storage saving, 10M KV + 1M OP.

ExpName="Exp6-kvSize"
schemes=("cassandra" "elect")
workloads=("workloadRead")
runningTypes=("normal" "degraded")
KVNumber=10000000
keylengthSet=(8 16 32 64 128)
fieldlengthSet=(32 128 512 2048 8192)
operationNumber=1000000
simulatedClientNumber=16
RunningRoundNumber=1

# Setup hosts
setupNodeInfo ./hosts.ini
# Run Exp
for scheme in "${schemes[@]}"; do
    echo "Start experiment of ${scheme}"
    for keylength in "${keylengthSet[@]}"; do
        for fieldlength in "${fieldlengthSet[@]}"; do
            # Load data for evaluation
            loadDataForEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${keylength}" "${fieldlength}" "${operationNumber}" "${simulatedClientNumber}"

            # Run experiment
            doEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${operationNumber}" "${simulatedClientNumber}" "${runningTypes[@]}" "${workloads[@]}" "${RunningRoundNumber}"
        done
    done
done
