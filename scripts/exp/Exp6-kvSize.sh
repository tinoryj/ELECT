#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"
# Exp6: YCSB core workloads, 3-way replication, (6,4) encoding, 60% target storage saving, 10M KV + 1M OP, vary KV sizes.

ExpName="Exp6-kvSize"
schemes=("cassandra" "elect")
workloads=("workloadRead")
runningTypes=("normal" "degraded")
KVNumber=10000000
keyLengthSet=(8 16 32 64 128)
valueLengthSet=(32 128 512 2048 8192)
fixedKeylength=32
fixedFieldlength=512
operationNumber=1000000
simulatedClientNumber=32
RunningRoundNumber=1

# Setup hosts
setupNodeInfo ./hosts.ini
# Run Exp
for scheme in "${schemes[@]}"; do
    echo "Start experiment of ${scheme}"
    for keyLength in "${keyLengthSet[@]}"; do
        # Load data for evaluation
        loadDataForEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${fixedFieldlength}" "${operationNumber}" "${simulatedClientNumber}"

        # Run experiment
        for workload in "${workloads[@]}"; do
            for runningMode in "${runningTypes[@]}"; do
                # Run experiment
                doEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${valueLength}" "${operationNumber}" "${simulatedClientNumber}" "${RunningRoundNumber}" "${runningMode}" "${workload}"
            done
        done
    done

    for valueLength in "${valueLengthSet[@]}"; do
        # Load data for evaluation
        loadDataForEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${fixedKeylength}" "${valueLength}" "${operationNumber}" "${simulatedClientNumber}"

        # Run experiment
        for workload in "${workloads[@]}"; do
            for runningMode in "${runningTypes[@]}"; do
                # Run experiment
                doEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${valueLength}" "${operationNumber}" "${simulatedClientNumber}" "${RunningRoundNumber}" "${runningMode}" "${workload}"
            done
        done
    done
done