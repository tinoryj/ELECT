#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"

# Exp1: YCSB core workloads, 3-way replication, (6,4) encoding, 60% target storage saving, 10M KV + 1M OP.

ExpName="Exp1-ycsb"
schemes=("cassandra" "elect")
workloads=("workloada" "workloadb" "workloadc" "workloadd" "workloade" "workloadf")
runningTypes=("normal")
KVNumber=100000000
keyLength=24
valueLength=1000
operationNumber=1000000
simulatedClientNumber=16
RunningRoundNumber=1

doEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${}" "${operationNumber}" "${simulatedClientNumber}" "${runningTypes[@]}" "${workloads[@]}" "${RunningRoundNumber}"

# Setup hosts
setupNodeInfo hosts.ini
# Run Exp
for scheme in "${schemes[@]}"; do
    echo "Start experiment of ${scheme}"
    # Load data for evaluation
    loadDataForEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${keyLength}" "${valueLength}" "${operationNumber}" "${simulatedClientNumber}"

    # Run experiment
    doEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${operationNumber}" "${simulatedClientNumber}" "${runningTypes[@]}" "${workloads[@]}" "${RunningRoundNumber}"
done
