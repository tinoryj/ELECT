#!/bin/bash
. /etc/profile
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/../Common.sh"
# Exp4: YCSB core workloads, 3-way replication, (6,4) encoding, 60% target storage saving, recovery performance.

ExpName="Exp4-recovery"
schemes=("cassandra" "elect")
KVNumberSet=(10000000 20000000 30000000)
keylength=24
fieldlength=1000
operationNumber=1000000
simulatedClientNumber=16
RunningRoundNumber=1
recoveryNode=($(shuf -i 1-${NodeNumber} -n 1))

# Setup hosts
setupNodeInfo ./hosts.ini
# Run Exp
for scheme in "${schemes[@]}"; do
    echo "Start experiment of ${scheme}"
    # Load data for evaluation
    for KVNumber in "${KVNumberSet[@]}"; do
        loadDataForEvaluation "${ExpName}" "${scheme}" "${KVNumber}" "${keylength}" "${fieldlength}" "${operationNumber}" "${simulatedClientNumber}"

        # Run experiment
        startupFromBackup "${ExpName}" "${scheme}" "${KVNumber}" "${keylength}" "${fieldlength}"
        recovery "${ExpName}" "${scheme}" "${recoveryNode}" "${KVNumber}" "${RunningRoundNumber}"
    done
done
