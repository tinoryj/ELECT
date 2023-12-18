#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/settings.sh"

playbookSet=(playbook-load.yaml playbook-run.yaml playbook-flush.yaml playbook-backup.yaml playbook-startup.yaml playbook-fail.yaml playbook-recovery.yaml)

function generate_tokens {
    python3 ${SCRIPT_DIR}/genToken.py ${NodeNumber} >${SCRIPT_DIR}/token.txt
    readarray -t lines <${SCRIPT_DIR}/token.txt
    local -a tokens=()
    for line in "${lines[@]}"; do
        if [[ $line == *"initial_token:"* ]]; then
            token=$(echo $line | grep -oP '(?<=initial_token: )[-0-9]+')
            tokens+=("$token") 
        fi
    done
    rm -rf ${SCRIPT_DIR}/token.txt
    echo ${tokens[*]}
}

function setupNodeInfo {
    targetHostInfo=$1
    if [ -f ${SCRIPT_DIR}/Exp/${targetHostInfo} ]; then
        rm -rf ${SCRIPT_DIR}/Exp/${targetHostInfo}
    fi
    echo "[elect_servers]" >>${SCRIPT_DIR}/Exp/${targetHostInfo}
    for ((i = 1; i <= NodeNumber; i++)); do
        echo "server${i} ansible_host=${NodesList[(($i - 1))]}" >>${SCRIPT_DIR}/Exp/${targetHostInfo}
    done
    echo >>${SCRIPT_DIR}/Exp/${targetHostInfo}
    echo "[elect_oss]" >>${SCRIPT_DIR}/Exp/${targetHostInfo}
    echo "oss ansible_host=${OSSServerNode}" >>${SCRIPT_DIR}/Exp/${targetHostInfo}
    echo >>${SCRIPT_DIR}/Exp/${targetHostInfo}
    echo "[elect_client]" >>${SCRIPT_DIR}/Exp/${targetHostInfo}
    echo "client ansible_host=${ClientNode}" >>${SCRIPT_DIR}/Exp/${targetHostInfo}
    # random select the failure node from the total node list
    failure_nodes=($(shuf -i 1-${NodeNumber} -n 1))
    echo >>${SCRIPT_DIR}/Exp/${targetHostInfo}
    echo "[elect_failure]" >>${SCRIPT_DIR}/Exp/${targetHostInfo}
    echo "server${failure_nodes} ansible_host=${NodesList[(($failure_nodes - 1))]}" >>${SCRIPT_DIR}/Exp/${targetHostInfo}

    # Setup user ID for each playbook
    for playbook in "${playbookSet[@]}"; do
        if [ ! -f ${playbook} ]; then
            cp ${SCRIPT_DIR}/playbook/${playbook} ${SCRIPT_DIR}/Exp/${playbook}
        else
            rm -rf ${SCRIPT_DIR}/Exp/${playbook}
            cp ${SCRIPT_DIR}/playbook/${playbook} ${SCRIPT_DIR}/Exp/${playbook}
        fi
        sed -i "s/\(become_user: \)".*"/become_user: ${UserName}/" ${SCRIPT_DIR}/Exp/${playbook}
        sed -i "s|PATH_TO_ELECT|${PathToArtifact}|g" "${SCRIPT_DIR}/Exp/${playbook}"
        sed -i "s|PATH_TO_DB_BACKUP|${PathToELECTExpDBBackup}|g" "${SCRIPT_DIR}/Exp/${playbook}"
    done
}

function load {
    expName=$1
    targetScheme=$2
    KVNumber=$3
    keylength=$4
    fieldlength=$5
    simulatedClientNumber=$6
    storageSavingTarget=$7
    ecK=$8

    echo "Start loading data into ${targetScheme}"
    # Make local results directory
    if [ ! -d ${PathToELECTResultSummary}/${targetScheme} ]; then
        mkdir -p ${PathToELECTResultSummary}/${targetScheme}
    fi

    # Copy playbook
    if [ -f "playbook-load.yaml" ]; then
        rm -rf playbook-load.yaml
    fi
    cp ../playbook/playbook-load.yaml .
    # Modify load playbook
    if [ ${targetScheme} == "cassandra" ]; then
        sed -i "s/\(mode: \)".*"/mode: raw/" playbook-load.yaml
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsbraw/" playbook-load.yaml
        sed -i "s/\(teeLevels: \)".*"/teeLevels: 9/" playbook-load.yaml
        sed -i "s/\(initialDelay: \)".*"/initialDelay: 65536/" playbook-load.yaml
        sed -i "s/\(concurrentEC: \)".*"/concurrentEC: 0/" playbook-load.yaml
    elif [ ${targetScheme} == "elect" ]; then
        sed -i "s/\(mode: \)".*"/mode: elect/" playbook-load.yaml
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsb/" playbook-load.yaml
        sed -i "s/\(teeLevels: \)".*"/teeLevels: ${teeLevels}/" playbook-load.yaml
        sed -i "s/\(initialDelay: \)".*"/initialDelay: ${initialDelay}/" playbook-load.yaml
        sed -i "s/\(target_saving: \)".*"/target_saving: ${storageSavingTarget}/" playbook-load.yaml
        sed -i "s/\(concurrentEC: \)".*"/concurrentEC: ${concurrentEC}/" playbook-load.yaml
        sed -i "s/\(data_block_num: \)".*"/data_block_num: ${ecK}/" playbook-load.yaml
    fi

    sed -i "s/\(expName: \)".*"/expName: "${expName}-${targetScheme}-Load"/" playbook-load.yaml
    sed -i "s/record_count:.*$/record_count: ${KVNumber}/" playbook-load.yaml
    sed -i "s/key_length:.*$/key_length: ${keylength}/" playbook-load.yaml
    sed -i "s/filed_length:.*$/filed_length: ${fieldlength}/" playbook-load.yaml

    modifyWorkload "workloadLoad"

    sed -i "s/\(workload: \)".*"/workload: \"workloadLoad\"/" playbook-load.yaml
    sed -i "s/\(threads: \)".*"/threads: ${simulatedClientNumber}/" playbook-load.yaml

    ansible-playbook -v -i hosts.ini playbook-load.yaml

    ## Collect load logs
    for nodeIP in "${NodesList[@]}"; do
        echo "Copy loading stats of loading for ${expName}-${targetScheme} back, current working on node ${nodeIP}"
        scp -r ${UserName}@${nodeIP}:${PathToELECTLog} ${PathToELECTResultSummary}/${targetScheme}/${ExpName}-Load-${nodeIP}
        ssh ${UserName}@${nodeIP} "rm -rf '${PathToELECTLog}'; mkdir -p '${PathToELECTLog}'"
    done
}

function modifyWorkload {
    workload=$1
    keylength=$2
    fieldlength=$3
    sed -i "s/\(keylength= \)".*"/keylength=${keylength}/" ./YCSB/workloads/${workload}
    sed -i "s/\(fieldlength= \)".*"/fieldlength=${fieldlength}/" ./YCSB/workloads/${workload}
}

function flush {
    expName=$1
    targetScheme=$2
    waitTime=$3
    echo "Start for flush and wait for compaction of ${targetScheme}"
    # Make local results directory
    if [ ! -d ${PathToELECTResultSummary}/${targetScheme} ]; then
        mkdir -p ${PathToELECTResultSummary}/${targetScheme}
    fi

    # Copy playbook
    if [ -f "playbook-flush.yaml" ]; then
        rm -rf playbook-flush.yaml
    fi
    cp ../playbook/playbook-flush.yaml .
    # Modify playbook
    sed -i "s/\(expName: \)".*"/expName: "${ExpName}-${targetScheme}-Load"/" playbook-flush.yaml
    sed -i "s/\(workload: \)".*"/workload: \"workloadLoad\"/" playbook-flush.yaml
    sed -i "s/\(seconds: \)".*"/seconds: ${waitTime}/" playbook-flush.yaml
    ansible-playbook -v -i hosts.ini playbook-flush.yaml
}

function backup {
    expName=$1
    targetScheme=$2
    KVNumber=$3
    keylength=$4
    fieldlength=$5
    extraFlag=${9:-}

    echo "Start copy data of ${targetScheme} to backup, this will kill the online system!!!"
    # Make local results directory
    if [ ! -d ${PathToELECTResultSummary}/${targetScheme} ]; then
        mkdir -p ${PathToELECTResultSummary}/${targetScheme}
    fi

    # Copy playbook
    if [ -f "playbook-backup.yaml" ]; then
        rm -rf playbook-backup.yaml
    fi
    cp ../playbook/playbook-backup.yaml .
    # Modify playbook
    if [ ${targetScheme} == "cassandra" ]; then
        sed -i "s/\(mode: \)".*"/mode: raw/" playbook-backup.yaml
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsbraw/" playbook-backup.yaml
    elif [ ${targetScheme} == "elect" ]; then
        sed -i "s/\(mode: \)".*"/mode: elect/" playbook-backup.yaml
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsb/" playbook-backup.yaml
    fi
    sed -i "s/Scheme/${targetScheme}/g" playbook-backup.yaml
    if [ -z "${extraFlag}" ]; then
        sed -i "s/DATAPATH/${expName}-KV-${KVNumber}-Key-${keylength}-Value-${fieldlength}g" playbook-backup.yaml
    else
        sed -i "s/DATAPATH/${expName}-KV-${KVNumber}-Key-${keylength}-Value-${fieldlength}-Flags=${extraFlag}g" playbook-backup.yaml
    fi
    ansible-playbook -v -i hosts.ini playbook-backup.yaml
}

function startupFromBackup {
    expName=$1
    targetScheme=$2
    KVNumber=$3
    keylength=$4
    fieldlength=$5
    extraFlag=${9:-}
    echo "Start copy data of ${targetScheme} back from backup"
    # Make local results directory
    if [ ! -d ${PathToELECTResultSummary}/${targetScheme} ]; then
        mkdir -p ${PathToELECTResultSummary}/${targetScheme}
    fi

    # Copy playbook
    if [ -f "playbook-startup.yaml" ]; then
        rm -rf playbook-startup.yaml
    fi
    cp ../playbook/playbook-startup.yaml .
    # Modify playbook
    sed -i "s/Scheme/${targetScheme}/g" playbook-startup.yaml
    if [ -z "${extraFlag}" ]; then
        sed -i "s/DATAPATH/${expName}-KV-${KVNumber}-Key-${keylength}-Value-${fieldlength}g" playbook-startup.yaml
    else
        sed -i "s/DATAPATH/${expName}-KV-${KVNumber}-Key-${keylength}-Value-${fieldlength}-Flags=${extraFlag}g" playbook-startup.yaml
    fi
    ansible-playbook -v -i hosts.ini playbook-startup.yaml
}

function failnodes {
    echo "Fail node for degraded test"
    # Copy playbook
    if [ -f "playbook-fail.yaml" ]; then
        rm -rf playbook-fail.yaml
    fi
    cp ../playbook/playbook-fail.yaml .
    # Modify playbook
    ansible-playbook -v -i hosts.ini playbook-fail.yaml
}

function runExp {
    expName=$1
    targetScheme=$2
    round=$3
    runningType=$4
    KVNumber=$5
    operationNumber=$6
    workloads=$7
    simulatedClientNumber=$8
    consistency=$9

    echo "Start run benchmark to ${targetScheme}, settings: expName is ${expName}, target scheme is ${targetScheme}, running mode is ${runningType}, KVNumber is ${KVNumber}, operationNumber is ${operationNumber}, workloads are ${workloads}, simulatedClientNumber is ${simulatedClientNumber}, consistency is ${consistency}."

    # Make local results directory
    if [ ! -d ${PathToELECTResultSummary}/${targetScheme} ]; then
        mkdir -p ${PathToELECTResultSummary}/${targetScheme}
    fi

    # Normal/Degraded Ops
    for workload in "${workloads[@]}"; do
        echo "Start running $workload on ${targetScheme} round $round"
        modifyWorkload $workload
        if [ -f "playbook-run.yaml" ]; then
            rm -rf playbook-run.yaml
        fi
        cp ../playbook/playbook-run.yaml .
        # Modify run palybook
        if [ ${targetScheme} == "cassandra" ]; then
            sed -i "s/\(keyspace: \)".*"/keyspace: ycsbraw/" playbook-run.yaml
        else
            sed -i "s/\(keyspace: \)".*"/keyspace: ycsb/" playbook-run.yaml
        fi
        sed -i "s/\(threads: \)".*"/threads: ${simulatedClientNumber}/" playbook-run.yaml
        sed -i "s/\(workload: \)".*"/workload: \"${workload}\"/" playbook-run.yaml
        sed -i "s/\(expName: \)".*"/expName: "${ExpName}-${targetScheme}-Run-${runningType}-Round-${round}"/" playbook-run.yaml
        sed -i "s/record_count:.*$/record_count: ${KVNumber}/" playbook-run.yaml
        sed -i "s/operation_count:.*$/operation_count: ${operationNumber}/" playbook-run.yaml
        sed -i "s/\(consistency: \)".*"/consistency: ${consistency}/" playbook-run.yaml
        if [ "${workload}" == "workloade" ] || [ "${workload}" == "workloadscan" ]; then
            # generate scanNumber = operationNumber / 10
            scanNumber=$((operationNumber / 10))
            sed -i "s/operation_count:.*$/operation_count: ${scanNumber}/" playbook-run.yaml
        fi
        ansible-playbook -v -i hosts.ini playbook-run.yaml
        ## Collect
        ## Collect running logs
        for nodeIP in "${NodesList[@]}"; do
            echo "Copy loading stats of loading for ${expName}-${targetScheme} back, current working on node ${nodeIP}"
            scp -r ${UserName}@${nodeIP}:${PathToELECTLog} ${PathToELECTResultSummary}/${targetScheme}/${ExpName}-Load-${nodeIP}
            ssh ${UserName}@${nodeIP} "rm -rf '${PathToELECTLog}'; mkdir -p '${PathToELECTLog}'"
        done
    done
}

function treeSizeEstimation {
    KVNumber=$1
    keylength=$2
    fieldlength=$3
    initial_count=${SSTableSize}
    ratio=${LSMTreeFanOutRatio}
    target_count=$((KVNumber * (keylength + fieldlength) / NodeNumber / 1024 / 1024 / 4))

    current_count=$initial_count
    current_level=1

    while [ $current_count -lt $target_count ]; do
        current_count=$((current_count * ratio))
        current_level=$((current_level + 1))
    done
    teeLevels=$((current_level))
    echo ${teeLevels}
}

function dataSizeEstimation {
    KVNumber=$1
    keylength=$2
    fieldlength=$3
    dataSizeOnEachNode=$(echo "scale=2; $KVNumber * ($keylength + $fieldlength) / $NodeNumber / 1024 / 1024 / 1024 * 3" | bc)
    echo ${dataSizeOnEachNode}
}

function initialDelayEstimation {
    dataSizeOnEachNode=$1
    scheme=$2
    if [ "${scheme}" == "cassandra" ]; then
        initialDelay=65536
        echo ${initialDelay}
    else
        initialDellayLocal=$(echo "scale=2; $dataSizeOnEachNode * 8" | bc)
        initialDellayLocalCeil=$(echo "scale=0; (${initialDellayLocal} + 0.5)/1" | bc)
        echo ${initialDellayLocalCeil}
    fi
}

function waitFlushCompactionTimeEstimation {
    dataSizeOnEachNode=$1
    scheme=$2
    if [ "${scheme}" == "cassandra" ]; then
        waitTime=$(echo "scale=2; $dataSizeOnEachNode * 500" | bc)
        waitTimeCeil=$(echo "scale=0; (${waitTime} + 0.5)/1" | bc)
        echo ${waitTimeCeil}
    else
        waitTime=$(echo "scale=2; ($dataSizeOnEachNode * 1024  / 4 / 3 / ($concurrentEC / 2)) * 80 + $dataSizeOnEachNode * 500" | bc)
        waitTimeCeil=$(echo "scale=0; (${waitTime} + 0.5)/1" | bc)
        echo ${waitTimeCeil}
    fi
}

function loadDataForEvaluation {
    expName=$1
    scheme=$2
    KVNumber=$3
    keylength=$4
    fieldlength=$5
    simulatedClientNumber=${6:-"${defaultSimulatedClientNumber}"}
    storageSavingTarget=${7:-"0.6"}
    codingK=${8:-"4"}
    extraFlag=${9:-}
    echo "The evaluation setting is ${scheme} (Loading), expName is ${expName}; KVNumber is ${KVNumber}, keylength is ${keylength}, fieldlength is ${fieldlength}, simulatedClientNumber is ${simulatedClientNumber}, storageSavingTarget is ${storageSavingTarget}, codingK is ${codingK}, extraFlag is ${extraFlag}."
    exit
    # Gen params
    dataSizeOnEachNode=$(dataSizeEstimation KVNumber keylength fieldlength)
    initialDelayTime=$(initialDelayEstimation ${dataSizeOnEachNode} ${scheme})
    waitFlushCompactionTime=$(waitFlushCompactionTimeEstimation ${dataSizeOnEachNode} ${scheme})
    teeLevels=$(treeSizeEstimation ${KVNumber} ${keylength} ${fieldlength})

    # Outpout params
    echo "Start experiment to ${scheme} (Loading), expName is ${expName}; KVNumber is ${KVNumber}, keylength is ${keylength}, fieldlength is ${fieldlength}, simulatedClientNumber is ${simulatedClientNumber}. Estimation of data size on each node is ${dataSizeOnEachNode} GiB, initial delay is ${initialDelayTime}, flush and compaction wait time is ${waitFlushCompactionTime}."
    ehco ""
    echo "The total running time is estimated to be (( (${waitFlushCompactionTime} + ${KVNumber}/20000)/60 )) minutes."

    # Load
    load "${expName}" "${scheme}" "${KVNumber}" "${keylength}" "${fieldlength}" "${simulatedClientNumber}" "${codingK}"
    flush "${expName}" "${scheme}" "${waitFlushCompactionTime}"
    backup "${expName}" "${scheme}" "${KVNumber}" "${keylength}" "${fieldlength}" "${extraFlag}"
}

function doEvaluation {
    expName=$1
    scheme=$2
    KVNumber=$3
    operationNumber=$4
    simulatedClientNumber=$5
    runningModes=$6
    workloads=$7
    RunningRoundNumber=$8
    extraFlag=${9:-}

    # Outpout params
    echo "Start experiment to ${scheme} (Running), expName is ${expName}; KVNumber is ${KVNumber}, keylength is ${keylength}, fieldlength is ${fieldlength}, operationNumber is ${operationNumber}, simulatedClientNumber is ${simulatedClientNumber}. The experiment will run ${RunningRoundNumber} rounds."

    readConsistency="ONE" # default read consistency level is ONE, can be changed by extraFlag to "TWO" and "ALL"
    if [[ "${extraFlag}" == *"consistency"* ]]; then
        readConsistency=$(echo "${extraFlag}" | awk -F'=' '{print $2}')
        echo "Read consistency level is chanegd to ${readConsistency}"
    fi

    for ((round = 1; round <= RunningRoundNumber; round++)); do
        for type in "${runningModes[@]}"; do
            if [ "${type}" == "normal" ]; then
                startupFromBackup "${expName}" "${scheme}" "${KVNumber}" "${keylength}" "${fieldlength}" "${extraFlag}"
                runExp "${expName}" "${scheme}" "${round}" "${type}" "${KVNumber}" "${operationNumber}" "${workloads}" "${simulatedClientNumber}" "${readConsistency}"
            elif [ "${type}" == "degraded" ]; then
                startupFromBackup "${expName}" "${scheme}" "${KVNumber}" "${keylength}" "${fieldlength}" "${extraFlag}"
                failnodes
                runExp "${expName}" "${scheme}" "${round}" "${type}" "${KVNumber}" "${operationNumber}" "${workloads}" "${simulatedClientNumber}" "${readConsistency}"
            fi
        done
    done
}

function recovery {
    expName=$1
    targetScheme=$2
    recoveryNode=$3
    KVNumber=$4
    runningRound=${5,-"1"}

    # Make local results directory
    if [ ! -d ${PathToELECTResultSummary}/${targetScheme} ]; then
        mkdir -p ${PathToELECTResultSummary}/${targetScheme}
    fi

    # Copy playbook
    if [ -f "playbook-recovery.yaml" ]; then
        rm -rf playbook-recovery.yaml
    fi
    cp ../playbook/playbook-recovery.yaml .

    if [ ${targetScheme} == "cassandra" ]; then
        sed -i "s/\(mode: \)".*"/mode: raw/" playbook-recovery.yaml
    else
        sed -i "s/\(mode: \)".*"/mode: elect/" playbook-recovery.yaml
    fi
    sed -i "s/\(seconds: \)".*"/seconds: 900/" playbook-recovery.yaml

    ansible-playbook -v -i hosts.ini playbook-recovery.yaml

    echo "Copy running data of ${targetScheme} back, ${recoveryNode}"
    scp elect@${recoveryNode}:${PathToELECTPrototype}/logs/recovery.log ${PathToELECTResultSummary}/"${targetScheme}"/"${expName}-Size-${KVNumber}-recovery-${runningRound}-${recoveryNode}"
}
