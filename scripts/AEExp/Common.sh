#!/bin/bash

# Common params for all experiments

NodesList=(172.27.96.1 172.27.96.2 172.27.96.3 172.27.96.4 172.27.96.5 172.27.96.6 172.27.96.7 172.27.96.8 172.27.96.9 172.27.96.10)
OSSServerNode="172.27.96.1"
ClientNode="172.27.96.1"
RunningRoundNumber=1

PathToELECT="/home/elect/ELECT/prototype"
PathToYCSB="/home/elect/ELECT/YCSB"
PathToELECTExpDBBackup="/home/elect/ELECTExpDBBackup"
PathToELECTLog="/home/elect/ELECTLog"
PathToELECTResultSummary="/home/elect/ELECTLog"
UserName="elect"
NodeNumber="${#NodesList[@]}"
SSTableSize=4
LSMTreeFanOutRatio=10

teeLevels=9
initialDelay=65536
concurrentEC=64

function setupNodeInfo {
    targetHostInfo=$1
    if [ -f ${targetHostInfo} ]; then
        rm -rf ${targetHostInfo}
    fi
    echo "[elect_servers]" >>${targetHostInfo}
    for ((i = 1; i <= NodeNumber; i++)); do
        echo "server${i} ansible_host=${NodesList[(($i - 1))]}" >>${targetHostInfo}
    done
    echo >>${targetHostInfo}
    echo "[elect_oss]" >>${targetHostInfo}
    echo "oss ansible_host=${OSSServerNode}" >>${targetHostInfo}
    echo >>${targetHostInfo}
    echo "[elect_client]" >>${targetHostInfo}
    echo "client ansible_host=${ClientNode}" >>${targetHostInfo}
    # random select the failure node from the total node list
    failure_nodes=($(shuf -i 1-${NodeNumber} -n 1))
    echo >>${targetHostInfo}
    echo "[elect_failure]" >>${targetHostInfo}
    echo "server${failure_nodes} ansible_host=${NodesList[(($failure_nodes - 1))]}" >>${targetHostInfo}
}

setupNodeInfo "hosts.ini"
exit

function load {
    targetScheme=$1
    expName=$2
    KVNumber=$3
    keylength=$4
    fieldlength=$5
    simulatedClientNumber=$6

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
        sed -i "s/\(concurrentEC: \)".*"/concurrentEC: ${concurrentEC}/" playbook-load.yaml
    fi

    sed -i "s/\(expName: \)".*"/expName: "${expName}-${targetScheme}-Load"/" playbook-load.yaml
    sed -i "s/record_count:.*$/record_count: ${KVNumber}/" playbook-load.yaml
    sed -i "s/key_length:.*$/key_length: ${keylength}/" playbook-load.yaml
    sed -i "s/filed_length:.*$/filed_length: ${fieldlength}/" playbook-load.yaml

    modifyWorkload "workload_load"

    sed -i "s/\(workload: \)".*"/workload: \"workload_load\"/" playbook-load.yaml
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
    sed -i "s/\(workload: \)".*"/workload: \"workload_template\"/" playbook-flush.yaml
    sed -i "s/\(seconds: \)".*"/seconds: ${waitTime}/" playbook-flush.yaml
    ansible-playbook -v -i hosts.ini playbook-flush.yaml
}

function backup {
    expName=$1
    targetScheme=$2
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
    sed -i "s/DATAPATH/${expName}g" playbook-backup.yaml
    ansible-playbook -v -i hosts.ini playbook-backup.yaml
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

function startupFromBackup {
    targetScheme=$1
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
    sed -i "s/DATAPATH/${ExpName}/g" playbook-startup.yaml
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

function run {
    targetScheme=$1
    round=$2
    runningType=$3

    echo "Start run benchmark to ${targetScheme}"
    # Make local results directory
    if [ ! -d ${PathToELECTLog}/${targetScheme} ]; then
        mkdir -p ${PathToELECTLog}/${targetScheme}
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
        if [ "${workload}" == "workloade" ]; then
            # generate scanNumber = operationNumber / 10
            scanNumber=$((operationNumber / 10))
            sed -i "s/operation_count:.*$/operation_count: ${scanNumber}/" playbook-run.yaml
        fi
        ansible-playbook -v -i hosts.ini playbook-run.yaml
        ## Collect
        for ((i = 1; i <= NodeNumber; i++)); do
            echo "Copy running data of ${targetScheme} back, node$i"
            scp -r elect@node$i:/home/elect/Results ${PathToELECTLog}/"${targetScheme}"/"${ExpName}-${workload}-${round}-Node$i"
            ssh elect@node$i "rm -rf /home/elect/Results && mkdir -p /home/elect/Results"
        done
    done
}

for scheme in "${schemes[@]}"; do
    echo "Start experiment to ${scheme}"

    # Gen params
    dataSizeOnEachNode=$(echo "scale=2; $KVNumber * ($keylength + $fieldlength) / $NodeNumber / 1024 / 1024 / 1024 * 3" | bc)
    initialDellayLocal=$(echo "scale=2; $dataSizeOnEachNode * 8" | bc)
    initialDellayLocalCeil=$(echo "scale=0; (${initialDellayLocal} + 0.5)/1" | bc)
    waitTime=$(echo "scale=2; $dataSizeOnEachNode * 500" | bc)
    teeLevels=0
    if [ "${scheme}" == "elect" ]; then
        waitTime=$(echo "scale=2; ($dataSizeOnEachNode * 1024  / 4 / 3 / ($concurrentEC / 2)) * 80 + $dataSizeOnEachNode * 500" | bc)
        teeLevels=$(treeSizeEstimation 10000000 16 1024)
        initialDelay=$initialDellayLocalCeil
    else
        teeLevels=9
        initialDelay=65536
    fi
    waitTimeCeil=$(echo "scale=0; (${waitTime} + 0.5)/1" | bc)
    echo "Data size on each node is ${dataSizeOnEachNode} GiB, initial delay is ${initialDelay}, target flush wait time is ${waitTimeCeil}, max level is ${teeLevels}"

    # Load

    # if [ "${scheme}" == "cassandra" ]; then
    #     echo "Skip load and backup for raw cassandra"
    # else
    #     load "${scheme}"
    #     if [ "${waitFlush}" == "true" ]; then
    #         flush "${scheme}" "${waitTimeCeil}"
    #     fi

    #     if [ "${restart}" == "true" ]; then
    #         backup "${scheme}"
    #     fi
    # fi

    for ((round = 1; round <= RunningRoundNumber; round++)); do
        for type in "${runningModes[@]}"; do
            if [ "${type}" == "normal" ]; then
                if [ "${restart}" == "true" ]; then
                    startup "${scheme}"
                fi
                run "${scheme}" "${round}" "${type}"
            elif [ "${type}" == "degraded" ]; then
                if [ "${restart}" == "true" ]; then
                    startup "${scheme}"
                fi
                failnodes
                run "${scheme}" "${round}" "${type}"
            fi
        done
    done
done
