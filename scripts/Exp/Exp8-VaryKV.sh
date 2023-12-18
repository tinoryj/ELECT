#!/bin/bash

# Exp4: SYN workload, 3-way replication, (6,4) enkeySize, 60% target storage saving, 10M KV + 1M OP.

if [ ! -f "hosts.ini" ]; then
    cp ../playbook/hosts.ini .
fi

ExpName="Exp8-VaryKV"
schemes=("cassandra" "elect")
workloads=("workloadReadKV" "workloadWriteKV")
types=("normal" "degraded")
waitFlush="true"
restart="true"
threadsNumber=16
operationNumber=1000000
KVNumber=10000000
NodeNumber=10
roundNumber=5
# For ELECT
maxLevel=9
initialDelay=65536
concurrentEC=64

FixedKeyLen="32"
FixedValueLen="512"
keylengths=("128" "64" "32" "16" "8")
valueLengths=("8192" "2048" "128" "32")

# Run Exp

function maxLevelCount {
    keySize=$1
    valueSize=$2
    # calculate target max level
    initial_count=4
    ratio=10
    target_count=$((KVNumber * (keySize + valueSize) / NodeNumber / 1024 / 1024 / 4))

    current_count=$initial_count
    current_layer=1

    while [ $current_count -lt $target_count ]; do
        current_count=$((current_count * ratio))
        current_layer=$((current_layer + 1))
    done
    maxLevel=$((current_layer))
}

function load {
    targetScheme=$1
    keySize=$2
    valueSize=$3
    echo "Start loading data into ${targetScheme}"
    # Make local results directory
    if [ ! -d ${PathToELECTLog}/${targetScheme} ]; then
        mkdir -p ${PathToELECTLog}/${targetScheme}
    fi

    # Copy playbook
    if [ -f "playbook-load.yaml" ]; then
        rm -rf playbook-load.yaml
    fi
    cp ../playbook/playbook-load.yaml .
    # Modify load playbook
    if [ ${targetScheme} == "cassandreas" ]; then
        sed -i "s/\(mode: \)".*"/mode: eas/" playbook-load.yaml
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsb/" playbook-load.yaml
    elif [ ${targetScheme} == "cassandra" ]; then
        sed -i "s/\(mode: \)".*"/mode: raw/" playbook-load.yaml
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsbraw/" playbook-load.yaml
    elif [ ${targetScheme} == "elect" ]; then
        sed -i "s/\(mode: \)".*"/mode: elect/" playbook-load.yaml
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsb/" playbook-load.yaml
    elif [ ${targetScheme} == "mlsm" ]; then
        sed -i "s/\(mode: \)".*"/mode: mlsm/" playbook-load.yaml
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsb/" playbook-load.yaml
    fi

    if [ ${targetScheme} == "elect" ]; then
        sed -i "s/\(maxLevel: \)".*"/maxLevel: ${maxLevel}/" playbook-load.yaml
        sed -i "s/\(initialDelay: \)".*"/initialDelay: ${initialDelay}/" playbook-load.yaml
        sed -i "s/\(concurrentEC: \)".*"/concurrentEC: ${concurrentEC}/" playbook-load.yaml
    else
        sed -i "s/\(maxLevel: \)".*"/maxLevel: 9/" playbook-load.yaml
        sed -i "s/\(initialDelay: \)".*"/initialDelay: 65536/" playbook-load.yaml
        sed -i "s/\(concurrentEC: \)".*"/concurrentEC: 0/" playbook-load.yaml
    fi
    sed -i "s/\(expName: \)".*"/expName: "${ExpName}-${targetScheme}-Key-${keySize}-Value-${valueSize}-Load"/" playbook-load.yaml
    sed -i "s/record_count:.*$/record_count: ${KVNumber}/" playbook-load.yaml
    sed -i "s/filed_length:.*$/filed_length: ${valueSize}/" playbook-load.yaml

    modifyWorkload "workloadReadKV" $keySize $valueSize

    sed -i "s/\(workload: \)".*"/workload: \"workloadReadKV\"/" playbook-load.yaml
    sed -i "s/\(threads: \)".*"/threads: ${threadsNumber}/" playbook-load.yaml

    ansible-playbook -v -i hosts.ini playbook-load.yaml

    ## Collect load results
    for ((i = 1; i <= NodeNumber; i++)); do
        echo "Copy loading stats of ${targetScheme} back, node$i"
        scp -r elect@node$i:/home/elect/Results ${PathToELECTLog}/${targetScheme}/${ExpName}-Key-${keySize}-value-${valueSize}-Load-Node$i
        ssh elect@node$i "rm -rf /home/elect/Results && mkdir -p /home/elect/Results"
    done
}

function modifyWorkload {
    workload=$1
    keySize=$2
    valueSize=$3
    paddingSize=$((keySize - 4))
    cd /home/elect/ELECTExp/YCSB/workloads || exit
    sed -i "s/\(keylength= \)".*"/keylength=${keySize}/" ${workload}
    sed -i "s/\(zeropadding= \)".*"/zeropadding=${paddingSize}/" ${workload}
    sed -i "s/\(keylengthSTDEV= \)".*"/keylengthSTDEV=0/" ${workload}
    sed -i "s/\(keylengthMin= \)".*"/keylengthMin=${keySize}/" ${workload}
    sed -i "s/\(keylengthMax= \)".*"/keylengthMax=${keySize}/" ${workload}
    sed -i "s/\(fieldlength= \)".*"/fieldlength=${valueSize}/" ${workload}
    sed -i "s/\(fieldlengthSTDEV= \)".*"/fieldlengthSTDEV=0/" ${workload}
    sed -i "s/\(fieldlengthMin= \)".*"/fieldlengthMin=${valueSize}/" ${workload}
    sed -i "s/\(fieldlengthMax= \)".*"/fieldlengthMax=${valueSize}/" ${workload}
    cd PATH_TO_SCRIPTS/Exp || exit
}

function flush {
    targetScheme=$1
    waitTime=$2
    keySize=$3
    valueSize=$4
    echo "Start for flush and wait for compaction of ${targetScheme}"
    # Make local results directory
    if [ ! -d ${PathToELECTLog}/${targetScheme} ]; then
        mkdir -p ${PathToELECTLog}/${targetScheme}
    fi

    # Copy playbook
    if [ -f "playbook-flush.yaml" ]; then
        rm -rf playbook-flush.yaml
    fi
    cp ../playbook/playbook-flush.yaml .
    # Modify playbook
    sed -i "s/\(expName: \)".*"/expName: "${ExpName}-key-${keySize}-value-${value}-${targetScheme}-Load"/" playbook-flush.yaml
    sed -i "s/\(workload: \)".*"/workload: \"workloadReadKV\"/" playbook-flush.yaml
    sed -i "s/\(seconds: \)".*"/seconds: ${waitTime}/" playbook-flush.yaml
    ansible-playbook -v -i hosts.ini playbook-flush.yaml
}

function backup {
    targetScheme=$1
    keySize=$2
    valueSize=$3
    echo "Start copy data of ${targetScheme} to backup, this will kill the online system!!!"
    # Make local results directory
    if [ ! -d ${PathToELECTLog}/${targetScheme} ]; then
        mkdir -p ${PathToELECTLog}/${targetScheme}
    fi

    # Copy playbook
    if [ -f "playbook-backup.yaml" ]; then
        rm -rf playbook-backup.yaml
    fi
    cp ../playbook/playbook-backup.yaml .
    # Modify playbook
    if [ ${targetScheme} == "cassandreas" ]; then
        sed -i "s/\(mode: \)".*"/mode: eas/" playbook-backup.yaml
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsb/" playbook-backup.yaml
    elif [ ${targetScheme} == "cassandra" ]; then
        sed -i "s/\(mode: \)".*"/mode: raw/" playbook-backup.yaml
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsbraw/" playbook-backup.yaml
    elif [ ${targetScheme} == "elect" ]; then
        sed -i "s/\(mode: \)".*"/mode: elect/" playbook-backup.yaml
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsb/" playbook-backup.yaml
    elif [ ${targetScheme} == "mlsm" ]; then
        sed -i "s/\(mode: \)".*"/mode: mlsm/" playbook-backup.yaml
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsb/" playbook-backup.yaml
    fi
    sed -i "s/Scheme/${targetScheme}/g" playbook-backup.yaml
    sed -i "s/DATAPATH/${ExpName}-${keySize}-${valueSize}/g" playbook-backup.yaml
    ansible-playbook -v -i hosts.ini playbook-backup.yaml
}

function startup {
    targetScheme=$1
    keySize=$2
    valueSize=$3
    echo "Start copy data back ${targetScheme} from backup"
    # Make local results directory
    if [ ! -d ${PathToELECTLog}/${targetScheme} ]; then
        mkdir -p ${PathToELECTLog}/${targetScheme}
    fi

    # Copy playbook
    if [ -f "playbook-startup.yaml" ]; then
        rm -rf playbook-startup.yaml
    fi
    cp ../playbook/playbook-startup.yaml .
    # Modify playbook
    if [ ${targetScheme} == "elect" ]; then
        sed -i "s/\(maxLevel: \)".*"/maxLevel: ${maxLevel}/" playbook-startup.yaml
        sed -i "s/\(initialDelay: \)".*"/initialDelay: ${initialDelay}/" playbook-startup.yaml
        sed -i "s/\(concurrentEC: \)".*"/concurrentEC: ${concurrentEC}/" playbook-startup.yaml
        sed -i "s/\(mode: \)".*"/mode: elect/" playbook-startup.yaml
    else
        sed -i "s/\(maxLevel: \)".*"/maxLevel: 9/" playbook-startup.yaml
        sed -i "s/\(initialDelay: \)".*"/initialDelay: 65536/" playbook-startup.yaml
        sed -i "s/\(concurrentEC: \)".*"/concurrentEC: 0/" playbook-startup.yaml
        sed -i "s/\(mode: \)".*"/mode: raw/" playbook-startup.yaml
    fi
    sed -i "s/Scheme/${targetScheme}/g" playbook-startup.yaml
    sed -i "s/DATAPATH/${ExpName}-${keySize}-${valueSize}/g" playbook-startup.yaml
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
    keySize=$4
    valueSize=$5

    echo "Start run benchmark to ${targetScheme}"
    # Make local results directory
    if [ ! -d ${PathToELECTLog}/${targetScheme} ]; then
        mkdir -p ${PathToELECTLog}/${targetScheme}
    fi

    # Normal/Degraded Ops
    for workload in "${workloads[@]}"; do
        echo "Start running $workload on ${targetScheme} round $round"
        modifyWorkload $workload $keySize $valueSize
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
        sed -i "s/\(threads: \)".*"/threads: ${threadsNumber}/" playbook-run.yaml
        sed -i "s/\(workload: \)".*"/workload: \"${workload}\"/" playbook-run.yaml
        sed -i "s/\(expName: \)".*"/expName: "${ExpName}-${targetScheme}-Run-${runningType}-Key-${keySize}-Value-${valueSize}-Round-${round}"/" playbook-run.yaml
        sed -i "s/record_count:.*$/record_count: ${KVNumber}/" playbook-run.yaml
        sed -i "s/operation_count:.*$/operation_count: ${operationNumber}/" playbook-run.yaml
        if [ "${workload}" == "workloadScan" ]; then
            # generate scanNumber = operationNumber / 10
            scanNumber=$((operationNumber / 10))
            sed -i "s/operation_count:.*$/operation_count: ${scanNumber}/" playbook-run.yaml
        fi
        ansible-playbook -v -i hosts.ini playbook-run.yaml
        ## Collect
        for ((i = 1; i <= NodeNumber; i++)); do
            echo "Copy running data of ${targetScheme} back, node$i"
            scp -r elect@node$i:/home/elect/Results ${PathToELECTLog}/"${targetScheme}"/"${ExpName}-Key-${keySize}-Value-${valueSize}-${workload}-${round}-Node$i"
            ssh elect@node$i "rm -rf /home/elect/Results && mkdir -p /home/elect/Results"
        done
    done
}

for scheme in "${schemes[@]}"; do
    echo "Start experiment to ${scheme}"

    # Gen params
    for keySize in "${keylengths[@]}"; do
        # Gen KV Number
        KVNumber=$(echo "scale=2; 10*1024*1024*1024 / ($keySize + $FixedValueLen)" | bc)
        KVNumberCeil=$(echo "scale=0; (${KVNumber} + 0.5)/1" | bc)
        KVNumber=$KVNumberCeil
        # Gen wait time
        dataSizeOnEachNode=$(echo "scale=2; $KVNumber * ($keySize + $FixedValueLen) / $NodeNumber / 1024 / 1024 / 1024 * 3" | bc)
        initialDellayLocal=$(echo "scale=2; $dataSizeOnEachNode * 7 + ($KVNumber / 2100000)" | bc)
        initialDellayLocalCeil=$(echo "scale=0; (${initialDellayLocal} + 0.5)/1" | bc)
        waitTime=$(echo "scale=2; $dataSizeOnEachNode * 450" | bc)
        if [ "${scheme}" == "elect" ]; then
            waitTime=$(echo "scale=2; ($dataSizeOnEachNode * 1024 / 4 / 2 / ($concurrentEC / 2)) * 80 + $dataSizeOnEachNode * 450" | bc)
            maxLevelCount ${keySize} ${FixedValueLen}
            initialDelay=$initialDellayLocalCeil
        else
            maxLevel=9
            initialDelay=65536
        fi
        waitTimeCeil=$(echo "scale=0; (${waitTime} + 0.5)/1" | bc)
        echo "Data size on each node is ${dataSizeOnEachNode} GiB, initial delay is ${initialDelay}, target flush wait time is ${waitTimeCeil}, max level is ${maxLevel}, KV Number is ${KVNumber}, key Size is ${keySize}, value size is ${FixedValueLen}"

        load "${scheme}" "${keySize}" "${FixedValueLen}"

        if [ "${waitFlush}" == "true" ]; then
            flush "${scheme}" "${waitTimeCeil}" "${keySize}" "${FixedValueLen}"
        fi

        if [ "${restart}" == "true" ]; then
            backup "${scheme}" "${keySize}" "${FixedValueLen}"
        fi

        for ((round = 1; round <= roundNumber; round++)); do
            for type in "${types[@]}"; do
                if [ "${type}" == "normal" ]; then
                    if [ "${restart}" == "true" ]; then
                        startup "${scheme}" "${keySize}" "${FixedValueLen}"
                    fi
                    run "${scheme}" "${round}" "${type}" "${keySize}" "${FixedValueLen}"
                elif [ "${type}" == "degraded" ]; then
                    if [ "${restart}" == "true" ]; then
                        startup "${scheme}" "${keySize}" "${FixedValueLen}"
                    fi
                    failnodes
                    run "${scheme}" "${round}" "${type}" "${keySize}" "${FixedValueLen}"
                fi
            done
        done
    done

    # Gen params
    for valueSize in "${valueLengths[@]}"; do
        # Gen KV Number
        KVNumber=$(echo "scale=2; 10*1024*1024*1024 / ($FixedKeyLen + $valueSize)" | bc)
        KVNumberCeil=$(echo "scale=0; (${KVNumber} + 0.5)/1" | bc)
        KVNumber=$KVNumberCeil
        # Gen wait time
        dataSizeOnEachNode=$(echo "scale=2; $KVNumber * ($FixedKeyLen + $valueSize) / $NodeNumber / 1024 / 1024 / 1024 * 3" | bc)
        initialDellayLocal=$(echo "scale=2; $dataSizeOnEachNode * 7 + ($KVNumber / 21000000)" | bc)
        initialDellayLocalCeil=$(echo "scale=0; (${initialDellayLocal} + 0.5)/1" | bc)
        waitTime=$(echo "scale=2; $dataSizeOnEachNode * 450" | bc)
        if [ "${scheme}" == "elect" ]; then
            waitTime=$(echo "scale=2; ($dataSizeOnEachNode * 1024 / 4 / 2 / ($concurrentEC / 2)) * 80 + $dataSizeOnEachNode * 450" | bc)
            maxLevelCount ${FixedKeyLen} ${valueSize}
            initialDelay=$initialDellayLocalCeil
        else
            maxLevel=9
            initialDelay=65536
        fi
        waitTimeCeil=$(echo "scale=0; (${waitTime} + 0.5)/1" | bc)
        echo "Data size on each node is ${dataSizeOnEachNode} GiB, initial delay is ${initialDelay}, target flush wait time is ${waitTimeCeil}, max level is ${maxLevel}, KV Number is ${KVNumber}, key Size is ${FixedKeyLen}, value size is ${valueSize}"

        load "${scheme}" "${FixedKeyLen}" "${valueSize}"

        if [ "${waitFlush}" == "true" ]; then
            flush "${scheme}" "${waitTimeCeil}" "${FixedKeyLen}" "${valueSize}"
        fi

        if [ "${restart}" == "true" ]; then
            backup "${scheme}" "${FixedKeyLen}" "${valueSize}"
        fi

        for ((round = 1; round <= roundNumber; round++)); do
            for type in "${types[@]}"; do
                if [ "${type}" == "normal" ]; then
                    if [ "${restart}" == "true" ]; then
                        startup "${scheme}" "${FixedKeyLen}" "${valueSize}"
                    fi
                    run "${scheme}" "${round}" "${type}" "${FixedKeyLen}" "${valueSize}"
                elif [ "${type}" == "degraded" ]; then
                    if [ "${restart}" == "true" ]; then
                        startup "${scheme}" "${FixedKeyLen}" "${valueSize}"
                    fi
                    failnodes
                    run "${scheme}" "${round}" "${type}" "${FixedKeyLen}" "${valueSize}"
                fi
            done
        done
    done
done
