#!/bin/bash

# Exp4: SYN workload, 3-way replication, (6,4) encoding, 60% target storage saving, 10M KV + 1M OP.

if [ ! -f "hosts.ini" ]; then
    cp ../playbook/hosts.ini .
fi

ExpName="Exp7-Recovery"
schemes=("cassandra")
workloads=("workloadRead" "workloadWrite")
waitFlush="true"
restart="true"
threadsNumber=64
operationNumber=1000000
KVNumber=10000000
keylength=24
keylengthMin=24
keylengthMax=24
fieldlength=1000
fieldlengthMin=1000
fieldlengthMax=1000
keylengthSTDEV=0
fieldlengthSTDEV=0
NodeNumber=10
roundNumber=5
# For ELECT
maxLevel=9
initialDelay=65536
concurrentEC=64

KVNumberSet=(20000000 30000000)

# Run Exp

function maxLevelCount {
    # calculate target max level
    initial_count=4
    ratio=10
    target_count=$((KVNumber * (keylength + fieldlength) / NodeNumber / 1024 / 1024 / 4))

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
    echo "Start loading data into ${targetScheme}"
    # Make local results directory
    if [ ! -d /home/elect/Results/${targetScheme} ]; then
        mkdir -p /home/elect/Results/${targetScheme}
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
    elif [ ${targetScheme} == "elect-trans" ]; then
        sed -i "s/\(mode: \)".*"/mode: elect/" playbook-load.yaml
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsb/" playbook-load.yaml
        sed -i "s/\(grade: \)".*"/grade: 1/" playbook-load.yaml
    elif [ ${targetScheme} == "elect-pdm" ]; then
        sed -i "s/\(mode: \)".*"/mode: elect/" playbook-load.yaml
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsb/" playbook-load.yaml
        sed -i "s/\(grade: \)".*"/grade: 2/" playbook-load.yaml
    elif [ ${targetScheme} == "elect-rdm" ]; then
        sed -i "s/\(mode: \)".*"/mode: elect/" playbook-load.yaml
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsb/" playbook-load.yaml
        sed -i "s/\(grade: \)".*"/grade: 3/" playbook-load.yaml
    elif [ ${targetScheme} == "mlsm" ]; then
        sed -i "s/\(mode: \)".*"/mode: mlsm/" playbook-load.yaml
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsb/" playbook-load.yaml
    fi

    if [[ ${targetScheme} == *"elect"* ]]; then
        sed -i "s/\(maxLevel: \)".*"/maxLevel: ${maxLevel}/" playbook-load.yaml
        sed -i "s/\(initialDelay: \)".*"/initialDelay: ${initialDelay}/" playbook-load.yaml
        sed -i "s/\(concurrentEC: \)".*"/concurrentEC: ${concurrentEC}/" playbook-load.yaml
    else
        sed -i "s/\(maxLevel: \)".*"/maxLevel: 9/" playbook-load.yaml
        sed -i "s/\(initialDelay: \)".*"/initialDelay: 65536/" playbook-load.yaml
        sed -i "s/\(concurrentEC: \)".*"/concurrentEC: ${concurrentEC}/" playbook-load.yaml
    fi
    sed -i "s/\(expName: \)".*"/expName: "${ExpName}-${targetScheme}-Load"/" playbook-load.yaml
    sed -i "s/record_count:.*$/record_count: ${KVNumber}/" playbook-load.yaml
    sed -i "s/filed_length:.*$/filed_length: ${fieldlength}/" playbook-load.yaml

    modifyWorkload "workload_template"

    sed -i "s/\(workload: \)".*"/workload: \"workload_template\"/" playbook-load.yaml
    sed -i "s/\(threads: \)".*"/threads: ${threadsNumber}/" playbook-load.yaml
    sed -i "s/\(runmode: \)".*"/runmode: trans/" playbook-load.yaml

    ansible-playbook -v -i hosts.ini playbook-load.yaml

    ## Collect load results
    # for ((i = 1; i <= NodeNumber; i++)); do
    #     echo "Copy loading stats of ${targetScheme} back, node$i"
    #     scp -r elect@node$i:/home/elect/Results /home/elect/Results/${targetScheme}/${ExpName}-Load-Node$i
    #     ssh elect@node$i "rm -rf /home/elect/Results && mkdir -p /home/elect/Results"
    # done
}

function modifyWorkload {
    workload=$1
    cd /home/elect/ELECTExp/YCSB/workloads || exit
    sed -i "s/\(keylength= \)".*"/keylength=${keylength}/" ${workload}
    sed -i "s/\(keylengthSTDEV= \)".*"/keylengthSTDEV=${keylengthSTDEV}/" ${workload}
    sed -i "s/\(keylengthMin= \)".*"/keylengthMin=${keylengthMin}/" ${workload}
    sed -i "s/\(keylengthMax= \)".*"/keylengthMax=${keylengthMax}/" ${workload}
    sed -i "s/\(fieldlength= \)".*"/fieldlength=${fieldlength}/" ${workload}
    sed -i "s/\(fieldlengthSTDEV= \)".*"/fieldlengthSTDEV=${fieldlengthSTDEV}/" ${workload}
    sed -i "s/\(fieldlengthMin= \)".*"/fieldlengthMin=${fieldlengthMin}/" ${workload}
    sed -i "s/\(fieldlengthMax= \)".*"/fieldlengthMax=${fieldlengthMax}/" ${workload}
    cd /home/elect/ELECTExp/scripts/Exp || exit
}

function flush {
    targetScheme=$1
    waitTime=$2
    echo "Start for flush and wait for compaction of ${targetScheme}"
    # Make local results directory
    if [ ! -d /home/elect/Results/${targetScheme} ]; then
        mkdir -p /home/elect/Results/${targetScheme}
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

    ## Collect load results
    # for ((i = 1; i <= NodeNumber; i++)); do
    #     echo "Copy loading stats of ${targetScheme} back, node$i"
    #     scp -r elect@node$i:/home/elect/Results /home/elect/Results/${targetScheme}/${ExpName}-Compact-Node$i
    #     ssh elect@node$i "rm -rf /home/elect/Results && mkdir -p /home/elect/Results"
    # done
}

function backup {
    targetScheme=$1
    echo "Start copy data of ${targetScheme} to backup, this will kill the online system!!!"
    # Make local results directory
    if [ ! -d /home/elect/Results/${targetScheme} ]; then
        mkdir -p /home/elect/Results/${targetScheme}
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
    elif [[ ${targetScheme} == *"elect"* ]]; then
        sed -i "s/\(mode: \)".*"/mode: elect/" playbook-backup.yaml
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsb/" playbook-backup.yaml
    elif [ ${targetScheme} == "mlsm" ]; then
        sed -i "s/\(mode: \)".*"/mode: mlsm/" playbook-backup.yaml
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsb/" playbook-backup.yaml
    fi
    sed -i "s/Scheme/${targetScheme}/g" playbook-backup.yaml
    sed -i "s/DATAPATH/${ExpName}-${KVNumber}/g" playbook-backup.yaml
    ansible-playbook -v -i hosts.ini playbook-backup.yaml
}

function startup {
    targetScheme=$1
    echo "Start copy data back ${targetScheme} from backup"
    # Make local results directory
    if [ ! -d /home/elect/Results/${targetScheme} ]; then
        mkdir -p /home/elect/Results/${targetScheme}
    fi

    # Copy playbook
    if [ -f "playbook-startup.yaml" ]; then
        rm -rf playbook-startup.yaml
    fi
    cp ../playbook/playbook-startup.yaml .
    # Modify playbook
    if [[ ${targetScheme} == *"elect"* ]]; then
        sed -i "s/\(maxLevel: \)".*"/maxLevel: ${maxLevel}/" playbook-startup.yaml
        sed -i "s/\(initialDelay: \)".*"/initialDelay: ${initialDelay}/" playbook-startup.yaml
        sed -i "s/\(concurrentEC: \)".*"/concurrentEC: ${concurrentEC}/" playbook-startup.yaml
        sed -i "s/\(mode: \)".*"/mode: elect/" playbook-startup.yaml
    else
        sed -i "s/\(maxLevel: \)".*"/maxLevel: 9/" playbook-startup.yaml
        sed -i "s/\(initialDelay: \)".*"/initialDelay: 65536/" playbook-startup.yaml
        sed -i "s/\(concurrentEC: \)".*"/concurrentEC: ${concurrentEC}/" playbook-startup.yaml
        sed -i "s/\(mode: \)".*"/mode: raw/" playbook-startup.yaml
    fi

    if [ ${targetScheme} == "cassandreas" ]; then
        sed -i "s/\(mode: \)".*"/mode: eas/" playbook-startup.yaml
    elif [ ${targetScheme} == "cassandra" ]; then
        sed -i "s/\(mode: \)".*"/mode: raw/" playbook-startup.yaml
        sed -i "s/\(keyspace: \)".*"/keyspace: ycsbraw/" playbook-startup.yaml
    elif [ ${targetScheme} == "elect" ]; then
        sed -i "s/\(mode: \)".*"/mode: elect/" playbook-startup.yaml
    elif [ ${targetScheme} == "elect-trans" ]; then
        sed -i "s/\(mode: \)".*"/mode: elect/" playbook-startup.yaml
        sed -i "s/\(grade: \)".*"/grade: 1/" playbook-startup.yaml
    elif [ ${targetScheme} == "elect-pdm" ]; then
        sed -i "s/\(mode: \)".*"/mode: elect/" playbook-startup.yaml
        sed -i "s/\(grade: \)".*"/grade: 2/" playbook-startup.yaml
    elif [ ${targetScheme} == "elect-rdm" ]; then
        sed -i "s/\(mode: \)".*"/mode: elect/" playbook-startup.yaml
        sed -i "s/\(grade: \)".*"/grade: 3/" playbook-startup.yaml
    elif [ ${targetScheme} == "mlsm" ]; then
        sed -i "s/\(mode: \)".*"/mode: mlsm/" playbook-startup.yaml
    fi

    sed -i "s/Scheme/${targetScheme}/g" playbook-startup.yaml
    sed -i "s/DATAPATH/${ExpName}-${KVNumber}/g" playbook-startup.yaml
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
    if [ ! -d /home/elect/Results/${targetScheme} ]; then
        mkdir -p /home/elect/Results/${targetScheme}
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
        sed -i "s/\(threads: \)".*"/threads: ${threadsNumber}/" playbook-run.yaml
        sed -i "s/\(workload: \)".*"/workload: \"${workload}\"/" playbook-run.yaml
        sed -i "s/\(expName: \)".*"/expName: "${ExpName}-${targetScheme}-Run-${runningType}-Round-${round}"/" playbook-run.yaml
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
            scp -r elect@node$i:/home/elect/Results /home/elect/Results/"${targetScheme}"/"${ExpName}-${workload}-${round}-Node$i"
            ssh elect@node$i "rm -rf /home/elect/Results && mkdir -p /home/elect/Results"
        done
    done
}

function recovery {
    targetScheme=$1
    recoveryNode=$2
    waitTime=$3
    echo "Start copy data back ${targetScheme} from backup"
    # Make local results directory
    if [ ! -d /home/elect/Results/${targetScheme} ]; then
        mkdir -p /home/elect/Results/${targetScheme}
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
    sed -i "s/\(seconds: \)".*"/seconds: ${waitTime}/" playbook-recovery.yaml

    ansible-playbook -v -i hosts.ini playbook-recovery.yaml

    echo "Copy running data of ${targetScheme} back, ${recoveryNode}"
    scp elect@${recoveryNode}:/mnt/ssd/CassandraEC/logs/recovery.log /home/elect/Results/"${targetScheme}"/"${ExpName}-Size-${KVNumber}-recovery-${round}-${recoveryNode}"
}

for KVNumberTemp in "${KVNumberSet[@]}"; do
    KVNumber=${KVNumberTemp}
    echo "Start experiment to ${KVNumber} KV pairs"
    for scheme in "${schemes[@]}"; do
        echo "Start experiment to ${scheme}"
        # Gen params
        dataSizeOnEachNode=$(echo "scale=2; $KVNumber * ($keylength + $fieldlength) / $NodeNumber / 1024 / 1024 / 1024 * 3" | bc)
        initialDellayLocal=$(echo "scale=2; $dataSizeOnEachNode * 7" | bc)
        initialDellayLocalCeil=$(echo "scale=0; (${initialDellayLocal} + 0.5)/1" | bc)
        waitTime=$(echo "scale=2; $dataSizeOnEachNode * 450" | bc)
        if [[ ${scheme} == *"elect"* ]]; then
            waitTime=$(echo "scale=2; ($dataSizeOnEachNode * 1024 / 4 / 2 / ($concurrentEC / 2)) * 100 + $dataSizeOnEachNode * 450" | bc)
            maxLevelCount
            initialDelay=$initialDellayLocalCeil
        else
            maxLevel=9
            initialDelay=65536
        fi
        waitTimeCeil=$(echo "scale=0; (${waitTime} + 0.5)/1" | bc)
        echo "Data size on each node is ${dataSizeOnEachNode} GiB, initial delay is ${initialDelay}, target flush wait time is ${waitTimeCeil}, max level is ${maxLevel}"

        # if [ "${scheme}" == "cassandra" ]; then
        #     if [ $KVNumber == 10000000 ]; then
        #         echo "Continue run cassandra"
        #     else
        #         continue
        #     fi
        # fi
        # continue;
        waitTimeForRecovery=180
        if [ "${scheme}" == "elect-rdm" ]; then
            waitTimeForRecovery=700
        fi

        # Load
        load "${scheme}"

        if [ "${waitFlush}" == "true" ]; then
            flush "${scheme}" "${waitTimeCeil}"
        fi

        if [ "${restart}" == "true" ]; then
            backup "${scheme}"
        fi
        # for ((round = 1; round <= roundNumber; round++)); do
        #     if [ "${restart}" == "true" ]; then
        #         startup "${scheme}"
        #     fi
        #     recovery "${scheme}" "node2" ${waitTimeForRecovery}
        # done
    done
done
