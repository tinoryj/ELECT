#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"
PathToELECTResultSummary=/home/tinoryj/Projects/ELECT/scripts/count/results

expName=$1
targetScheme=$2
KVNumber=$3
keylength=$4
fieldlength=$5
OPNumber=$6
# codingK=${6:-4}
# storageSavingTarget=${7:-0.6}

function calculate {
    values=("$@")
    num_elements=${#values[@]}

    if [ $num_elements -eq 1 ]; then
        printf "Only one round: %.2f\n" "${values[0]}"
    elif [ $num_elements -lt 5 ]; then
        min=${values[0]}
        max=${values[0]}
        sum=0
        for i in "${values[@]}"; do
            if (($(echo "$i < $min" | bc -l))); then
                min=$i
            fi
            if (($(echo "$i > $max" | bc -l))); then
                max=$i
            fi
            sum=$(echo "$sum + $i" | bc -l)
        done
        avg=$(echo "scale=2; $sum / $num_elements" | bc -l)
        echo "Average: $avg, Min: $min, Max: $max"
    elif [ $num_elements -ge 5 ]; then
        values_csv=$(printf ",%.2f" "${values[@]}")
        values_csv=${values_csv:1}
        python3 -c "
import numpy as np
import scipy.stats

data = np.array([${values_csv[*]}])
confidence = 0.95
mean = np.mean(data)
sem = scipy.stats.sem(data)
interval = scipy.stats.t.interval(confidence, len(data)-1, loc=mean, scale=sem)

print('Average: {:.2f}; The 95% confidence interval: ({:.2f}, {:.2f})'.format(mean, interval[0], interval[1]))
"
    fi
}

memtableTimeCostList=()
commitLogTimeCostList=()
flushTimeCostList=()
compactionTimeCostList=()
rewriteTimeCostList=()
ecsstableCompactionTimeCostList=()
encodingTimeCostList=()
migrateRawSSTableTimeCostList=()
migrateRawSSTableSendTimeCostList=()
migrateParityCodeTimeCostList=()
readIndexTimeCostList=()
readCacheTimeCostList=()
readMemtableTimeCostList=()
readSSTableTimeCostList=()
readMigratedRawDataTimeCostList=()
waitForMigrationTimeCostList=()
waitForRecoveryTimeCostList=()
retrieveTimeCostList=()
decodingTimeCostList=()
readMigratedParityTimeCostList=()
retrieveLsmTreeTimeCostList=()
recoveryLsmTreeTimeCostList=()

function fetchContent {
    file=$1
    fileContent=$(${file})

    memtableTimeCost=$(echo "$fileContent" | grep -oP 'Memtable time cost: \K[0-9]+')
    commitLogTimeCost=$(echo "$fileContent" | grep -oP 'CommitLog time cost: \K[0-9]+')
    flushTimeCost=$(echo "$fileContent" | grep -oP 'Flush time cost: \K[0-9]+')
    compactionTimeCost=$(echo "$fileContent" | grep -oP 'Compaction time cost: \K[0-9]+')
    rewriteTimeCost=$(echo "$fileContent" | grep -oP 'Rewrite time cost: \K[0-9]+')
    ecsstableCompactionTimeCost=$(echo "$fileContent" | grep -oP 'ECSSTable compaction time cost: \K[0-9]+')
    encodingTimeCost=$(echo "$fileContent" | grep -oP 'Encoding time cost: \K[0-9]+')
    migrateRawSSTableTimeCost=$(echo "$fileContent" | grep -oP 'Migrate raw SSTable time cost: \K[0-9]+')
    migrateRawSSTableSendTimeCost=$(echo "$fileContent" | grep -oP 'Migrate raw SSTable send time cost: \K[0-9]+')
    migrateParityCodeTimeCost=$(echo "$fileContent" | grep -oP 'Migrate parity code time cost: \K[0-9]+')

    # "Read operations"
    readIndexTimeCost=$(echo "$fileContent" | grep -oP 'Read index time cost: \K[0-9]+')
    readCacheTimeCost=$(echo "$fileContent" | grep -oP 'Read cache time cost: \K[0-9]+')
    readMemtableTimeCost=$(echo "$fileContent" | grep -oP 'Read memtable time cost: \K[0-9]+')
    readSSTableTimeCost=$(echo "$fileContent" | grep -oP 'Read SSTable time cost: \K[0-9]+')
    readMigratedRawDataTimeCost=$(echo "$fileContent" | grep -oP 'Read migrated raw data time cost: \K[0-9]+')
    waitForMigrationTimeCost=$(echo "$fileContent" | grep -oP 'Wait for migration time cost: \K[0-9]+')
    waitForRecoveryTimeCost=$(echo "$fileContent" | grep -oP 'Wait for recovery time cost: \K[0-9]+')
    retrieveTimeCost=$(echo "$fileContent" | grep -oP 'Retrieve time cost: \K[0-9]+')
    decodingTimeCost=$(echo "$fileContent" | grep -oP 'Decoding time cost: \K[0-9]+')
    readMigratedParityTimeCost=$(echo "$fileContent" | grep -oP 'Read migrated parity time cost: \K[0-9]+')

    # "Recovery operations"
    retrieveLsmTreeTimeCost=$(echo "$fileContent" | grep -oP 'Retrieve LSM tree time cost: \K[0-9]+')
    recoveryLsmTreeTimeCost=$(echo "$fileContent" | grep -oP 'Recovery LSM tree time cost: \K[0-9]+')

    memtableTimeCostList+=(${memtableTimeCost})
    commitLogTimeCostList+=(${commitLogTimeCost})
    flushTimeCostList+=(${flushTimeCost})
    compactionTimeCostList+=(${compactionTimeCost})
    rewriteTimeCostList+=(${rewriteTimeCost})
    ecsstableCompactionTimeCostList+=(${ecsstableCompactionTimeCost})
    encodingTimeCostList+=(${encodingTimeCost})
    migrateRawSSTableTimeCostList+=(${migrateRawSSTableTimeCost})
    migrateRawSSTableSendTimeCostList+=(${migrateRawSSTableSendTimeCost})
    migrateParityCodeTimeCostList+=(${migrateParityCodeTimeCost})
    readIndexTimeCostList+=(${readIndexTimeCost})
    readCacheTimeCostList+=(${readCacheTimeCost})
    readMemtableTimeCostList+=(${readMemtableTimeCost})
    readSSTableTimeCostList+=(${readSSTableTimeCost})
    readMigratedRawDataTimeCostList+=(${readMigratedRawDataTimeCost})
    waitForMigrationTimeCostList+=(${waitForMigrationTimeCost})
    waitForRecoveryTimeCostList+=(${waitForRecoveryTimeCost})
    retrieveTimeCostList+=(${retrieveTimeCost})
    decodingTimeCostList+=(${decodingTimeCost})
    readMigratedParityTimeCostList+=(${readMigratedParityTimeCost})
    retrieveLsmTreeTimeCostList+=(${retrieveLsmTreeTimeCost})
    recoveryLsmTreeTimeCostList+=(${recoveryLsmTreeTimeCost})
}

function calculateWithDataSizeForMS {
    totalDataSizeInMiB=$(echo "scale=2; ${KVNumber} * (${keylength} + ${fieldlength}) / 1024 / 1024" | bc -l)
    targetArray=("$@")
    newArray=()
    arrayLength=${#targetArray[@]}
    for ((i = 0; i < $arrayLength; i++)); do
        sum=$(echo "scale=2; ${targetArray[i]} / $totalDataSizeInMiB" | bc -l)
        newArray+=($sum)
    done
    calculate "${newArray[@]}"
}

function calculateWithDataSizeForNS {
    totalDataSizeInMiB=$(echo "scale=2; ${KVNumber} * (${keylength} + ${fieldlength}) / 1024 / 1024" | bc -l)
    targetArray=("$@")
    newArray=()
    arrayLength=${#targetArray[@]}
    for ((i = 0; i < $arrayLength; i++)); do
        sum=$(echo "scale=2; ${targetArray[i]} / 1000000 / $totalDataSizeInMiB" | bc -l)
        newArray+=($sum)
    done
    calculate "${newArray[@]}"
}

function calculateWithDataSizeForUS {
    totalDataSizeInMiB=$(echo "scale=2; ${KVNumber} * (${keylength} + ${fieldlength}) / 1024 / 1024" | bc -l)
    targetArray=("$@")
    newArray=()
    arrayLength=${#targetArray[@]}
    for ((i = 0; i < $arrayLength; i++)); do
        sum=$(echo "scale=2; ${targetArray[i]} / 1000 / $totalDataSizeInMiB" | bc -l)
        newArray+=($sum)
    done
    calculate "${newArray[@]}"
}

function generateForELECT {
    filePathList=("$@")
    for file in "${filePathList[@]}"; do
        fetchContent "${file}"
    done

    echo -e "\033[1m\033[34m[Breakdown info for Write] scheme: ${targetScheme}, KVNumber: ${KVNumber}, KeySize: ${keylength}, ValueSize: ${fieldlength}\033[0m"
    echo -e "WAL (unit: ms/MiB):"
    calculateWithDataSizeForMS "${commitLogTimeCostList[@]}"
    echo -e "MemTable (unit: ms/MiB):"
    calculateWithDataSizeForMS "${memtableTimeCostList[@]}"
    echo -e "Flushing (unit: ms/MiB):"
    calculateWithDataSizeForMS "${flushTimeCostList[@]}"
    echo -e "Compaction (unit: ms/MiB):"
    calculateWithDataSizeForMS "${compactionTimeCostList[@]}"
    echo -e "Transitioning (unit: ms/MiB):"
    transitioningArray=()
    arrayLengthTransition=${#rewriteTimeCostList[@]}
    for ((i = 0; i < $arrayLengthTransition; i++)); do
        sum=$(echo "scale=2; ${rewriteTimeCostList[i]} + ${ecsstableCompactionTimeCostList[i]} + ${encodingTimeCostList[i]}" | bc -l)
        transitioningArray+=($sum)
    done
    calculateWithDataSizeForMS "${transitioningArray[@]}"
    echo -e "Migration (unit: ms/MiB):"
    migrationArray=()
    arrayLengthMigration=${#migrateRawSSTableTimeCostList[@]}
    for ((i = 0; i < $arrayLengthMigration; i++)); do
        sum=$(echo "scale=2; ${migrateRawSSTableTimeCostList[i]} + ${migrateParityCodeTimeCostList[i]}" | bc -l)
        migrationArray+=($sum)
    done
    calculateWithDataSizeForMS "${migrationArray[@]}"

    echo -e "\033[1m\033[34m[Breakdown info for normal Read] scheme: ${targetScheme}, KVNumber: ${KVNumber}, OPNumber: ${OPNumber} KeySize: ${keylength}, ValueSize: ${fieldlength}\033[0m"

    echo -e "Cache (unit: ms/MiB):"
    calculateWithDataSizeForNS "${readCacheTimeCostList[@]}"
    echo -e "MemTable (unit: ms/MiB):"
    calculateWithDataSizeForMS "${readMemtableTimeCostList[@]}"
    echo -e "SSTables (unit: ms/MiB):"
    calculateWithDataSizeForMS "${readSSTableTimeCostList[@]}"

    echo -e "\033[1m\033[34m[Breakdown info for degraded Read] scheme: ${targetScheme}, KVNumber: ${KVNumber}, OPNumber: ${OPNumber} KeySize: ${keylength}, ValueSize: ${fieldlength}\033[0m"

    echo -e "Cache (unit: ms/MiB):"
    calculateWithDataSizeForNS "${readCacheTimeCostList[@]}"
    echo -e "MemTable (unit: ms/MiB):"
    calculateWithDataSizeForMS "${readMemtableTimeCostList[@]}"
    echo -e "SSTables (unit: ms/MiB):"
    calculateWithDataSizeForMS "${readSSTableTimeCostList[@]}"
    echo -e "Recovery (unit: ms/MiB):"
    calculateWithDataSizeForUS "${waitForRecoveryTimeCostList[@]}"
}

function generateForCassandra {
    filePathList=("$@")
    for file in "${filePathList[@]}"; do
        fetchContent "${file}"
    done
}

function processRecoveryResults {
    file_dict=()
    for file in "$PathToELECTResultSummary"/*; do
        if [[ $file =~ ${expName}-Scheme-${targetScheme}-Size-${KVNumber}-recovery-Round-[0-9]+-RecoverNode-[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+-Time-[0-9]+ ]]; then
            file_dict+=("$file")
        fi
    done
    # echo "${file_dict[@]}"
    if [ "$targetScheme" == "elect" ]; then
        generateForELECT "${file_dict[@]}"
        echo -e "\033[1m\033[34m[Exp info] scheme: ${targetScheme}, KVNumber: ${KVNumber}, KeySize: ${keylength}, ValueSize: ${fieldlength}\033[0m"
        echo -e "\033[31;1mTotal recovery time cost (unit: s):\033[0m"
        calculate "${totalRecoveryTimeCostListForELECT[@]}"
        echo -e "\033[31;1mRecovery time cost for retrieve LSM-trees (unit: s):\033[0m"
        calculate "${totalRetrieveCostListForELECT[@]}"
        echo -e "\033[31;1mRecovery time cost for decode SSTables (unit: s):\033[0m"
        calculate "${totalDecodeTimeCostListForELECT[@]}"
    else
        generateForCassandra "${file_dict[@]}"
        echo -e "\033[1m\033[34m[Exp info] scheme: ${targetScheme}, KVNumber: ${KVNumber}, KeySize: ${keylength}, ValueSize: ${fieldlength}\033[0m"
        echo -e "\033[31;1mTotal recovery time cost (unit: s):\033[0m"
        calculate "${totalRepairCostListForCassandra[@]}"
    fi
}

processRecoveryResults
echo ""
