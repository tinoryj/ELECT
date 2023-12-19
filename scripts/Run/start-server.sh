#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../Common.sh"

function startServerNode {

    kill -9 $(ps aux | grep CassandraDaemon | grep -v grep | awk 'NR == 1' | awk {'print $2'})

    maxLevel=$1
    initialDelay=$2
    targetStorageSaving=$3
    dataBlockNum=$4
    parityBlockNum=$5
    mode=$6

    cd ${PathToELECTPrototype}
    if [ ! -f conf/cassandra.yaml ]; then
        if [ ! -f ${PathToELECTPrototype}/elect.yaml ]; then
            echo "No elect.yaml or cassandra.yaml configuration file found, error"
            exit
        else
            cp ${PathToELECTPrototype}/elect.yaml ${PathToELECTPrototype}/conf/cassandra.yaml
        fi
    fi

    rm -rf data logs
    mkdir -p data/receivedParityHashes/
    mkdir -p data/localParityHashes/
    mkdir -p data/ECMetadata/
    mkdir -p data/tmp/
    mkdir -p logs

    # varify mode
    if [ ${mode} == "cassandra" ]; then
        sed -i "s/enable_migration:.*$/enable_migration: false/" conf/cassandra.yaml
        sed -i "s/enable_erasure_coding:.*$/enable_erasure_coding: false/" conf/cassandra.yaml
    else
        sed -i "s/enable_migration:.*$/enable_migration: true/" conf/cassandra.yaml
        sed -i "s/enable_erasure_coding:.*$/enable_erasure_coding: true/" conf/cassandra.yaml
        sed -i "s/target_storage_saving:.*$/target_storage_saving: ${targetStorageSaving}/" conf/cassandra.yaml
        sed -i "s/ec_data_nodes:.*$/ec_data_nodes: ${dataBlockNum}/" conf/cassandra.yaml
        sed -i "s/parity_nodes:.*$/parity_nodes: ${parityBlockNum}/" conf/cassandra.yaml
        sed -i "s/max_level_count:.*$/max_level_count: ${maxLevel}/" conf/cassandra.yaml
        sed -i "s/initial_delay:.*$/initial_delay: ${initialDelay}/" conf/cassandra.yaml
        sed -i "s/concurrent_ec:.*$/concurrent_ec: ${concurrentEC}/" conf/cassandra.yaml
        sendSSTables=${concurrentEC}/2
        sed -i "s/max_send_sstables:.*$/max_send_sstables: ${sendSSTables}/" conf/cassandra.yaml
    fi

    nohup bin/cassandra >logs/debug.log 2>&1 &
}

startServerNode "$1" "$2" "$3" "$4" "$5" "$6"
