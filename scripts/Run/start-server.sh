#!/bin/bash
. /etc/profile
function startServerNode {

    kill -9 $(ps aux | grep CassandraDaemon | grep -v grep | awk 'NR == 1' | awk {'print $2'})

    maxLevel=$1
    initialDelay=$2
    targetStorageSaving=$4
    dataBlockNum=$5
    parityBlockNum=$6
    mode=$7

    cd ${PathToELECTPrototype}

    if [ -f conf/cassandra.yaml ]; then
        echo "Remove old conf/cassandra.yaml"
        rm conf/cassandra.yaml
    fi

    rm -rf data logs
    mkdir -p data/receivedParityHashes/
    mkdir -p data/localParityHashes/
    mkdir -p data/ECMetadata/
    mkdir -p data/tmp/
    mkdir -p logs

    # varify value of maxLevel
    if [ ${mode} == "cassandra" ]; then
        sed -i "s/enable_migration:.*$/enable_migration: false/" conf/cassandra.yaml
        sed -i "s/enable_erasure_coding:.*$/enable_erasure_coding: false/" conf/cassandra.yaml
    else
        sed -i "s/enable_migration:.*$/enable_migration: true/" conf/cassandra.yaml
        sed -i "s/enable_erasure_coding:.*$/enable_erasure_coding: true/" conf/cassandra.yaml
    fi

    sed -i "s/target_storage_saving:.*$/target_storage_saving: ${targetStorageSaving}/" conf/cassandra.yaml
    sed -i "s/ec_data_nodes:.*$/ec_data_nodes: ${dataBlockNum}/" conf/cassandra.yaml
    sed -i "s/parity_nodes:.*$/parity_nodes: ${parityBlockNum}/" conf/cassandra.yaml
    sed -i "s/max_level_count:.*$/max_level_count: ${maxLevel}/" conf/cassandra.yaml
    sed -i "s/initial_delay:.*$/initial_delay: ${initialDelay}/" conf/cassandra.yaml
    sed -i "s/concurrent_ec:.*$/concurrent_ec: ${concurrentEC}/" conf/cassandra.yaml
    sendSSTables=${concurrentEC}/2
    sed -i "s/max_send_sstables:.*$/max_send_sstables: ${sendSSTables}/" conf/cassandra.yaml

    nohup bin/cassandra >logs/debug.log 2>&1 &
}

func "$1" "$2" "$3" "$4" "$5" "$6" "$7" "$8"
