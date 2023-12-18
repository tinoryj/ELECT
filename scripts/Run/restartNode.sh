#!/bin/bash
source ../settings.sh
function restartNode {

    kill -9 $(ps aux | grep CassandraDaemon | grep -v grep | awk 'NR == 1' | awk {'print $2'})

    sourceDataDir=$1

    echo "Copy DB back from ${PathToELECTExpDBBackup}/${sourceDataDir} to ${PathToELECTPrototype}/data"

    if [ ! -d "${sourceDataDir}" ]; then
        echo "Target ${sourceDataDir} does not exist"
        exit
    fi

    cd "${PathToELECTPrototype}" || exit

    rm -rf data
    cp -r " ${PathToELECTExpDBBackup}/${sourceDataDir}" data
    chmod -R 775 data

    if [ -f conf/cassandra.yaml ]; then
        echo "Remove old conf/cassandra.yaml"
        rm conf/cassandra.yaml
    fi
    cp data/cassandra.yaml conf/cassandra.yaml

    rm -rf logs
    mkdir -p logs

    nohup bin/cassandra >logs/debug.log 2>&1 &
}

func "$1"