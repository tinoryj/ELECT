#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"
function restartNode {
    sourceDataDir=$1
    configureFilePath=$2

    kill -9 $(ps aux | grep CassandraDaemon | grep -v grep | awk 'NR == 1' | awk {'print $2'})

    echo "Copy DB back from ${sourceDataDir} to ${PathToELECTPrototype}/data"

    if [ ! -d "${sourceDataDir}" ]; then
        echo "Target ${sourceDataDir} does not exist"
        exit
    fi

    cd "${PathToELECTPrototype}" || exit

    rm -rf data
    cp -r ${sourceDataDir} data
    chmod -R 775 data

    if [ -f conf/cassandra.yaml ]; then
        echo "Remove old conf/cassandra.yaml"
        rm conf/cassandra.yaml
    fi
    cp ${configureFilePath} conf/cassandra.yaml

    rm -rf logs
    mkdir -p logs

    nohup bin/cassandra >logs/debug.log 2>&1 &
}

func "$1" "$2"
