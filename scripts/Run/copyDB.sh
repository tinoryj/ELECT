#!/bin/bash
. /etc/profile
func() {

    echo "Copy DB from $1 to $2"
    sourceDataDir=$1
    targetDataDir=$2
    mode=$3

    rm -rf "${targetDataDir}"
    if [ ! -d "${targetDataDir}" ]; then
        mkdir -p "${targetDataDir}"
    fi

    if [ ! -d "${sourceDataDir}" ]; then
        echo "Target ${sourceDataDir} does not exist"
        exit
    fi

    cd /mnt/ssd/CassandraEC || exit

    bin/nodetool coldStartup backup

    sleep 10

    bin/nodetool drain

    kill -9 $(ps aux | grep CassandraDaemon | grep -v grep | awk 'NR == 1' | awk {'print $2'})

    cp -r "${sourceDataDir}" "${targetDataDir}"
}

func "$1" "$2" "$3"
