#!/bin/bash
. /etc/profile
func() {

    mode=$1
    cd /mnt/ssd/CassandraEC || exit
    bin/nodetool coldStartup reload
}

func "$1"
