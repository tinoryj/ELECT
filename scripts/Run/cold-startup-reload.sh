#!/bin/bash
. /etc/profile
func() {
    cd /mnt/ssd/CassandraEC || exit
    bin/nodetool coldStartup reload
}

func "$1"
