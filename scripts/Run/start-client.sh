#!/bin/bash
. /etc/profile

kill -9 $(ps aux | grep ycsb | grep -v grep | awk 'NR == 1' | awk {'print $2'})
func() {
    coordinator=$1
    sstable_size=$2
    fanout_size=$3
    mode=$4

    cd /home/elect/cassandra || exit

    if [ $mode == "raw" ]; then
        echo "Create keyspace ycsbraw;"
        bin/cqlsh "$coordinator" -e "create keyspace ycsbraw WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 3 };
        USE ycsbraw;
        create table usertable0 (y_id varchar primary key, field0 varchar);
        ALTER TABLE usertable0 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': $sstable_size, 'fanout_size': $fanout_size};
        ALTER TABLE ycsbraw.usertable0 WITH compression = {'enabled':'false'};
        consistency all;"
    else
        echo "Create keyspace ycsb;"
        bin/cqlsh "$coordinator" -e "create keyspace ycsb WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 3 };
        USE ycsb;
        create table usertable0 (y_id varchar primary key, field0 varchar);
        ALTER TABLE usertable0 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': $sstable_size, 'fanout_size': $fanout_size};
        ALTER TABLE usertable1 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': $sstable_size, 'fanout_size': $fanout_size};
        ALTER TABLE usertable2 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': $sstable_size, 'fanout_size': $fanout_size};
        ALTER TABLE ycsb.usertable0 WITH compression = {'enabled':'false'};
        ALTER TABLE ycsb.usertable1 WITH compression = {'enabled':'false'};
        ALTER TABLE ycsb.usertable2 WITH compression = {'enabled':'false'};
        consistency all;"
    fi
}

func "$1" "$2" "$3" "$4"
