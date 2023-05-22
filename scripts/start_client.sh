#!/bin/bash

func() {
    coordinator=$1
    sstable_size=$2
    fanout_size=$3
    cd /home/yjren/cassandra
    bin/cqlsh $coordinator -e "create keyspace ycsb WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 3 };
    USE ycsb;
    create table usertable (y_id varchar primary key, field0 varchar);
    ALTER TABLE usertable WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': $sstable_size, 'fanout_size': $fanout_size};
    ALTER TABLE usertable1 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': $sstable_size, 'fanout_size': $fanout_size};
    ALTER TABLE usertable2 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': $sstable_size, 'fanout_size': $fanout_size};
    consistency all;"
}

func "$1" "$2" "$3"