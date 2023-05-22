#!/bin/bash

cd /mnt/ssd/Debug/CassandraEC

git checkout yuanming
git pull origin yuanming
kill -9 $(ps aux | grep cassandra| grep -v grep | awk 'NR == 1'  | awk {'print $2'})
rm -rf data logs
mkdir -p data/receivedParityHashes/
mkdir -p data/localParityHashes/
mkdir -p data/ECMetadata/
mkdir -p data/tmp/
mkdir -p logs
ant -Duse.jdk11=true
cp src/native/src/org/apache/cassandra/io/erasurecode/libec.so lib/sigar-bin
bin/cassandra &> logs/debug.log