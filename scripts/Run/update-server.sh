#!/bin/bash

. /etc/profile
kill -9 $(ps aux | grep CassandraDaemon | grep -v grep | awk 'NR == 1' | awk {'print $2'})

cd /mnt/ssd/CassandraEC
git checkout yuanming
git pull origin yuanming

my_ip=$(ifconfig eth0 | grep 'inet ' | awk '{print $2}')
echo "my_ip: ${my_ip}"
last_octet=$(echo $my_ip | awk -F'.' '{print $NF}')
echo "last_octet: ${last_octet}"
tokens=(-9223372036854775808 -7378697629483820544 -5534023222112865280 -3689348814741909504 -1844674407370954752 0 1844674407370956800 3689348814741911552 5534023222112866304 7378697629483821056)

selected_token=${tokens[(( last_octet - 1))]}
echo "selected_token: ${selected_token}"
sed -i "s/initial_token:.*$/initial_token: ${selected_token}/" /home/elect/elect.yaml
sed -i "s/rpc_address:.*$/rpc_address: ${my_ip}/" /home/elect/elect.yaml
sed -i "s/listen_address:.*$/listen_address: ${my_ip}/" /home/elect/elect.yaml

cp /home/elect/elect.yaml conf/cassandra.yaml

rm -rf data logs
mkdir -p data/receivedParityHashes/
mkdir -p data/localParityHashes/
mkdir -p data/ECMetadata/
mkdir -p data/tmp/
mkdir -p logs
ant realclean && ant -Duse.jdk11=true

cd src/native/src/org/apache/cassandra/io/erasurecode/
./genlib.sh
rm -rf /mnt/ssd/CassandraEC/lib/sigar-bin/libec.so
cd /mnt/ssd/CassandraEC
cp src/native/src/org/apache/cassandra/io/erasurecode/libec.so lib/sigar-bin

