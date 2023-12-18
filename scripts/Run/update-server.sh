#!/bin/bash
. /etc/profile
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/../Common.sh"

kill -9 $(ps aux | grep CassandraDaemon | grep -v grep | awk 'NR == 1' | awk {'print $2'})

cd ${PathToELECTPrototype}
git checkout master
git pull origin master

my_ip=$(ifconfig ${networkInterface} | grep 'inet ' | awk '{print $2}')
echo "my_ip: ${my_ip}"
last_octet=$(echo $my_ip | awk -F'.' '{print $NF}')
echo "last_octet: ${last_octet}"
new_tokens=( $(generate_tokens) )
echo "tokens: ${new_tokens[*]}"

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
rm -rf ${PathToELECTPrototype}/lib/sigar-bin/libec.so
cd ${PathToELECTPrototype}
cp src/native/src/org/apache/cassandra/io/erasurecode/libec.so lib/sigar-bin

