#!/bin/bash
. /etc/profile

targetHostInfo=$1
totalNodeNumber=$2
nodeList=$3
OSSNode=$4
clientNode=$5


echo "[elect_servers]" >> ${targetHostInfo}
for ((i = 1; i <= totalNodeNumber; i++)); do
    echo "server${i} ansible_host=${nodeList[$i]}" >> ${targetHostInfo}
done
echo "[elect_oss]" >> ${targetHostInfo}
echo "oss ansible_host=${OSSNode}" >> ${targetHostInfo}
echo "[elect_client]" >> ${targetHostInfo}
echo "client ansible_host=${clientNode}" >> ${targetHostInfo}
# random select the failure node from the total node list
failure_nodes=($(shuf -i 1-${totalNodeNumber} -n 1))
echo "[elect_failure]" >> ${targetHostInfo}
echo "server${failure_nodes} ansible_host=${nodeList[$failure_nodes]}" >> ${targetHostInfo}

