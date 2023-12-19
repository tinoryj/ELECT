#!/bin/bash
. /etc/profile
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/settings.sh"


# SSH keygen on control node
if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -q -t rsa -b 2048 -N "" -f ~/.ssh/id_rsa
fi
# SSH key-free connection from control node to all nodes
for nodeIP in "${NodesList[@]}" "${OSSServerNode}" "${ClientNode}"; do
    ssh-keyscan -H ${nodeIP} >> ~/.ssh/known_hosts
done

for nodeIP in "${NodesList[@]}" "${OSSServerNode}" "${ClientNode}"; do
    echo "Set SSH key-free connection to node ${nodeIP}"
    ssh-copy-id -i ~/.ssh/id_rsa.pub ${UserName}@${nodeIP}
done

# Install packages
printf  ${sudoPasswd} | sudo -S apt update 
printf  ${sudoPasswd} | sudo -S apt install -y ant maven clang llvm python3 ansible python3-pip #libisal-dev openjdk-11-jdk openjdk-11-jre 
pip install cassandra-driver

if [ ! -d "${PathToELECTResultSummary}" ]; then
    mkdir -p ${PathToELECTResultSummary}
fi

FullNodeList=("${NodesList[@]}")
FullNodeList+=("${OSSServerNode}")
FullNodeList+=("${ClientNode}")

for nodeIP in "${FullNodeList[@]}"; do
    echo "Set up each nodes"
    ssh ${UserName}@${nodeIP} "cd ${PathToScripts}; bash setupOnEachNode.sh"
done