#!/bin/bash
source settings.sh

source settings.sh

# SSH keygen on control node
if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -q -t rsa -b 2048 -N "" -f ~/.ssh/id_rsa
fi
# SSH key-free connection from control node to all nodes
for nodeIP in "${NodesList[@]}"; do
    echo "Set SSH key-free connection to node ${nodeIP}"
    ssh-copy-id -i ~/.ssh/id_rsa.pub ${UserName}@${nodeIP}
done

ssh-copy-id -i ~/.ssh/id_rsa.pub ${UserName}@${OSSServerNode}
ssh-copy-id -i ~/.ssh/id_rsa.pub ${UserName}@${ClientNode}


# Install packages
sudo apt-get update 
sudo apt install openjdk-11-jdk openjdk-11-jre ant maven clang llvm libisal-dev python3 ansible python3-pip 
pip install cassandra-driver

if [ ! -d "${PathToELECTResultSummary}" ]; then
    mkdir -p ${PathToELECTResultSummary}
fi

FullNodeList=("${NodesList[@]}")
FullNodeList+=("${OSSServerNode}")
FullNodeList+=("${ClientNode}")

for nodeIP in "${NodesList[@]}"; do
    echo "Set up each nodes"
    ssh ${UserName}@${nodeIP} "cd ${PathToELECTPrototype}/../scripts/; bash scripts/setupOnEachNode.sh"
done