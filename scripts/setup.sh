#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/settings.sh"

# SSH keygen on control node
if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -q -t rsa -b 2048 -N "" -f ~/.ssh/id_rsa
fi
# SSH key-free connection from control node to all nodes

for nodeIP in "${NodesList[@]}" "${OSSServerNode}" "${ClientNode}"; do
    if [ ${UserName} == "cc" ]; then
        scp ~/.ssh/config cc@${nodeIP}:~/.ssh/
        scp ~/.ssh/id_rsa cc@${nodeIP}:~/.ssh/
    else
        echo "Set SSH key-free connection to node ${nodeIP}"
        ssh-keyscan -H ${nodeIP} >>~/.ssh/known_hosts
        ssh-copy-id -i ~/.ssh/id_rsa.pub ${UserName}@${nodeIP}
    fi
done

for nodeIP in "${NodesList[@]}" "${OSSServerNode}" "${ClientNode}"; do
    ssh ${UserName}@${nodeIP} "rm -rf ${PathToArtifact}"
    scp -r ${PathToArtifact} ${UserName}@${nodeIP}:~/
done

# Install packages
if [ ! -z "${sudoPasswd}" ]; then
    printf ${sudoPasswd} | sudo -S apt-get update
    printf ${sudoPasswd} | sudo -S apt-get install -y ant maven clang llvm python3 ansible python3-pip libisal-dev openjdk-11-jdk openjdk-11-jre
else
    sudo apt-get update
    sudo apt-get install -y ant maven clang llvm python3 ansible python3-pip libisal-dev openjdk-11-jdk openjdk-11-jre
fi

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
