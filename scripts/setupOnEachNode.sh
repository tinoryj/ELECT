#!/bon/bash
source settings.sh

# SSH keygen on control node
if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -q -t rsa -b 2048 -N "" -f ~/.ssh/id_rsa
fi
# SSH key-free connection from control node to all nodes
for nodeIP in "${FullNodeList[@]}"; do
    echo "Set SSH key-free connection to node ${nodeIP}"
    ssh-copy-id -i ~/.ssh/id_rsa.pub ${UserName}@${nodeIP}
done

# Install packages
sudo apt-get update 
sudo apt install openjdk-11-jdk openjdk-11-jre ant maven clang llvm libisal-dev python3 ansible python3-pip 
pip install cassandra-driver

# Java configuration
export _JAVA_OPTIONS='-Xmx12g -Xms2048m -XX:MaxDirectMemorySize=2048m'

if [ ! -d "${PathToELECTExpDBBackup}" ]; then
    mkdir -p ${PathToELECTExpDBBackup}
fi

if [ ! -d "${PathToELECTLog}" ]; then
    mkdir -p ${PathToELECTLog}
fi

if [ ! -d "${PathToELECTResultSummary}" ]; then
    mkdir -p ${PathToELECTResultSummary}
fi