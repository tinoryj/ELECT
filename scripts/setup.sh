#!/bin/bash
NodeNumber=10

for ((i = 1; i <= NodeNumber; i++)); do
    echo "Setup ssh login, node$i"
    ssh-copy-id -i ~/.ssh/id_rsa.pub elect@172.27.96.$i
done

ssh-keygen -f "/home/elect/.ssh/known_hosts" -R "localhost"
ssh-copy-id -i ~/.ssh/id_rsa.pub elect@localhost

export _JAVA_OPTIONS='-Xmx12g -Xms2048m -XX:MaxDirectMemorySize=2048m'