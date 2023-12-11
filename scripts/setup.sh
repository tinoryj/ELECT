#!/bin/bash
NodeNumber=10
for ((i = 1; i <= NodeNumber; i++)); do
    echo "Reset ssh login, node$i"
    ssh-keygen -f "/home/elect/.ssh/known_hosts" -R "172.27.96.$i"
    ssh-copy-id -i ~/.ssh/id_rsa.pub elect@172.27.96.$i
done

ssh-keygen -f "/home/elect/.ssh/known_hosts" -R "localhost"
ssh-copy-id -i ~/.ssh/id_rsa.pub elect@localhost

exit
export OSS_ACCESS_KEY_ID=LTAI5tQEt53urpQUv1YN3o4v
export OSS_ACCESS_KEY_SECRET=bOvnouli699OYw3MRtvDY4haNmwr0f
export _JAVA_OPTIONS='-Xmx12g -Xms2048m -XX:MaxDirectMemorySize=2048m'