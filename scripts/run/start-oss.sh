#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"

function startOSSNode {

    kill -9 $(ps aux | grep FileServer | grep -v grep | awk 'NR == 1' | awk {'print $2'})

    cd ${PathToArtifact}/src/coldTier || exit

    # rm -rf data OSSLog.txt
    if [ ! -d "data" ]; then
        mkdir -p data
    fi

    nohup java FileServer ${OSSServerPort} >>${PathToArtifact}/ColdTier/OSSLog.txt 2>&1 &
}

startOSSNode
