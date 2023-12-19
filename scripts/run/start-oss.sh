#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"

function startOSSNode {

    kill -9 $(ps aux | grep FileServer | grep -v grep | awk 'NR == 1' | awk {'print $2'})

    cd ${PathToArtifacts}/ColdTier

    rm -rf data OSSLog.txt
    mkdir -p data

    nohup java FileServer ${OSSServerPort} >${PathToArtifacts}/ColdTier/OSSLog.txt 2>&1 &
}

startServerNode
