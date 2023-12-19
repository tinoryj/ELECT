#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../Common.sh"

function startOSSNode {

    kill -9 $(ps aux | grep FileServer | grep -v grep | awk 'NR == 1' | awk {'print $2'})

    cd ${PathToArtifacts}/ColdTier

    rm -rf data log
    mkdir -p data

    nohup java FileServer 8000 >OSSLog.txt 2>&1 &
}

startServerNode "$1" "$2" "$3" "$4" "$5" "$6"
