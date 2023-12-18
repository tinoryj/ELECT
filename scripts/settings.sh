#!/bin/bash
. /etc/profile
# Common params for all experiments

NodesList=(192.168.10.21 192.168.10.22 192.168.10.23 192.168.10.25 192.168.10.26 192.168.10.28) 
OSSServerNode="192.168.10.27"
ClientNode="192.168.10.29"
UserName="yjren"
PathToELECTPrototype="/home/${UserName}/ELECT/prototype"
PathToYCSB="/home/${UserName}/ELECT/YCSB"
PathToScripts="/home/${UserName}/ELECT/scripts"
PathToELECTExpDBBackup="/home/${UserName}/ELECTExpDBBackup"
PathToELECTLog="/home/${UserName}/ELECTLog"
PathToELECTResultSummary="/home/${UserName}/ELECTLog"

NodeNumber="${#NodesList[@]}"
SSTableSize=4
LSMTreeFanOutRatio=10
concurrentEC=64
defaultSimulatedClientNumber=16
networkInterface="eth0"


FullNodeList=("${NodesList[@]}")
FullNodeList+=("${OSSServerNode}")
FullNodeList+=("${ClientNode}")