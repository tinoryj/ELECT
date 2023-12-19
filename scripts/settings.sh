#!/bin/bash
. /etc/profile
# Common params for all experiments

NodesList=(192.168.0.21 192.168.0.22 192.168.0.23 192.168.0.25 192.168.0.26 192.168.0.28) # The IP addresses of the ELECT cluster nodes
OSSServerNode="192.168.0.27" # The IP address of the OSS server node
ClientNode="192.168.0.29" # The IP address of the client node (it can be the local node running the scripts)
UserName="yjren" # The user name of all the previous nodes
sudoPasswd="yjren" # The sudo password of all the previous nodes; we use this to automatically install the required packages; we assume all the nodes have the same user name.
networkInterface="eth0" # the network interface name (for the given IP address) of all the previous nodes; we assume all the nodes have the same network interface name.
PathToArtifact="/home/${UserName}/ELECT" # The path to the artifact folder; we assume all the nodes have the same path.
PathToELECTExpDBBackup="/home/${UserName}/ELECTExpDBBackup" # The path to the backup folder for storing the loaded DB content; we assume all the nodes have the same path.
PathToELECTLog="/home/${UserName}/ELECTLog" # The path to the log folder for storing the experiment logs; we assume all the nodes have the same path.
PathToELECTResultSummary="/home/${UserName}/ELECTResules" # The path to the result summary folder for storing the final experiment results; we assume all the nodes have the same path. 

PathToELECTPrototype="${PathToArtifact}/Prototype"
PathToYCSB="${PathToArtifact}/YCSB"
PathToScripts="${PathToArtifact}/scripts"

NodeNumber="${#NodesList[@]}"
SSTableSize=4
LSMTreeFanOutRatio=10
concurrentEC=64
defaultSimulatedClientNumber=16

FullNodeList=("${NodesList[@]}")
FullNodeList+=("${OSSServerNode}")
FullNodeList+=("${ClientNode}")