#!/bin/bash
. /etc/profile
# Common params for all experiments

NodesList=(10.31.0.185 10.31.0.181 10.31.0.182 10.31.0.184 10.31.0.188 10.31.0.180) # The IP addresses of the ELECT cluster nodes
OSSServerNode="10.31.0.190" # The IP address of the OSS server node
OSSServerPort=8000 # The port number of the OSS server node
ClientNode="10.31.0.187" # The IP address of the client node (it can be the local node running the scripts)
UserName="cc" # The user name of all the previous nodes
sudoPasswd="" # The sudo password of all the previous nodes; we use this to automatically install the required packages; we assume all the nodes have the same user name.
PathToArtifact="/home/${UserName}/ELECT" # The path to the artifact folder; we assume all the nodes have the same path.
PathToELECTExpDBBackup="/home/${UserName}/ELECTExpDBBackup" # The path to the backup folder for storing the loaded DB content; we assume all the nodes have the same path.
PathToELECTLog="/home/${UserName}/ELECTLog" # The path to the log folder for storing the experiment logs; we assume all the nodes have the same path.
PathToELECTResultSummary="/home/${UserName}/ELECTResules" # The path to the result summary folder for storing the final experiment results; we assume all the nodes have the same path. 

PathToELECTPrototype="${PathToArtifact}/src/elect"
PathToYCSB="${PathToArtifact}/scripts/ycsb"
PathToScripts="${PathToArtifact}/scripts"
PathToColdTier="${PathToArtifact}/src/coldTier"

NodeNumber="${#NodesList[@]}"
SSTableSize=4
LSMTreeFanOutRatio=10
concurrentEC=64
defaultSimulatedClientNumber=16

FullNodeList=("${NodesList[@]}")
FullNodeList+=("${OSSServerNode}")
FullNodeList+=("${ClientNode}")

# Variable to store the matching interface
networkInterface=""

# Loop through each IP in the list
for ip in "${FullNodeList[@]}"; do
    # Check each IP against all local interfaces
    for interface in $(ip -o -4 addr list | awk '{print $2, $4}'); do
        iface_name=$(echo $interface | cut -d ' ' -f 1)
        iface_ip=$(echo $interface | cut -d ' ' -f 2 | cut -d '/' -f 1)
        
        # If the IP matches, save the interface name
        if [ "$ip" == "$iface_ip" ] && [ "$iface_name" != "lo" ]; then
            networkInterface=$iface_name
            echo $iface_name
            break 2
        fi
    done
done

# Output the result
if [ -n "$networkInterface" ]; then
    echo "Found matching interface: $networkInterface"
else
    echo "No matching interface found for the given IPs."
fi
