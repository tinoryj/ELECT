#!/bin/bash
. /etc/profile
# Common params for all experiments
NodesList=(192.168.0.21 192.168.0.22 192.168.0.23 192.168.0.25 192.168.0.26 192.168.0.28) # The IP addresses of the ELECT cluster nodes
OSSServerNode="192.168.0.27" # The IP address of the OSS server node
OSSServerPort=8000 # The port number of the OSS server node
ClientNode="192.168.0.29" # The IP address of the client node (it can be the local node running the scripts)
UserName="yjren" # The user name of all the previous nodes
sudoPasswd="yjren" # The sudo password of all the previous nodes; we use this to install the required packages automatically; we assume all the nodes have the same user name. For the Chameleon cloud, please keep this as empty.
PathToArtifact="/mnt/ssd/ELECT" # The path to the artifact folder; we assume all the nodes have the same path.
PathToELECTExpDBBackup="/mnt/ssd/ELECTExpDBBackup" # The path to the backup folder for storing the loaded DB content; we assume all the nodes have the same path.
PathToELECTLog="/mnt/ssd/ELECTLogs" # The path to the log folder for storing the experiment logs; we assume all the nodes have the same path.
PathToELECTResultSummary="/home/${UserName}/ELECTResults" # The path to the result summary folder for storing the final experiment results; we assume all the nodes have the same path. 

PathToELECTPrototype="${PathToArtifact}/src/elect"
PathToYCSB="${PathToArtifact}/scripts/ycsb"
PathToScripts="${PathToArtifact}/scripts"
PathToColdTier="${PathToArtifact}/src/coldTier"

NodeNumber="${#NodesList[@]}"
SSTableSize=4
LSMTreeFanOutRatio=10
concurrentEC=64
defaultSimulatedClientNumber=16

NodesList=($(printf "%s\n" "${NodesList[@]}" | sort -V))
FullNodeList=("${NodesList[@]}")
FullNodeList+=("${OSSServerNode}")
FullNodeList+=("${ClientNode}")

# Variable to store the matching interface
networkInterface=""

# Loop through each IP in the list
for ip in "${FullNodeList[@]}"; do
    # Check each IP against all local interfaces
    while IFS= read -r line; do
        iface_name=$(echo "$line" | awk '{print $2}')
        iface_ip=$(echo "$line" | awk '{print $4}' | cut -d'/' -f1)

        # If the IP matches, save the interface name
        if [[ "$ip" == "$iface_ip" ]]; then
            networkInterface=$iface_name
            break 2
        fi
    done < <(ip -o -4 addr list)
done

# Output the result
# if [ ! -n "$networkInterface" ]; then
#     echo "ERROR no matching interface found for the given IPs. The node should not be used in the experiment."
# fi
