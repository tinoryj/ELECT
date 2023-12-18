#!/bin/bash

# Common params for all experiments

NodesList=(172.27.96.1 172.27.96.2 172.27.96.3 172.27.96.4 172.27.96.5 172.27.96.6 172.27.96.7 172.27.96.8 172.27.96.9 172.27.96.10)
OSSServerNode="172.27.96.1"
ClientNode="172.27.96.1"
UserName="elect"
PathToELECT="/home/elect/ELECT/prototype"
PathToYCSB="/home/elect/ELECT/YCSB"
PathToELECTExpDBBackup="/home/elect/ELECTExpDBBackup"
PathToELECTLog="/home/elect/ELECTLog"
PathToELECTResultSummary="/home/elect/ELECTLog"

NodeNumber="${#NodesList[@]}"
SSTableSize=4
LSMTreeFanOutRatio=10
concurrentEC=64
defaultSimulatedClientNumber=16
