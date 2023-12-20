# Instructions to reproduce the evaluations of the paper

Here are the detailed instructions to perform the same experiments in our paper.

## Artifact claims

We claim that the resultant numbers might differ from those in our paper due to various factors (e.g., cluster sizes, machines, OS, software packages, etc.). Nevertheless, we expect ELECT to still achieve similar performance (in normal operations) with Cassandra while significantly reducing storage overhead (i.e., our main results).

## Environment setup (5 human-minutes + ~ 40 compute-minutes)

We provide scripts to set up the environment for the evaluation. The scripts are tested on Ubuntu 22.04 LTS. Note that the running time of the scripts depends on the node number, network bandwidth, and the performance of the cluster nodes.

**Step 1:** Set up and check the user account and sudo password on each node. We assume all the nodes have the same user name and password. We use will the user name and password to automatically setup the running environment. In additon, please also check whether each node have the same network interface name (for the given IP address). If not, please modify the `networkInterface` variable in `scripts/settings.sh` on each node.

**Step 2:** Set up the cluster node info in `scripts/settings.sh` on each node. Please fill in the following variables in the script. Note that we assume all the nodes have the same configurations (e.g., same user name, same path to the artifact folder, same network interface name, etc.).

```shell
NodesList=(10.31.0.185 10.31.0.181 10.31.0.182 10.31.0.184 10.31.0.188 10.31.0.180) # The IP addresses of the ELECT cluster nodes
OSSServerNode="10.31.0.190" # The IP address of the OSS server node
OSSServerPort=8000 # The port number of the OSS server node
ClientNode="10.31.0.187" # The IP address of the client node (it can be the local node running the scripts)
UserName="cc" # The user name of all the previous nodes
sudoPasswd="" # The sudo password of all the previous nodes; we use this to automatically install the required packages; we assume all the nodes have the same user name. For the chamelone cloud, please keep this as empty.
PathToArtifact="/home/${UserName}/ELECT" # The path to the artifact folder; we assume all the nodes have the same path.
PathToELECTExpDBBackup="/home/${UserName}/ELECTExpDBBackup" # The path to the backup folder for storing the loaded DB content; we assume all the nodes have the same path.
PathToELECTLog="/home/${UserName}/ELECTLog" # The path to the log folder for storing the experiment logs; we assume all the nodes have the same path.
PathToELECTResultSummary="/home/${UserName}/ELECTResules" # The path to the result summary folder for storing the final experiment results; we assume all the nodes have the same path. 
```

**Step 3:** Run the following script on one of the nodes (we suggest running on the client node). This script will install the required packages, set up the environment variables, and set up the SSH connection between the nodes.

```shell
bash scripts/setup.sh
```

## Evaluations

Note: To reduce the influence of cloud storage location, hardware requirement, complexity, and running time of the evaluation, we made some changes to the evaluation configurations.

* We require a single client node, six server nodes, and one storage node (as the cold tier) in AE.
* We replaced the Alibaba OSS with a server node within the same cluster as the higher tier to store the cold data.
* We reduced the number of test cases in some evaluations, such as Exp#6,7,10.

To simplify the reproduction process, we provide an `Ansible`-based script to run all the experiments. The script will automatically run the experiments and generate the result logs. The scripts will take about 9~10 days to finish all the experiments. We suggest **run the scripts of Exp#2 first**, which can reproduce the main results (i.e., achieve controllable storage saving compared with Cassandra; provide similar performance of different types of KV operations such as read, write, scan, and update) of our paper.

### Overall system analysis (Exp#1~5 in our paper)

#### Exp#1: Performance with YCSB core workloads

#### Exp#2: Micro-benchmarks on KV operations

#### Exp#3: Performance breakdown

#### Exp#4: Full-node recovery

#### Exp#5: Resource usage

### Parameter analysis (Exp#6~8 in our paper)

#### Exp#6: Impact of key and value sizes

#### Exp#7: Impact of storage-saving target

#### Exp#8: Impact of erasure coding parameters

### System setting analysis (Exp#9,10 in our paper)

#### Exp#9: Impact of read consistency level

#### Exp#10: Impact of number of clients