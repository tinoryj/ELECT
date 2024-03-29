# Instructions to reproduce the evaluations of the paper

Here are the detailed instructions to perform the same experiments in our paper.

## Artifact claims

We claim that the results might differ from those in our paper due to various factors (e.g., cluster sizes, machines, OS, software packages, etc.). Nevertheless, we expect ELECT to still achieve similar performance (in normal operations) with Cassandra while significantly reducing storage overhead (i.e., our main results). In addition, to reduce the influence of cloud storage location, hardware requirement, complexity, and running time of the evaluation, we made some changes to the evaluation configurations.

* We require a single client node, six server nodes, and one storage node (as the cold tier) in AE.
* We replaced the Alibaba OSS with a server node within the same cluster as the cold tier to store the cold data.

## Environment setup (5 human-minutes + ~ 40 compute-minutes)

We provide scripts to set up the environment for the evaluation. The scripts are tested on Ubuntu 22.04 LTS. Note that the running time of the scripts depends on the node number, network bandwidth, and the performance of the cluster nodes.

**Step 1:** Set up and check each node's user account and sudo password. We assume all the nodes have the same username and password. We use the user name and password to set up the running environment automatically. 

**Step 2:** Set up the cluster node info in `scripts/settings.sh` on each node. Please fill in the following variables in the script. Note that we assume all the nodes have the same configurations (e.g., same user name, same path to the artifact folder, same network interface name, etc.).

```shell
NodesList=(10.31.0.185 10.31.0.181 10.31.0.182 10.31.0.184 10.31.0.188 10.31.0.180) # The IP addresses of the ELECT cluster nodes
OSSServerNode="10.31.0.190" # The IP address of the OSS server node
OSSServerPort=8000 # The port number of the OSS server node
ClientNode="10.31.0.187" # The IP address of the client node (it can be the local node running the scripts)
UserName="cc" # The user name of all the previous nodes
sudoPasswd="" # The sudo password of all the previous nodes; we use this to install the required packages automatically; we assume all the nodes have the same user name. For the Chameleon cloud, please keep this as empty.
PathToArtifact="/home/${UserName}/ELECT" # The path to the artifact folder; we assume all the nodes have the same path.
PathToELECTExpDBBackup="/home/${UserName}/ELECTExpDBBackup" # The path to the backup folder for storing the loaded DB content; we assume all the nodes have the same path.
PathToELECTLog="/home/${UserName}/ELECTLog" # The path to the log folder for storing the experiment logs; we assume all the nodes have the same path.
PathToELECTResultSummary="/home/${UserName}/ELECTResules" # The path to the result summary folder for storing the final experiment results; we assume all the nodes have the same path. 
```

**Step 3:** Run the following script on one of the nodes (we suggest running on the client node). This script will install the required packages, set up the environment variables, and set up the SSH connection between the nodes.

```shell
bash scripts/setup.sh full
```

## Evaluations

This section describes how to reproduce the evaluations in our paper. To simplify the reproduction process, we provide Ansible-based scripts to run all the experiments. The script will automatically run the experiments and generate the result logs. The scripts will take about 9~10 days to finish all the experiments. **We suggest running the scripts of Exp#0 first, which can reproduce the main results of our paper while including most of the functionality verification (i.e., achieve controllable storage saving compared with Cassandra; provide similar performance of different types of KV operations such as read, write, scan, and update)**.

### Note on the experiment scripts

These evaluation scripts require a long time to run. To avoid the interruption of the experiments, we suggest running the scripts in the background with `tmux`, `nohup`, `screen`, etc. In addition, please make sure that all scripts have been given execution permissions. You can do this according to the following example:

```shell
cd scripts
find . -type f -name "*.sh" -exec chmod +x {} \;
```

### Note on the evaluation results

The raw running log generated by the YCSB benchmark tool will be stored in the folder `${PathToELECTResultSummary}/` configured in the `scripts/settings.sh` file. The log file will be named with detailed configuration information, such as workload, KV number, operation number, etc.

To make the result easier to read, we provide the summarized result of each evaluation in the `scripts/exp/` folder and named `${ExpName}.log`. For example, the result of Exp#1 will be stored in `scripts/exp/Exp1-ycsb.log`.

For the **performance evaluation**, the result will be summarized in different ways according to the running round number of the experiment (defined in each of the experiment scripts by the variable `RunningRoundNumber`).

* If the running round number is 1, the result will be directly output as in the example shown below.

```shell
[Exp info] scheme: elect, workload: Write, KVNumber: 600000, OPNumber: 60000, KeySize: 24, ValueSize: 1000, ClientNumber: 16, ConsistencyLevel: ONE, ExtraFlag: 
Throughput (unit: op/s): 
Only one round: 9992.38
[READ] Average operation latency (unit: us):
Only one round: 1369.53
[READ] 99th percentile latency (unit: us):
Only one round: 1883.00
```

* If the running round number is between 1 and 5, the result will be output with the average, maximum, and minimum, as shown in the example below.

```shell
[Exp info] scheme: elect, workload: Write, KVNumber: 600000, OPNumber: 60000, KeySize: 24, ValueSize: 1000, ClientNumber: 16, ConsistencyLevel: ONE, ExtraFlag: 
Throughput (unit: op/s): 
Average: 9992.38, Min:  9753.433208489389, Max:  10231.330379889298
[READ] Average operation latency (unit: us):
Average: 1369.53, Min: 1355.774261, Max: 1383.292994
[READ] 99th percentile latency (unit: us):
Average: 1883.00, Min: 1823, Max: 1943
```

* If the running round number is more than or equal to 5, the result will be output with the average and 95% student-t distribution confidence interval, as shown in the example below.

```shell
[Exp info] scheme: elect, workload: Write, KVNumber: 600000, OPNumber: 60000, KeySize: 24, ValueSize: 1000, ClientNumber: 16, ConsistencyLevel: ONE, ExtraFlag: 
Throughput (unit: op/s): 
Average: 31219.24; The 95% confidence interval: (30463.56, 31974.92)
[READ] Average operation latency (unit: us):
Average: 465.35; The 95% confidence interval: (452.85, 477.85)
[READ] 99th percentile latency (unit: us):
Average: 1623.00; The 95% confidence interval: (1553.32, 1692.68)
```

For the **storage overhead evaluation**, the result will be summarized based on the total, hot-tier, and cold-tier storage overhead. For example:

```shell
[Exp info] Scheme: elect, KVNumber: 100000000, KeySize: 24, ValueSize: 1000
Total storage overhead (unit: GiB): 185.90
Hot-tier storage overhead (unit: GiB): 134.10
Cold-tier storage overhead (unit: GiB): 51.80
```

For **other evaluations (i.e., Exp#3, 4, and 5)**, the result will be summarized similarly to the summarized performance results. Again, depending on the number of running rounds conducted, the output formats include options such as a single-round summary (run experiment with one round) or more comprehensive data sets featuring average, maximum, and minimum values (run experiment with 2~4 rounds), as well as average values with a 95% Student-T distribution confidence interval (run experiment more than five rounds). We provide examples of the summarized results of operation breakdown, recovery time cost, and resource usage in each related evaluation.

### Simple experiment (For quick verification)

#### Exp#0: Simple experiment (1 human-minutes + ~ 10 compute-hours)

We provide this simple experiment to verify our main experimental results quickly: ELECT provides similar performance compared to Cassandra while significantly reducing hot-tier storage overhead. Specifically, we use 10M KV pairs and 1M KV operations (including read/write/update/scan, consistent with Exp2). This experiment will provide storage overhead (main results of Exp#1,2), performance of normal and degraded operations (main results of Exp#2), KV operation breakdown (main results of Exp#3), recovery time overhead when a single node fails (main results of Exp#4), and average resource utilization under load/normal/degraded conditions (main results of Exp#5). The summarized results will be printed on the screen after the evaluation and saved in the `scripts/exp/Exp0-simpleOverall.log` file.

You can run this simple experiment via the following command:

```shell
bash scripts/exp/Exp0-simpleOverall.sh
```

The results will be output in the order shown below. Here, we only show the title line and output sequence of each part. The specific result format of each part is as shown in the "Note on the evaluation results" above and the example of each specific experiment below.

```shell
The storage overhead results:
...
The performance evaluation results:
...
The operation breakdown evaluation results:
...
The full-node recovery time cost results:
...
The resource usage evaluation results:
...
```

### Overall system analysis (Exp#1~5 in our paper)

#### Exp#1: Performance with YCSB core workloads (1 human minutes + ~ 20 compute-hours)

*Running:*

```shell
bash scripts/exp/Exp1-ycsb.sh
```

#### Exp#2: Micro-benchmarks on KV operations (1 human-minutes + ~ 5 compute-hours)

*Running:*

```shell
bash scripts/exp/Exp2-operations.sh
```

#### Exp#3: Performance breakdown (1 human-minutes + ~ 5 compute-hours)

*Running:*

```shell
bash scripts/exp/Exp3-breakdown.sh
```

*Results:* These summaries are available in the `scripts/exp/` directory and can be found in the file named `Exp3-breakdown.log`. For example, the write operation breakdown result of ELECT will be output as in the example shown below. Note that the title for each metric is the same as the title in the paper.

```shell
[Breakdown info for Write] scheme: elect, KVNumber: 6000000, KeySize: 24, ValueSize: 1000
WAL (unit: ms/MiB):
Only one round: 388.95
MemTable (unit: ms/MiB):
Only one round: 686.65
Flushing (unit: ms/MiB):
Only one round: 200.79
Compaction (unit: ms/MiB):
Only one round: 1638.06
Transitioning (unit: ms/MiB):
Only one round: 259.84
Migration (unit: ms/MiB):
Only one round: 17.72
...
```

#### Exp#4: Full-node recovery (1 human-minutes + ~ 14 compute-hours)

*Running:*

```shell
bash scripts/exp/Exp4-recovery.sh
```

*Results:* These summaries are available in the `scripts/exp/` directory and can be found in the file named `Exp4-recovery.log`.

* For ELECT, the recovery time is the time cost of retrieving the LSM-trees from the replication nodes and decoding the SSTables. The result will be output as in the example shown below.

```shell
[Exp info] scheme: elect, KVNumber: 6000000, KeySize: 24, ValueSize: 1000
Total recovery time cost (unit: s):
Average: 6653.00, Min: 6653, Max: 6653
Recovery time cost for retrieve LSM-trees (unit: s):
Average: 3442.00, Min: 3442, Max: 3442
Recovery time cost for decode SSTables (unit: s):
Average: 3211.00, Min: 3211, Max: 3211
```

* For Cassandra, the recovery time is the time cost of retrieving the SSTables from the replication only. The result will be output as in the example shown below.
    
```shell
[Exp info] scheme: cassandra, KVNumber: 6000000, KeySize: 24, ValueSize: 1000
Total recovery time cost (unit: s):
Only one round: 7515.00
```

#### Exp#5: Resource usage (1 human-minutes + ~ 5 compute-hours)

*Running:*

```shell
bash scripts/exp/Exp5-resource.sh
```

*Results:* We summarize resource utilization as the 95th percentile of CPU usage, the 95th percentile of total memory overhead, total disk I/O, and total network overhead (bidirectional). In particular, we obtain the 95% percentile CPU usage based on the sum of the CPU usage of all nodes with the same timestamp and then calculate the average usage of each core (i.e., total usage/total number of cores). Therefore, the CPU usage results will be significantly affected by the differences in hardware configurations of different testbeds. The results will be output as in the example shown below.

```shell
[Resource usage with degraded operations] scheme: elect, KVNumber: 6000000, KeySize: 24, ValueSize: 1000, OPNumber: 600000
95%-percentile CPU Usage (%):
Only one round: 1.19
95%-percentile RAM Usage (GiB):
Only one round: 22.98
Total Disk I/O (GiB):
Only one round: 1.08
Total Network traffic (GiB):
Only one round: 202.38
```

### Parameter analysis (Exp#6~8 in our paper)

#### Exp#6: Impact of key and value sizes (1 human-minutes + ~ 40 compute-hours)

*Running:*

```shell
bash scripts/exp/Exp6-kvSize.sh
```

#### Exp#7: Impact of storage-saving target (1 human-minutes + ~ 45 compute-hours)

*Running:*

```shell
bash scripts/exp/Exp7-balanceParam.sh
```

#### Exp#8: Impact of erasure coding parameters (1 human-minutes + ~ 12 compute-hours)

The original experiment requires at least 12 nodes (1 client node, 10 server nodes, and 1 storage node). For the provided testbeds, limited by the number of available nodes, we adapt the changing range of erasure code K from 4~8 to 2~4. This result only verifies ELECT's adaptability to different K values.

*Running:*

```shell
bash scripts/exp/Exp8-ecParam.sh
```

### System setting analysis (Exp#9,10 in our paper)

#### Exp#9: Impact of read consistency level (1 human-minutes + ~ 5 compute-hours)

*Running:*

```shell
bash scripts/exp/Exp9-consistency.sh
```

#### Exp#10: Impact of number of clients (1 human minutes + ~ 5 compute-hours)

*Running:*

```shell
bash scripts/exp/Exp10-clients.sh
```
