# Instructions to reproduce the evaluations of the paper

Here are the detailed instructions to perform the same experiments in our paper.

## Artifact claims

We claim that the resultant numbers might be different from in our paper due to various factors (e.g., different cluster size, different machines, different OS, different software packages...). Nevertheless, we expect that ELECT should still achieve similar performance (in normal operations) with Cassandra while significantly reduce storage overhead (i.e., our main results).

## Environment setup

We provide scripts to setup the environment for the evaluation. The scripts are tested on Ubuntu 22.04 LTS.

## Evaluations

Note: to reduce the hardware requirement, complexity, and also running time of the evaluation, we made some changes to the evaluation configurations.

* We use a single client node with six server nodes in AE.
* We replaced the Alibaba OSS with a server node within the same cluster as the higher tier to store the cold data.
* We reduced the number of test case in some evaluations such as the Exp#6,7,10.

To simplify the reproduce process, we provide anible-based script to run all the experiments. The script will automatically run the experiments and generate the result logs. The scripts will take about 9~10 days to finish all the experiments.

### Overall system analysis (Exp#1~5,9,10 in our paper)

Note: To reduce the long running time, we pre-load 10M KV pairs for the following evaluations. Especially, we reduced the workload size in Exp#1 to utilize the pre-loaded data. The storage saving and performance trend should be similar to the original evaluation.

#### Exp#1: Performance with YCSB core workloads



#### Exp#2: Micro-benchmarks on KV operations

#### Exp#3: Performance breakdown

#### Exp#4: Full-node recovery

#### Exp#5: Resource usage

#### Exp#9: Impact of read consistency level

#### Exp#10: Impact of number of clients

### Parameter analysis (Exp#6~8 in our paper)

#### Exp#6: Impact of key and value sizes

#### Exp#7: Impact of storage saving target

#### Exp#8: Impact of erasure codig parameters
