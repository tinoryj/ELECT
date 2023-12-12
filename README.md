# ELECT

## Introduction

ELECT is a distributed tiered KV store that enables replication and erasure coding tiering. This repo contains the implementation of the ELECT prototype, YCSB benchmark tool, and evaluation scripts used in our USENIX FAST 2024 paper.

* `./Prototype`: includes the implementation of the ELECT prototype.
* `./YCSB`: includes the modified version of YCSB which supports user-defined key and value sizes.
* `./scripts`: includes the prototype setup and evaluation scripts.

## Prerequisites

### Testbed

As a distributed KV store, ELECT requires a cluster of machines to run. With the default erasure coding parameters (i.e., [n,k]==[6,4]), ELECT requires a minimum of 6 machines as the storage nodes. In addition, to avoid unstable access to Alibaba OSS, we use a server node within the same cluster as the cold tier to store the cold data. We also need a client node to run the YCSB benchmark tool. Therefore, we need at least 8 machines to run the prototype. 

### Dependencies

* For Java project build (used for Prototype and YCSB): openjdk-11-jdk, openjdk-11-jre, ant, maven.
* For erasure-coding library build (used for Prototype via JNI): clang, llvm, libisal-dev.
* For scripts: python3, ansible.

The packages above can be directly installed via `apt` package manager:

```shell 
sudo apt install openjdk-11-jdk openjdk-11-jre ant maven clang llvm libisal-dev python3 ansible
```

Note that, the dependencies for both ELECT and YCSB will be automatically installed via maven during compile.

## Build

### Environment setup (5 human-minutes)

The build procedure of both the ELECT prototype and YCSB requires an internet connection to download the dependencies via Maven. In case the internet connection requires a proxy, we provide an example maven setting file `./scripts/env/settings.xml`. Please modify the file according to your proxy settings and then put it into the local Maven directory as shown below.

```shell
mkdir -p ~/.m2
cp ./scripts/env/settings.xml ~/.m2/
```

### Prototype (5 human-minutes + ~ 40 compute-minutes)

Since the Prototype utilizes the Intel Isa-L library to achieve high-performance erasure coding, we need to build the EC library first:

```shell
# Set Java Home for isa-l library 
export JAVA_HOME=/usr/lib/jvm/java-1.11.0-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
# Build the JNI-based erasure coding library
cd ./Prototype
cd ELECT/src/native/src/org/apache/cassandra/io/erasurecode/
chmod +x genlib.sh 
./genlib.sh
```

Then, we can build the ELECT prototype:

```shell
# Build with java 11
cd ./Prototype
mkdir build lib
ant realclean
ant -Duse.jdk11=true
```

### YCSB (2 human-minutes + ~ 30 compute-minutes)

We build the modified version of YCSB which supports user-defined key and value sizes. The build procedure is similar to the original YCSB.

```shell
cd ./YCSB
mvn clean package
```

## Configuration

### Cluster setup (~20 human-minutes)

To run the experiment scripts, SSH key-free access is required between all nodes in the cluster.

Step 1: Generate the SSH key pair on each node.

```shell 
ssh-keygen -q -t rsa -b 2048 -N "" -f ~/.ssh/id_rsa
```

Step 2: Create an SSH configuration file. You can run the following command with the specific node IP, Port, and User to generate the configuration file. Note that, you can run the command multiple times to add all the nodes to the configuration file.

```shell
# Replace xxx to the correct IP, Port, and User information, and replace ${i} to the correct node ID.
cat <<EOT >> ~/.ssh/config
Host node${i}
    StrictHostKeyChecking no
    HostName xxx.xxx.xxx.xxx
    Port xx
    User xxx
EOT
```

Step 3: Copy the SSH public key to all the nodes in the cluster.

```shell
ssh-copy-id node${i}
```

### Configuring ELECT (~20 human-minutes)

The ELECT prototype requires to configure the cluster information before running. We provide an example configuration file `./Prototype/conf/cassandra.yaml`. Please modify the file according to your cluster settings and the instructions shown below (lines 11-34).

```shell
cluster_name: 'ELECT Cluster'

# ELECT settings
ec_data_nodes: 4 # The erasure coding parameter (k)
parity_nodes: 2 # The erasure coding parameter (n - k)
target_storage_saving: 0.6 # The balance parameter (storage saving target), which controls the approximate storage saving ratio of the cold tier.
enable_migration: true # Enable the migration module to migrate cold data to the cold tier.
enable_erasure_coding: true # Enable redundancy transitioning module to encode the cold data.
# Manual settings to balance workload across different nodes.
initial_token: -9223372036854775808 # The initial token of the current node.
token_ranges: -9223372036854775808,-6148914691236517376,-3074457345618258944,0,3074457345618257920,6148914691236515840 # The initial token ranges of all nodes in the cluster.
# Current node settings
listen_address: 192.168.10.21 # IP address of the current node.
rpc_address: 192.168.10.21 # IP address of the current node.
seed_provider:
  # Addresses of hosts that are deemed contact points.
  # Cassandra nodes use this list of hosts to find each other and learn
  # the topology of the ring.  You must change this if you are running
  # multiple nodes!
  - class_name: org.apache.cassandra.locator.SimpleSeedProvider
    parameters:
      # ELECT: put all server nodes IP here.
      # Ex: "<ip1>,<ip2>,<ip3>"
      - seeds: "192.168.10.21,192.168.10.22,192.168.10.23,192.168.10.25,192.168.10.26,192.168.10.28" # IP address of all the server nodes.
```

To simplify the configuration of `initial_token` and `token_ranges`, which is important for ELECT to achieve optimal storage saving. We provide a script `./scripts/genToken.sh` to generate the token ranges for all the nodes in the cluster with the given node number. 

```shell
cd ./scripts
python3 genToken.py ${n} # Replace ${n} to the number of nodes in the cluster.
# Sample output:
[Node 1]
initial_token: -9223372036854775808
[Node 2]
initial_token: -6148914691236517376
[Node 3]
initial_token: -3074457345618258944
[Node 4]
initial_token: 0
[Node 5]
initial_token: 3074457345618257920
[Node 6]
initial_token: 6148914691236515840
```

After getting the initial token for each node, please fill the generated number into the `initial_token` and `token_ranges` fields in the configuration file.

## Running 

### Run the ELECT cluster (~5 human-minutes + ~3 compute-time)

After configuring the cluster information in the `cassandra.yaml` file on each of the server nodes, we can run the ELECT cluster with the following command (on each server node):

```shell
cd Prototype
mkdir logs data # Create the log and data directories.
nohup bin/cassandra >logs/debug.log 2>&1 &
```

It will take about 1~2 minutes to fully set up the cluster. You can check the cluster status via the following command:

```shell
cd Prototype
bin/nodetool ring
```

Once the cluster is ready, you can see the information of all nodes in the cluster on each of the server nodes. Note that each node in the cluster should own the same percentage of the consistent hashing ring. For example:

```shell

```

### Run YCSB benchmark

After the ELECT cluster is set up, we can run the YCSB benchmark tool on the client node to evaluate the performance of ELECT. 

```shell
cd YCSB
bin/ycsb run cassandra-cql -p hosts=${NodesList} -p cassandra.readconsistencylevel=${consistency} -p cassandra.keyspace=${keyspace} -p cassandra.tracing="false" -threads $threads -s -P workloads/${workload}
````

```shell
cd YCSB
bin/ycsb load cassandra-cql -p hosts=${NodesList} -p cassandra.keyspace=${keyspace} -p cassandra.tracing="false" -threads ${threads} -s -P workloads/${workload}
```
