# ELECT

## Introduction

ELECT is a distributed tiered KV store that enables replication and erasure coding tiering. This repo contains the implementation of the ELECT prototype, YCSB benchmark tool, and evaluation scripts used in our USENIX FAST 2024 paper.

* `./Prototype`: includes the implementation of the ELECT prototype.
* `./YCSB`: includes the modified version of YCSB which supports user-defined key and value sizes.
* `./scripts`: includes the prototype setup and evaluation scripts.

## Prerequisites

### Testbed

As a distributed KV store, ELECT requires a cluster of machines to run. With the default erasure coding parameters (i.e., [n,k]==[6,4]), ELECT requires a minimum of 6 machines as the storage nodes. In addition, to avoid the long procedure of setting up Alibaba OSS, we use a server node within the same cluster as the higher tier to store the cold data. We also needs a client node to run the YCSB benchmark tool. Therefore, we need at least 8 machines to run the prototype. 

### Dependencies

* For Java project build (used for Prototype and YCSB): openjdk-11-jdk, openjdk-11-jre, ant, maven.
* For erasure-coding library build (used for Prototype via JNI): clang, llvm, libisal-dev.

The packages above can be directly installed via apt-get:

```shell 
sudo apt install openjdk-11-jdk openjdk-11-jre ant maven clang llvm libisal-dev
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

