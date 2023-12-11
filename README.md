# ELECT

## Introduction

ELECT is a distributed tiered KV store that enables replication and erasure coding tiering. This repo contains the implementation of the ELECT prototype, YCSB benchmark tool, and evaluation scripts used in our USENIX FAST 2024 paper.

* `./ELECT`: includes the implementation of the ELECT prototype.
* `./YCSB`: includes the modified version of YCSB which supports user-defined key and value sizes.
* `./scripts`: includes the prototype setup and evaluation scripts.

## Dependencies

* For Java project build (used for ELECT and YCSB): openjdk-11-jdk, openjdk-11-jre, ant, maven.
* For erasure-coding library build (used for ELECT via JNI): clang, llvm, libisal-dev.

The packages above can be directly installed via apt-get:

```shell 
sudo apt install openjdk-11-jdk openjdk-11-jre ant maven clang llvm libisal-dev
```

Note that, the dependencies for both ELECT and YCSB will be automatically installed via maven during compile.

## Build

### ELECT 

Since ELECT utilizes the Intel Isa-L library to achieve high-performance erasure coding, we need to build the EC library first:

```shell
# Set Java Home for isa-l library 
export JAVA_HOME=/usr/lib/jvm/java-1.11.0-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
# Build the JNI-based erasure coding library
cd ELECT/src/native/src/org/apache/cassandra/io/erasurecode/
chmod +x genlib.sh 
./genlib.sh
```

Then, we can build the ELECT prototype:

```shell
# Build with java 11
ant clean
ant -Duse.jdk11=true
```

### YCSB

