<?xml version="1.0" encoding="UTF-8"?>

<!--
 ~ Licensed to the Apache Software Foundation (ASF) under one
 ~ or more contributor license agreements.  See the NOTICE file
 ~ distributed with this work for additional information
 ~ regarding copyright ownership.  The ASF licenses this file
 ~ to you under the Apache License, Version 2.0 (the
 ~ "License"); you may not use this file except in compliance
 ~ with the License.  You may obtain a copy of the License at
 ~
 ~   http://www.apache.org/licenses/LICENSE-2.0
 ~
 ~ Unless required by applicable law or agreed to in writing,
 ~ software distributed under the License is distributed on an
 ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ~ KIND, either express or implied.  See the License for the
 ~ specific language governing permissions and limitations
 ~ under the License.
-->

# ELECT Implementation Log

## Build

```shell
# Build with java 11
ant clean
ant -Duse.jdk11=true
# Set Java Home for isa-l library 
export JAVA_HOME=/usr/lib/jvm/java-1.11.0-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

Build ec library first: 

```shell
sudo apt install libisal-dev
cd src/native/src/org/apache/cassandra/io/erasurecode/
chmod +x genlib.sh 
./genlib.sh
```
