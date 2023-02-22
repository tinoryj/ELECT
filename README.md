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

# CassandraEC Implementation Log

## Build

```shell
# Build with java 11
ant -Duse.jdk11=true
```

## Notes


1. SSTables' management:
   1. src/java/org/apache/cassandra/db/ColumnFamilyStore.java
   2. src/java/org/apache/cassandra/db/compaction/CompactionStrategyManager.java
   3. get SSTables' level info: /home/tinoryj/Projects/CassandraEC/src/java/org/apache/cassandra/db/compaction/LeveledGenerations.java-> getAllLevelSuize();
   4. get SSTables' corresponding level: SSTableReader->getSSTableLevel();


## Note to Java

1. The Throwable class is the superclass of all errors and exceptions in the Java language. Only objects that are instances of this class (or one of its subclasses) are thrown by the Java Virtual Machine or can be thrown by the Java throw statement. Similarly, only this class or one of its subclasses can be the argument type in a catch clause. For the purposes of compile-time checking of exceptions, Throwable and any subclass of Throwable that is not also a subclass of either RuntimeException or Error are regarded as checked exceptions.