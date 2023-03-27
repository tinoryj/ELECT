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
ant clean
ant -Duse.jdk11=true
# Set Java Home for isa-l library 
export JAVA_HOME=/usr/lib/jvm/java-1.11.0-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

## Test 

```shell 
ant -Duse.jdk11=true testsome -Dtest.name=<TestClassName> -Dtest.methods=<testMethodName>
# For example 
ant -Duse.jdk11=true testsome -Dtest.name=ErasureCodeTest
```

## Sync Messages

1. Erasure coding:

* Delete old files: src/native/, src/java/org/apache/cassandra/utils/erasurecode/.
* Add new files: src/native/, src/java/org/apache/cassandra/io/erasurecode/, test/long/org/apache/cassandra/io/erasure/, src/java/org/apache/cassandra/exceptions/ErasureCodeException.java.
* Replace files: src/java/org/apache/cassandra/exceptions/ExceptionCode.java

Build ec library first: 

```shell
sudo apt install libisal-dev
cd src/native/src/org/apache/cassandra/io/erasurecode/
chmod +x genlib.sh 
./genlib.sh
```

## Plan

### Add EC metadata to avoid SSTables' integrity check fails after the redundancy transition

1. Check all "Component.DATA"
2. Add a new component `EC_METADATA` for EC metadata in Component.java
   1. In progress: SSTableReaderBuilder.java: ibuilder+dbuilder
3. Add new writer for SSTableWriter and BigTableWriter to write new SSTs with only EC_METADATA rather than DATA component.
4. Add isSSTableTransferred flag in `StatsMetadata` and `Version`.

## Notes

1. SSTables' management:

   1. src/java/org/apache/cassandra/db/ColumnFamilyStore.java
   2. src/java/org/apache/cassandra/db/compaction/CompactionStrategyManager.java
   3. get SSTables' level info: /home/tinoryj/Projects/CassandraEC/src/java/org/apache/cassandra/db/compaction/LeveledGenerations.java-> getAllLevelSuize();
   4. get SSTables' corresponding level: SSTableReader->getSSTableLevel();
   5. SSTableReaders are open()ed by Keyspace.onStart; after that they are created by SSTableWriter.renameAndOpen.
   6. SSTableReader open()
   7. SingleSSTavleLCSTask -> only change level, but not perform compaction.
   8. Load SSTable: SSTableLoader.java -> openSSTables()
   9. FileHandle->Builder(AutoCloseable) : determine how the file will be read (compression/mapped/cached).
2. Open SSTable:

   1. `public static SSTableReader open(Descriptor descriptor, Set<Component> components, TableMetadataRef metadata, boolean validate, boolean isOffline)`
   2. Components: https://developer.aliyun.com/article/701157

   ```java
        DATA("Data.db"),
        // index of the row keys with pointers to their positions in the data file
        PRIMARY_INDEX("Index.db"),
        // serialized bloom filter for the row keys in the sstable
        FILTER("Filter.db"),
        // file to hold information about uncompressed data length, chunk offsets etc.
        COMPRESSION_INFO("CompressionInfo.db"),
        // statistical metadata about the content of the sstable
        STATS("Statistics.db"),
        // holds CRC32 checksum of the data file
        DIGEST("Digest.crc32"),
        // holds the CRC32 for chunks in an a uncompressed file.
        CRC("CRC.db"),
        // holds SSTable Index Summary (sampling of Index component)
        SUMMARY("Summary.db"),
        // table of contents, stores the list of all components for the sstable
        TOC("TOC.txt"),
        // built-in secondary index (may be multiple per sstable)
        SECONDARY_INDEX("SI_.*.db"),
        // custom component, used by e.g. custom compaction strategy
        CUSTOM(null);
   ```

   Java file.length() -> length of the file in bits

   3. SSTable name format: <version> - <generation> - <implementation> - <component> - <ID> - <level>.db
      * cfs -> 0++
      * SSTableReader -> getSSTableLevel()
   4. Compaction: OneSSTableOperation() function-> execute()

## Note to Java

1. The Throwable class is the superclass of all errors and exceptions in the Java language. Only objects that are instances of this class (or one of its subclasses) are thrown by the Java Virtual Machine or can be thrown by the Java throw statement. Similarly, only this class or one of its subclasses can be the argument type in a catch clause. For the purposes of compile-time checking of exceptions, Throwable and any subclass of Throwable that is not also a subclass of either RuntimeException or Error are regarded as checked exceptions.
