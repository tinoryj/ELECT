/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.compaction.writers;

import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.compaction.LeveledManifest;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

public class MajorLeveledCompactionWriter extends CompactionAwareWriter {
    private final long maxSSTableSize;
    private int currentLevel = 1;
    private long averageEstimatedKeysPerSSTable;
    private long partitionsWritten = 0;
    private long totalWrittenInLevel = 0;
    private int sstablesWritten = 0;
    private final long keysPerSSTable;
    private Directories.DataDirectory sstableDirectory;
    private final int levelFanoutSize;

    public MajorLeveledCompactionWriter(ColumnFamilyStore cfs,
            Directories directories,
            LifecycleTransaction txn,
            Set<SSTableReader> nonExpiredSSTables,
            long maxSSTableSize) {
        this(cfs, directories, txn, nonExpiredSSTables, maxSSTableSize, false);
    }

    @SuppressWarnings("resource")
    public MajorLeveledCompactionWriter(ColumnFamilyStore cfs,
            Directories directories,
            LifecycleTransaction txn,
            Set<SSTableReader> nonExpiredSSTables,
            long maxSSTableSize,
            boolean keepOriginals) {
        super(cfs, directories, txn, nonExpiredSSTables, keepOriginals);
        this.maxSSTableSize = maxSSTableSize;
        this.levelFanoutSize = cfs.getLevelFanoutSize();
        long estimatedSSTables = Math.max(1, SSTableReader.getTotalBytes(nonExpiredSSTables) / maxSSTableSize);
        keysPerSSTable = estimatedTotalKeys / estimatedSSTables;
    }

    @Override
    @SuppressWarnings("resource")
    public boolean realAppend(UnfilteredRowIterator partition) {
        if(sstableWriter.currentWriter().first != null && sstableWriter.currentWriter().first.compareTo(partition.partitionKey()) >= 0) {
            logger.debug("rymError: MajorLeveledCompactionWriter first key {} is larger than right key {}",
                         sstableWriter.currentWriter().first.getToken(),
                         sstableWriter.currentWriter().last.getToken());
        }
        if(sstableWriter.currentWriter().getEstimatedOnDiskBytesWritten() <= 1024) {
            sstableWriter.currentWriter().first = partition.partitionKey();
        }
        RowIndexEntry rie = sstableWriter.append(partition);
        partitionsWritten++;
        long totalWrittenInCurrentWriter = sstableWriter.currentWriter().getEstimatedOnDiskBytesWritten();
        if (totalWrittenInCurrentWriter > maxSSTableSize) {
            totalWrittenInLevel += totalWrittenInCurrentWriter;
            sstableWriter.currentWriter().last = partition.partitionKey();
            logger.debug("rymDebug: MajorLeveledCompactionWriter first key is {}, last key is {}",
                         sstableWriter.currentWriter().first, sstableWriter.currentWriter().last);
            if (totalWrittenInLevel > LeveledManifest.maxBytesForLevel(currentLevel, levelFanoutSize, maxSSTableSize)) {
                totalWrittenInLevel = 0;
                // logger.debug("[Tinoryj] current total written in level: {}, current level = {}", totalWrittenInLevel, currentLevel);
                currentLevel++;
            }
            switchCompactionLocation(sstableDirectory);
        }
        return rie != null;

    }

    // [CASSANDRAEC]
    public boolean realAppend(UnfilteredRowIterator partition, boolean isSwitchWriter) {

        if (isSwitchWriter) {
            totalWrittenInLevel += sstableWriter.currentWriter().getEstimatedOnDiskBytesWritten();
            // if (totalWrittenInLevel > LeveledManifest.maxBytesForLevel(currentLevel, levelFanoutSize, maxSSTableSize)) {
            //     totalWrittenInLevel = 0;
            //     // logger.debug("[Tinoryj] current total written in level: {}, current level = {}", totalWrittenInLevel, currentLevel);
            //     currentLevel++;
            // }
            switchCompactionLocation(sstableDirectory);
        }

        RowIndexEntry rie = sstableWriter.append(partition);
        partitionsWritten++;
        long totalWrittenInCurrentWriter = sstableWriter.currentWriter().getEstimatedOnDiskBytesWritten();
        if (totalWrittenInCurrentWriter > maxSSTableSize) {
            totalWrittenInLevel += totalWrittenInCurrentWriter;
            if (totalWrittenInLevel > LeveledManifest.maxBytesForLevel(currentLevel, levelFanoutSize, maxSSTableSize)) {
                totalWrittenInLevel = 0;
                // logger.debug("[Tinoryj] current total written in level: {}, current level = {}", totalWrittenInLevel, currentLevel);
                currentLevel++;
            }
            switchCompactionLocation(sstableDirectory);
        }
        return rie != null;

    }

    @Override
    public void switchCompactionLocation(Directories.DataDirectory location) {
        this.sstableDirectory = location;
        averageEstimatedKeysPerSSTable = Math
                .round(((double) averageEstimatedKeysPerSSTable * sstablesWritten + partitionsWritten)
                        / (sstablesWritten + 1));
        sstableWriter.switchWriter(
                SSTableWriter.create(cfs.newSSTableDescriptor(getDirectories().getLocationForDisk(sstableDirectory)),
                        keysPerSSTable,
                        minRepairedAt,
                        pendingRepair,
                        isTransient,
                        cfs.metadata,
                        new MetadataCollector(txn.originals(), cfs.metadata().comparator, currentLevel),
                        SerializationHeader.make(cfs.metadata(), txn.originals()),
                        cfs.indexManager.listIndexes(),
                        txn));
        partitionsWritten = 0;
        sstablesWritten = 0;
    }

    @Override
    protected long getExpectedWriteSize() {
        return Math.min(maxSSTableSize, super.getExpectedWriteSize());
    }
}
