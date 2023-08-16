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
package org.apache.cassandra.tools.nodetool;

import static org.apache.commons.lang3.StringUtils.EMPTY;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "frequency", description = "Get sstale access frequency for every lsm-tree in a specified keyspace")
public class AccessFrequency extends NodeToolCmd
{

    @Arguments(description = "The keyspace name", required = true)
    String keyspace = EMPTY;

    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        out.println("Schema Version:" + probe.getSchemaVersion());

        int level = DatabaseDescriptor.getMaxLevelCount();

        for(ColumnFamilyStore cfs : Keyspace.open(keyspace).getColumnFamilyStores()) {

            int[] sstablesCountEachLevel = new int[level];
            long[] accessFrequencyEachLevel = new long[level];
            long[] min = new long[level];
            for(int i = 0; i < level; i++) {
                min[i] = Long.MAX_VALUE;
            }
            long[] max = new long[level];
            long[] average = new long[level];

            for(SSTableReader sstable : cfs.getTracker().getView().liveSSTables()) {
                sstablesCountEachLevel[sstable.getSSTableLevel()]++;
                accessFrequencyEachLevel[sstable.getSSTableLevel()] += sstable.getReadMeter().count();
                min[sstable.getSSTableLevel()] = min[sstable.getSSTableLevel()] < sstable.getReadMeter().count() ? min[sstable.getSSTableLevel()] : sstable.getReadMeter().count();
                max[sstable.getSSTableLevel()] = max[sstable.getSSTableLevel()] > sstable.getReadMeter().count() ? max[sstable.getSSTableLevel()] : sstable.getReadMeter().count();
            }
            for(int i = 0; i < level; i++) {
                average[i] = accessFrequencyEachLevel[i] / sstablesCountEachLevel[i];
            }

            out.println("SSTable's Access Frequency of Each Level:");
            out.println("\tSSTables in each level: " + sstablesCountEachLevel.toString());
            out.println("\tTotal sstables's access count of each level: " + accessFrequencyEachLevel.toString());
            out.println("\tMinimum sstables's access count of each level: " + min.toString());
            out.println("\tMaximum sstables's access count of each level: " + max.toString());
            out.println("\tAverage sstables's access count of each level: " + average.toString());

        } 
    }
}
