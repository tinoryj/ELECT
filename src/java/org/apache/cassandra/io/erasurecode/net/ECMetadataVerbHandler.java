/**
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

package org.apache.cassandra.io.erasurecode.net;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECMetadataVerbHandler implements IVerbHandler<ECMetadata> {
    public static final ECMetadataVerbHandler instance = new ECMetadataVerbHandler();
    // private static final String ecMetadataDir = System.getProperty("user.dir") + "/data/ECMetadata/";
    public static List<ECMetadata> ecMetadatas = new ArrayList<ECMetadata>();

    private static final Logger logger = LoggerFactory.getLogger(ECMetadataVerbHandler.class);

    @Override
    public void doVerb(Message<ECMetadata> message) throws IOException {
        // receive metadata and record it to files (append)
        ecMetadatas.add(message.payload);
        logger.debug("rymDebug: received metadata: {}, {},{},{}", message.payload,
                message.payload.sstHashIdList, message.payload.primaryNodes, message.payload.relatedNodes);

        Map<String, List<InetAddressAndPort>> sstHashIdToReplicaMap = message.payload.sstHashIdToReplicaMap;

        logger.debug("rymDebug: got sstHashIdToReplicaMap: {} ", sstHashIdToReplicaMap);

        InetAddressAndPort localIP = FBUtilities.getBroadcastAddressAndPort();
        for (Map.Entry<String, List<InetAddressAndPort>> entry : sstHashIdToReplicaMap.entrySet()) {
            String sstableHash = entry.getKey();
            if (!localIP.equals(entry.getValue().get(0)) && entry.getValue().contains(localIP)) {
                String ks = message.payload.keyspace;
                String cfName = message.payload.cfName;
                int index = entry.getValue().indexOf(localIP);
                
                ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(cfName+index);

                // get the dedicated level of sstables
                Set<SSTableReader> sstables = cfs.getSSTableForLevel(DatabaseDescriptor.getCompactionThreshold());

                // traverse the sstables and find the most similar node to do transition
                boolean flag = false;
                for (SSTableReader sstable : sstables) {
                    if(sstable.getSSTableHashID().equals(sstableHash)) {
                        // delete sstable if sstableHash can be found
                        sstable.replaceDatabyECMetadata(message.payload);
                        flag = true;
                    }
                }

                if (!flag) {
                    // rewrite the most similar sstables
                    // use binary search to find related sstables

                    // convert the sstable contents to Iterable<UnfilteredRowIterator>
                    

                    // first search which sstable does the first key stored
                    DecoratedKey first = null;
                    DecoratedKey last = null;
                    

                    // then search which sstable does the last key stored


                    
                    
                }

                StorageService.instance.globalSSTMap.remove(sstableHash);

            }

        }

    }
}
