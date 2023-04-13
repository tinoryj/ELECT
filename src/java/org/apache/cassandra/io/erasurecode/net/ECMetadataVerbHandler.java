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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
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
                List<SSTableReader> sstables = new ArrayList<>(cfs.getSSTableForLevel(DatabaseDescriptor.getCompactionThreshold()));
                Collections.sort(sstables, new SSTableReaderComparator());

                // rewrite the most similar sstables
                List<SSTableReader> rewriteSStables = new ArrayList<SSTableReader>();

                // use binary search to find related sstables
                int keyNums = StorageService.instance.globalSSTMap.get(sstableHash).size();
                DecoratedKey first = StorageService.instance.globalSSTMap.get(sstableHash).get(0);
                DecoratedKey last = StorageService.instance.globalSSTMap.get(sstableHash).get(keyNums - 1);
                rewriteSStables = getRewriteSSTables(sstables, first, last);

                // rewrite and update the sstables
                // M is the sstable from primary node, M` is the corresponding sstable of secondary node
                if(rewriteSStables.size()==1) {
                    List<DecoratedKey> allKeys = rewriteSStables.get(0).getAllKeys();

                    if(rewriteSStables.get(0).getSSTableHashID().equals(sstableHash)) {
                        // delete sstable if sstable Hash can be found 
                        rewriteSStables.get(0).replaceDatabyECMetadata(message.payload);
                    } else {
                        // a bit different, update sstable and delete it
                        if(allKeys.size() >= keyNums) {
                            // M missed some keys,
                            // suppose that the compaction speed of primary tree is faster than that of
                            // secondary, so the these missed keys should be deleted
                            rewriteSStables.get(0).replaceDatabyECMetadata(message.payload);
                        } else {
                            // M` missed some keys,
                            // need to update the metadata
                            
                        }
                        


                        

                    }
                } else {
                    // many sstables are involved


                }
                

                StorageService.instance.globalSSTMap.remove(sstableHash);

            }

        }

    }

    private static List<SSTableReader> getRewriteSSTables(List<SSTableReader> sstables, DecoratedKey first, DecoratedKey last) {
        List<SSTableReader> rewriteSStables = new ArrayList<SSTableReader>();
        // first search which sstable does the first key stored
        int left = 0;
        int right = sstables.size() - 1;
        int mid = 0;
        while(left <= right) {
            mid = (left + right) / 2;
            SSTableReader sstable = sstables.get(mid);
            if(sstable.first.compareTo(first)<=0 && 
               sstable.last.compareTo(first)>=0) {
                rewriteSStables.add(sstable);
                break;
            }
            else if (sstable.first.compareTo(first)<0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        // then search which sstable does the last key stored
        mid++;
        while (mid<sstables.size() && last.compareTo(sstables.get(mid).last)>0) {
            rewriteSStables.add(sstables.get(mid));
        }
        return rewriteSStables;
    }

    private class SSTableReaderComparator implements Comparator<SSTableReader> {

        @Override
        public int compare(SSTableReader o1, SSTableReader o2) {
            return o1.first.getToken().compareTo(o2.first.getToken());
        }
        
    }
}
