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
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionManager.AllSSTableOpStatus;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECMetadataVerbHandler implements IVerbHandler<ECMetadata> {
    public static final ECMetadataVerbHandler instance = new ECMetadataVerbHandler();
    // private static final String ecMetadataDir = System.getProperty("user.dir") + "/data/ECMetadata/";
    public static List<ECMetadata> ecMetadatas = new ArrayList<ECMetadata>();

    private static final Logger logger = LoggerFactory.getLogger(ECMetadataVerbHandler.class);


    // Reset
    public static final String RESET = "\033[0m"; // Text Reset

    // Regular Colors
    public static final String WHITE = "\033[0;30m"; // WHITE
    public static final String RED = "\033[0;31m"; // RED
    public static final String GREEN = "\033[0;32m"; // GREEN
    public static final String YELLOW = "\033[0;33m"; // YELLOW
    public static final String BLUE = "\033[0;34m"; // BLUE
    public static final String PURPLE = "\033[0;35m"; // PURPLE
    public static final String CYAN = "\033[0;36m"; // CYAN
    public static final String GREY = "\033[0;37m"; // GREY


    @Override
    public void doVerb(Message<ECMetadata> message) throws IOException {
        // Check if there were any forwarding headers in this message
        ForwardingInfo forwardTo = message.forwardTo();
        if (forwardTo != null) {
            forwardToLocalNodes(message, forwardTo);
            logger.debug("rymDebug: this is a forwarding header");
        }

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
                int index = entry.getValue().indexOf(localIP);
                String cfName = message.payload.cfName + index;
                
                ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(cfName);

                // get the dedicated level of sstables
                List<SSTableReader> sstables = new ArrayList<>(cfs.getSSTableForLevel(DatabaseDescriptor.getCompactionThreshold()));
                if (!sstables.isEmpty()) {
                    Collections.sort(sstables, new SSTableReaderComparator());

                    // rewrite the most similar sstables
                    List<SSTableReader> rewriteSStables = new ArrayList<SSTableReader>();

                    // use binary search to find related sstables
                    List<DecoratedKey> updateKeys = StorageService.instance.globalSSTMap.get(sstableHash);
                    DecoratedKey first = updateKeys.get(0);
                    DecoratedKey last = updateKeys.get(updateKeys.size() - 1);
                    rewriteSStables = getRewriteSSTables(sstables, first, last);

                    // rewrite and update the sstables
                    // M is the sstable from primary node, M` is the corresponding sstable of
                    // secondary node
                    // one to one
                    if (rewriteSStables.size() == 1) {
                        List<DecoratedKey> allKeys = rewriteSStables.get(0).getAllDecoratedKeys();

                        if (rewriteSStables.get(0).getSSTableHashID().equals(sstableHash)) {
                            // delete sstable if sstable Hash can be found
                            rewriteSStables.get(0).replaceDatabyECMetadata(message.payload);
                            logger.debug(RED + "rymDebug: get the match sstbales, delete it!");
                        } else {
                            // a bit different, update sstable and delete it
                            if (first.equals(allKeys.get(0)) && last.equals(allKeys.get(allKeys.size() - 1))
                                    && allKeys.size() < updateKeys.size()) {
                                // M` missed some keys in the middle, update metadata and delete M`,
                                // suppose that the compaction speed of primary tree is faster than that of
                                // secondary, so the these missed keys should be deleted
                                // TODO: need to consider rewrite sstables.
                                // rewriteSStables.get(0).updateBloomFilter(cfs, updateKeys);
                                rewriteSStables.get(0).replaceDatabyECMetadata(message.payload);
                                logger.debug(RED + "rymDebug: M` missed keys in the middle, update and delete it!");

                            } else if (first.equals(allKeys.get(0)) && last.equals(allKeys.get(allKeys.size() - 1))
                                    && allKeys.size() >= updateKeys.size()) {
                                // M missed some keys in the middle, just delete it
                                // IMPORTANT NOTE: we set the latency is as long as possible, so we can assume
                                // that the compaction speed of secondary node is slower than primary node
                                rewriteSStables.get(0).replaceDatabyECMetadata(message.payload);
                                logger.debug(RED + "rymDebug: M missed keys in the middle, delete it!");
                            } else {
                                // M` missed some keys in the boundary,
                                // need to update the metadata
                                logger.debug(RED + "rymDebug: M` missed keys in the edge, update and delete it!");
                                rewriteSStables.get(0).replaceDatabyECMetadata(message.payload);
                                // TODO: write update statsmetadata and bloomfilter 
                                // rewriteSStables.get(0).updateBloomFilter(cfs, updateKeys);
                            }

                        }
                    } else {
                        // many sstables are involved
                        // delete the sstables and update the metadata
                        // rewrite the sstables, just delete the key ranges of M` which are matching
                        // those of M

                        logger.debug("rymDebug: many sstbales are involved, {} sstables need to rewrite!",
                                rewriteSStables.size());
                        try {
                            AllSSTableOpStatus status = cfs.sstablesRewrite(updateKeys,
                                    rewriteSStables, false, Long.MAX_VALUE, false, 1);
                            // TODO: write ec_metadata 

                            if (status != AllSSTableOpStatus.SUCCESSFUL)
                                printStatusCode(status.statusCode, cfs.name);
                        } catch (ExecutionException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }

                    StorageService.instance.globalSSTMap.remove(sstableHash);

                } else {
                    logger.info("rymDebug: cannot replace the existing sstables yet, as {} is lower than {}",
                                 cfName, DatabaseDescriptor.getCompactionThreshold());
                }

            }

        }

    }

    private static void forwardToLocalNodes(Message<ECMetadata> originalMessage, ForwardingInfo forwardTo) {
        Message.Builder<ECMetadata> builder = Message.builder(originalMessage)
                .withParam(ParamType.RESPOND_TO, originalMessage.from())
                .withoutParam(ParamType.FORWARD_TO);

        boolean useSameMessageID = forwardTo.useSameMessageID(originalMessage.id());
        // reuse the same Message if all ids are identical (as they will be for 4.0+
        // node originated messages)
        Message<ECMetadata> message = useSameMessageID ? builder.build() : null;

        forwardTo.forEach((id, target) -> {
            Tracing.trace("Enqueuing forwarded write to {}", target);
            MessagingService.instance().send(useSameMessageID ? message : builder.withId(id).build(), target);
        });
    }

    private static void printStatusCode(int statusCode, String cfName) {
        switch (statusCode) {
            case 1:
                logger.debug("Aborted rewrite sstables for at least one table in cfs {}, check server logs for more information.", cfName);
                break;
            case 2:
                logger.error("Failed marking some sstables compacting in cfs {}, check server logs for more information.", cfName);
        }
    }

    private static List<SSTableReader> getRewriteSSTables(List<SSTableReader> sstables, DecoratedKey first, DecoratedKey last) {
        List<SSTableReader> rewriteSStables = new ArrayList<SSTableReader>();
        // TODO: add head and tail 
        // first search which sstable does the first key stored
        int left = 0;
        int right = sstables.size() - 1;
        int mid = 0;
        while(left <= right) {
            mid = (left + right) / 2;
            SSTableReader sstable = sstables.get(mid);
            if(sstable.first.compareTo(first)<=0 && 
               sstable.last.compareTo(first)>=0) {
                // rewriteSStables.add(sstable);
                break;
            }
            else if (sstable.first.compareTo(first)<0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        // then search which sstable does the last key stored
        int tail = mid + 1;
        while (tail < sstables.size() && last.compareTo(sstables.get(tail).first)>=0) {
            rewriteSStables.add(sstables.get(tail));
            tail++;
        }

        int head = mid - 1;
        if(head >= 0 && (rewriteSStables.size() > 1 || first.compareTo(sstables.get(head).last)<=0)) {
            rewriteSStables.add(sstables.get(head));
            head--;
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
