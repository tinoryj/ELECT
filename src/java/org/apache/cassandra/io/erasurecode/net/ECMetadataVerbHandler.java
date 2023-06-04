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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;


import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.CompactionManager.AllSSTableOpStatus;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.io.erasurecode.net.ECNetutils.SSTableReaderComparator;
import org.apache.cassandra.io.erasurecode.net.ECSyncSSTableVerbHandler.DataForRewrite;
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
    // private static final String ecMetadataDir = System.getProperty("user.dir") +
    // "/data/ECMetadata/";
    public static List<ECMetadata> ecMetadatas = new ArrayList<ECMetadata>();

    private static final Logger logger = LoggerFactory.getLogger(ECMetadataVerbHandler.class);
    
    private volatile static boolean isConsumeBlockedECMetadataOccupied = false;

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

        InetAddressAndPort sourceIP = message.from();
        InetAddressAndPort localIP = FBUtilities.getBroadcastAddressAndPort();
        // receive metadata and record it to files (append)
        // ecMetadatas.add(message.payload);
        // logger.debug("rymDebug: received metadata: {}, {},{},{}", message.payload,
        // message.payload.sstHashIdList, message.payload.primaryNodes,
        // message.payload.relatedNodes);
        ECMetadata ecMetadata = message.payload;

        Map<String, List<InetAddressAndPort>> sstHashIdToReplicaMap = ecMetadata.ecMetadataContent.sstHashIdToReplicaMap;
        String updateNewSSTHash = ecMetadata.ecMetadataContent.sstHashIdList.get(ecMetadata.ecMetadataContent.targetIndex);

        // logger.debug("rymDebug: got sstHashIdToReplicaMap: {} ",
        // sstHashIdToReplicaMap);
        for (Map.Entry<String, List<InetAddressAndPort>> entry : sstHashIdToReplicaMap.entrySet()) {
            String newSSTableHash = entry.getKey();
            if (!localIP.equals(entry.getValue().get(0)) && entry.getValue().contains(localIP)) {
                int index = entry.getValue().indexOf(localIP);
                String cfName = ecMetadata.ecMetadataContent.cfName + index;
                logger.debug("rymDebug: [Parity Update: {}, Save it for {}] ECMetadataVerbHandler get sstHash {} from {}, the replica nodes are {}, strip id is {}",
                             ecMetadata.ecMetadataContent.isParityUpdate, cfName, newSSTableHash, sourceIP, entry.getValue(), ecMetadata.stripeId);

                // transformECMetadataToECSSTable(ecMetadata, ks, cfName, sstableHash, sourceIP);

                BlockedECMetadata blockedECMetadata = new BlockedECMetadata(newSSTableHash, sourceIP, ecMetadata, cfName);
                if(!entry.getKey().equals(updateNewSSTHash)) {
                    blockedECMetadata.ecMetadata.ecMetadataContent.oldSSTHash = entry.getKey();
                }

                // Check if the old sstable is available, if not, add it to the queue
                
                saveECMetadataToBlockList(blockedECMetadata, blockedECMetadata.ecMetadata.ecMetadataContent.oldSSTHash,
                                          (StorageService.instance.globalSSTHashToECSSTable.get(blockedECMetadata.ecMetadata.ecMetadataContent.oldSSTHash) != null));

            } else {
                logger.debug("rymDebug: [Drop it] ECMetadataVerbHandler get sstHash {} from {}, the replica nodes are {}, strip id is {}",
                             newSSTableHash, sourceIP, entry.getValue(), ecMetadata.stripeId);
            }

        }

    }


    public static Runnable getConsumeBlockedECMetadataRunnable() {
        // Consume the blocked ecMetadata if needed
        logger.debug("rymDebug: This is getConsumeBlockedECMetadataRunnable, the globalBlockedECMetadata is {}, isConsumeBlockedECMetadataOccupied is ({})",
                     StorageService.instance.globalBlockedECMetadata.size(),
                     isConsumeBlockedECMetadataOccupied);
        
        return new ConsumeBlockedECMetadataRunnable();
    }


    /**
     * To avoid concurrency conflicts, we store all received ECMetadata in Map <globalBlockedECMetadata>.
     * And we set up a periodically task to consume ECMetadata, that is transforming the ECMetadata into a ecSSTable.
     * Note that the ECMetadata could be generated in two cases:
     *  1. During the first time generating erasure coding;
     *  2. Parity update.
     */
    private static class ConsumeBlockedECMetadataRunnable implements Runnable {

        private final int MAX_RETRY_COUNT = 5;

        @Override
        public void run() {
            if(StorageService.instance.globalBlockedECMetadata.isEmpty() || isConsumeBlockedECMetadataOccupied){
                return;
            }

            isConsumeBlockedECMetadataOccupied = true;
            logger.debug("rymDebug: This is ConsumeBlockedECMetadataRunnable");
            for (Map.Entry<String, ConcurrentLinkedQueue<BlockedECMetadata>> entry : StorageService.instance.globalBlockedECMetadata.entrySet()) {
                String ks = "ycsb";
                String cfName = entry.getKey();

                for (BlockedECMetadata metadata : entry.getValue()) {
                    if (!transformECMetadataToECSSTable(metadata.ecMetadata, ks, cfName, metadata.newSSTableHash,
                            metadata.sourceIP)) {
                        logger.debug("rymDebug: Perform transformECMetadataToECSSTable successfully");
                        entry.getValue().remove(metadata);
                    } else if (metadata.retryCount < MAX_RETRY_COUNT) {
                        metadata.retryCount++;
                        continue;
                    } else {
                        logger.debug("rymDebug: Still cannot create transactions, but we won't try it again, write the data down immediately.");
                        ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(cfName);

                        // If the ecMetadata is for erasure coding, just transform it
                        if (!metadata.ecMetadata.ecMetadataContent.isParityUpdate) {
                            DataForRewrite dataForRewrite = StorageService.instance.globalSSTMap
                                    .get(metadata.newSSTableHash);
                            if (dataForRewrite != null) {
                                String fileNamePrefix = dataForRewrite.fileNamePrefix;
                                transformECMetadataToECSSTableForErasureCode(metadata.ecMetadata,
                                        new ArrayList<SSTableReader>(),
                                        cfs,
                                        fileNamePrefix,
                                        metadata.newSSTableHash,
                                        null, null, null);
                                entry.getValue().remove(metadata);
                            } else {
                                logger.warn("rymDebug: cannot get rewrite data of {} during redo transformECMetadataToECSSTable",
                                            metadata.newSSTableHash);
                            }
                        } else {
                            // TODO: Wait until the target ecSSTable is released
                            // transformECMetadataToECSSTableForParityUpdate(metadata.ecMetadata, cfs, metadata.sstableHash);
                            logger.debug("rymERROR: wait until the target ecSSTable is released");
                        }

                    }

                    // entry.getValue().remove(metadata);
                }

            }
            isConsumeBlockedECMetadataOccupied = false;

        }

    }

    /**
     * 
     * @param ecMetadata
     * @param ks
     * @param cfName
     * @param sstableHash
     * @param sourceIP
     * @return Is failed to create the transaction?
     * Add a concurrent lock here
     */
    private static boolean transformECMetadataToECSSTable(ECMetadata ecMetadata, String ks, String cfName, String newSSTHash, InetAddressAndPort sourceIP) {

        ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(cfName);
        // get the dedicated level of sstables
        if (!ecMetadata.ecMetadataContent.isParityUpdate) {
            // [In progress of erasure coding]
            DataForRewrite dataForRewrite = StorageService.instance.globalSSTMap.get(newSSTHash);

            if (dataForRewrite != null) {
    
                String fileNamePrefix = dataForRewrite.fileNamePrefix;
                List<SSTableReader> sstables = new ArrayList<>(
                        cfs.getSSTableForLevel(DatabaseDescriptor.getCompactionThreshold()));
                if (!sstables.isEmpty()) {
                    Collections.sort(sstables, new SSTableReaderComparator());
                    List<SSTableReader> rewriteSStables = new ArrayList<SSTableReader>();
                    DecoratedKey firstKeyForRewrite = dataForRewrite.firstKey;
                    DecoratedKey lastKeyForRewrite = dataForRewrite.lastKey;
                    // use binary search to find related sstables
                    rewriteSStables = getRewriteSSTables(sstables, firstKeyForRewrite, lastKeyForRewrite);
                    // logger.debug("rymDebug: read sstable from ECMetadata, sstable name is {}", ecSSTable.getFilename());
    
                    return transformECMetadataToECSSTableForErasureCode(ecMetadata, rewriteSStables, cfs, fileNamePrefix,
                                                                        newSSTHash, sourceIP, firstKeyForRewrite, lastKeyForRewrite);
                } else {
                    logger.info("rymDebug: cannot replace the existing sstables yet, as {} is lower than {}",
                                cfs.getColumnFamilyName(), DatabaseDescriptor.getCompactionThreshold());
                }
            } else {
                logger.warn("rymERROR: cannot get rewrite data of {} during erasure coding, message is from {}, target cfs is {}", newSSTHash, sourceIP, cfName);
            }
            return false;


            

        } else {
            // if(ecMetadata.ecMetadataContent.sstHashIdList.indexOf(newSSTHash) == ecMetadata.ecMetadataContent.targetIndex)
            return transformECMetadataToECSSTableForParityUpdate(ecMetadata, cfs, newSSTHash);
            // return false;
        }

    }

    private static boolean transformECMetadataToECSSTableForErasureCode(ECMetadata ecMetadata, List<SSTableReader> rewriteSStables,
                                                                        ColumnFamilyStore cfs, String fileNamePrefix,
                                                                        String newSSTHash, InetAddressAndPort sourceIP,
                                                                        DecoratedKey firstKeyForRewrite, DecoratedKey lastKeyForRewrite) {

        final LifecycleTransaction updateTxn = cfs.getTracker().tryModify(rewriteSStables, OperationType.COMPACTION);

        // M is the sstable from primary node, M` is the corresponding sstable of
        // secondary node
        if (rewriteSStables.isEmpty()) {
            logger.warn("rymWarn: rewriteSStables is empty, just record it!");
            cfs.replaceSSTable(ecMetadata, newSSTHash, cfs, fileNamePrefix, updateTxn);
            StorageService.instance.globalSSTMap.remove(newSSTHash);
            return false;
        }

        if (updateTxn != null) {

            if (rewriteSStables.size() == 1) {
                logger.debug("rymDebug: Anyway, we just replace the sstables");
                cfs.replaceSSTable(ecMetadata, newSSTHash, cfs, fileNamePrefix, updateTxn);
            } else if (rewriteSStables.size() > 1) {
                logger.debug("rymDebug: many sstables are involved, {} sstables need to rewrite!", rewriteSStables.size());
                // logger.debug("rymDebug: rewrite sstable {} Data.db with EC.db",
                // ecSSTable.descriptor);
                try {
                    AllSSTableOpStatus status = cfs.sstablesRewrite(firstKeyForRewrite,
                            lastKeyForRewrite,
                            rewriteSStables, ecMetadata, fileNamePrefix, updateTxn, false,
                            Long.MAX_VALUE, false, 1);
                    if (status != AllSSTableOpStatus.SUCCESSFUL)
                        ECNetutils.printStatusCode(status.statusCode, cfs.name);
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


            StorageService.instance.globalSSTMap.remove(newSSTHash);
        } else {
            // Save ECMetadata and redo ec transition later
            logger.debug("rymDebug: [ErasureCoding] failed to get transactions for the sstables ({}), we will try it later", newSSTHash);
            // BlockedECMetadata blockedECMetadata = new BlockedECMetadata(sstableHash, sourceIP, ecMetadata);
            // saveECMetadataToBlockList(cfs.getColumnFamilyName(), blockedECMetadata);
            return true;
        }

        return false;

    }


    private static boolean transformECMetadataToECSSTableForParityUpdate(ECMetadata ecMetadata, ColumnFamilyStore cfs, String newSSTHash) {
        // [In progress of parity update], update the related sstables, there are two
        // cases:
        // 1. For the parity update sstable, replace the ECMetadata
        // 2. For the non-updated sstable, just replace the files
        // String currentSSTHash = entry.getKey();
        int sstIndex = ecMetadata.ecMetadataContent.sstHashIdList.indexOf(newSSTHash);
        // need a old sstHash
        logger.debug("rymDebug: [Parity Update] we are going to update the old sstable ({}) with a new one ({}) for strip id ({}) in ({})",
                     ecMetadata.ecMetadataContent.oldSSTHash, newSSTHash, ecMetadata.stripeId, cfs.getColumnFamilyName());
        SSTableReader oldECSSTable = StorageService.instance.globalSSTHashToECSSTable.get(ecMetadata.ecMetadataContent.oldSSTHash);

        if(oldECSSTable != null) {
            if (sstIndex == ecMetadata.ecMetadataContent.targetIndex) {
                // replace ec sstable

                DataForRewrite dataForRewrite = StorageService.instance.globalSSTMap.get(newSSTHash);
                if (dataForRewrite != null) {

                    String fileNamePrefix = dataForRewrite.fileNamePrefix;
                    final LifecycleTransaction updateTxn = cfs.getTracker()
                            .tryModify(Collections.singletonList(oldECSSTable), OperationType.COMPACTION);
                    if (updateTxn != null) {
                        cfs.replaceSSTable(ecMetadata, newSSTHash, cfs, fileNamePrefix, updateTxn);

                        return false;
                    } else {
                        logger.debug("rymDebug:[Parity Update] failed to get transactions for the sstables ({}), we will try it later",
                                     newSSTHash);
                        return true;
                    }
                } else {
                    logger.warn("rymERROR:[Parity Update] cannot get rewrite data of {} during parity update",
                                newSSTHash);
                }

            } else {
                // Just replace the files
                SSTableReader.loadECMetadata(ecMetadata, oldECSSTable.descriptor);
            }
        } else {
            ECNetutils.printStackTace(String.format("rymERROR: [Parity Update] cannot get ecSSTable for sstHash(%s)", ecMetadata.ecMetadataContent.oldSSTHash));
        }
        return false;
    }


    public static void checkTheBlockedUpdateECMetadata(SSTableReader oldECSSTable) {
        if(StorageService.instance.globalBlockedUpdatedECMetadata.containsKey(oldECSSTable.getSSTableHashID()) && 
        !StorageService.instance.globalBlockedUpdatedECMetadata.get(oldECSSTable.getSSTableHashID()).isEmpty() ) {
         if(StorageService.instance.globalBlockedUpdatedECMetadata.get(oldECSSTable.getSSTableHashID()).size()>1) {
             logger.debug("rymDebug: the size of globalBlockedUpdatedECMetadata for sstHash ({}) is ({})",
                           oldECSSTable.getSSTableHashID(),
                           StorageService.instance.globalBlockedUpdatedECMetadata.get(oldECSSTable.getSSTableHashID()).size());
         }
         while(!StorageService.instance.globalBlockedUpdatedECMetadata.get(oldECSSTable.getSSTableHashID()).isEmpty()) {
             BlockedECMetadata blockedECMetadata = StorageService.instance.globalBlockedUpdatedECMetadata.get(oldECSSTable.getSSTableHashID()).poll();
             saveECMetadataToBlockList(blockedECMetadata, null, true);
         }
     }
    }



    public static class BlockedECMetadata {
        public final String newSSTableHash;
        public final InetAddressAndPort sourceIP;
        public final String cfName;
        public ECMetadata ecMetadata;
        public int retryCount = 0;
        public BlockedECMetadata(String newSSTableHash, InetAddressAndPort sourceIP, ECMetadata ecMetadata, String cfName) {
            this.newSSTableHash = newSSTableHash;
            this.sourceIP = sourceIP;
            this.ecMetadata = ecMetadata;
            this.cfName = cfName;
        }
    }

    private static void saveECMetadataToBlockList(BlockedECMetadata metadata, String oldSSTHash, boolean isECSSTableAvailable) {

        if(isECSSTableAvailable) {
            if(StorageService.instance.globalBlockedECMetadata.containsKey(metadata.cfName)) {
                if(!StorageService.instance.globalBlockedECMetadata.get(metadata.cfName).contains(metadata))
                    StorageService.instance.globalBlockedECMetadata.get(metadata.cfName).add(metadata);
            } else {
                ConcurrentLinkedQueue<BlockedECMetadata> blockList = new ConcurrentLinkedQueue<BlockedECMetadata>();
                blockList.add(metadata);
                StorageService.instance.globalBlockedECMetadata.put(metadata.cfName, blockList);
            }
        } else {
            if(StorageService.instance.globalBlockedUpdatedECMetadata.containsKey(oldSSTHash)) {
                if(!StorageService.instance.globalBlockedUpdatedECMetadata.get(oldSSTHash).contains(metadata))
                    StorageService.instance.globalBlockedUpdatedECMetadata.get(oldSSTHash).add(metadata);
            } else {
                ConcurrentLinkedQueue<BlockedECMetadata> blockList = new ConcurrentLinkedQueue<BlockedECMetadata>();
                blockList.add(metadata);
                StorageService.instance.globalBlockedUpdatedECMetadata.put(oldSSTHash, blockList);
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

    private static List<SSTableReader> getRewriteSSTables(List<SSTableReader> sstables, DecoratedKey first,
            DecoratedKey last) {
        List<SSTableReader> rewriteSStables = new ArrayList<SSTableReader>();
        // TODO: add head and tail
        // first search which sstable does the first key stored
        int left = 0;
        int right = sstables.size() - 1;
        int mid = 0;
        while (left <= right) {
            mid = (left + right) / 2;
            SSTableReader sstable = sstables.get(mid);
            if (sstable.first.compareTo(first) <= 0 &&
                    sstable.last.compareTo(first) >= 0) {
                // rewriteSStables.add(sstable);
                break;
            } else if (sstable.first.compareTo(first) < 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        // then search which sstable does the last key stored
        int tail = mid + 1;
        while (tail < sstables.size() && last.compareTo(sstables.get(tail).first) >= 0) {
            if (sstables.get(tail).getSSTableLevel() != DatabaseDescriptor.getCompactionThreshold())
                logger.warn("rymWarnings: sstable level {} is not equal to threshold {}",
                        sstables.get(tail).getSSTableLevel(), DatabaseDescriptor.getCompactionThreshold());
            if (!sstables.get(tail).isReplicationTransferredToErasureCoding())
                rewriteSStables.add(sstables.get(tail));
            tail++;
        }

        int head = mid - 1;
        if (head >= 0 && (rewriteSStables.size() > 1 || first.compareTo(sstables.get(head).last) <= 0)) {
            if (sstables.get(head).getSSTableLevel() != DatabaseDescriptor.getCompactionThreshold())
                logger.warn("rymWarnings: sstable level {} is not equal to threshold {}",
                        sstables.get(head).getSSTableLevel(), DatabaseDescriptor.getCompactionThreshold());
            if (!sstables.get(head).isReplicationTransferredToErasureCoding())
                rewriteSStables.add(sstables.get(head));
            head--;
        }

        if (head >= 0 && !sstables.get(head).isReplicationTransferredToErasureCoding())
            rewriteSStables.add(sstables.get(head));
        if (tail < sstables.size() && !sstables.get(tail).isReplicationTransferredToErasureCoding())
            rewriteSStables.add(sstables.get(tail));

        return rewriteSStables;
    }


}
