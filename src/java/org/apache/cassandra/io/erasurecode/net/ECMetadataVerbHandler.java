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
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.CompactionManager.AllSSTableOpStatus;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.Tracker;
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
        // ecMetadatas.add(message.payload);
        // logger.debug("rymDebug: received metadata: {}, {},{},{}", message.payload,
        // message.payload.sstHashIdList, message.payload.primaryNodes,
        // message.payload.relatedNodes);

        Map<String, List<InetAddressAndPort>> sstHashIdToReplicaMap = message.payload.ecMetadataContent.sstHashIdToReplicaMap;

        // logger.debug("rymDebug: got sstHashIdToReplicaMap: {} ",
        // sstHashIdToReplicaMap);

        InetAddressAndPort localIP = FBUtilities.getBroadcastAddressAndPort();
        for (Map.Entry<String, List<InetAddressAndPort>> entry : sstHashIdToReplicaMap.entrySet()) {
            String sstableHash = entry.getKey();
            if (!localIP.equals(entry.getValue().get(0)) && entry.getValue().contains(localIP)) {
                String ks = message.payload.ecMetadataContent.keyspace;
                int index = entry.getValue().indexOf(localIP);
                String cfName = message.payload.ecMetadataContent.cfName + index;

                ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(cfName);

                // get the dedicated level of sstables
                if (!message.payload.ecMetadataContent.isParityUpdate) {
                    // [In progress of erasure coding]

                    DataForRewrite dataForRewrite = StorageService.instance.globalSSTMap.get(sstableHash);
                    logger.debug("rymDebug: ECMetadataVerbHandler get sstHash {} from {}",
                            sstableHash, message.from());

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
                            // M is the sstable from primary node, M` is the corresponding sstable of secondary node
                            if (rewriteSStables.isEmpty()) {
                                logger.warn("rymERROR: rewriteSStables is empty!");
                                // TODO: Save the ECMetadata

                                continue;
                            }

                            // TODO: mark this sstable COMPACTION
                            // logger.debug("rymDebug: read sstable from ECMetadata, sstable name is {}",
                            // ecSSTable.getFilename());
                            final LifecycleTransaction updateTxn = cfs.getTracker().tryModify(rewriteSStables,
                                    OperationType.COMPACTION);
                            if (updateTxn != null) {
                                if (rewriteSStables.size() == 1) {
                                    // List<DecoratedKey> allKeys = rewriteSStables.get(0).getAllDecoratedKeys();
                                    DecoratedKey fistKey = rewriteSStables.get(0).first;
                                    DecoratedKey lastKey = rewriteSStables.get(0).last;
                                    // logger.debug("rymDebug: replace sstable {} Data.db with EC.db",
                                    // ecSSTable.descriptor);
                                    if (rewriteSStables.get(0).getSSTableHashID().equals(sstableHash)) {
                                        // delete sstable if sstable Hash can be found
                                        cfs.replaceSSTable(message.payload, cfs, fileNamePrefix, updateTxn);
                                        logger.debug(RED + "rymDebug: get the match sstbales, delete it!");
                                    } else if (firstKeyForRewrite.equals(fistKey)
                                            && lastKeyForRewrite.equals(lastKey)) {
                                        // Case1: M missed some keys in the middle, just delete M`
                                        // IMPORTANT NOTE: we set the latency is as long as possible, so we can assume
                                        // that the compaction speed of secondary node is slower than primary node
                                        // Case2: M` missed some keys in the middle, need to update the metadata
                                        cfs.replaceSSTable(message.payload, cfs, fileNamePrefix, updateTxn);
                                        logger.debug(RED
                                                + "rymDebug: M or M1 missed some keys in the middle, update sstable!");
                                    } else {
                                        cfs.replaceSSTable(message.payload, cfs, fileNamePrefix, updateTxn);
                                        logger.warn("rymWarning: get unexpected sstables num");
                                    }
                                } else if (rewriteSStables.size() > 1) {
                                    // many sstables are involved
                                    // delete the sstables and update the metadata
                                    // rewrite the sstables, just delete the key ranges of M` which are matching
                                    // those of M

                                    logger.debug("rymDebug: many sstables are involved, {} sstables need to rewrite!",
                                            rewriteSStables.size());
                                    // logger.debug("rymDebug: rewrite sstable {} Data.db with EC.db",
                                    // ecSSTable.descriptor);
                                    try {

                                        AllSSTableOpStatus status = cfs.sstablesRewrite(firstKeyForRewrite,
                                                lastKeyForRewrite,
                                                rewriteSStables, message.payload, fileNamePrefix, updateTxn, false,
                                                Long.MAX_VALUE, false, 1);
                                        if (status != AllSSTableOpStatus.SUCCESSFUL)
                                            printStatusCode(status.statusCode, cfs.name);
                                    } catch (ExecutionException e) {
                                        e.printStackTrace();
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                }
                                StorageService.instance.globalSSTMap.remove(sstableHash);
                            } else {
                                // TODO: Save ECMetadata and redo ec transition later
                                logger.debug("rymDebug: failed to get transactions for the sstables, we will try it later");
                            }
                        } else {
                            logger.info("rymDebug: cannot replace the existing sstables yet, as {} is lower than {}",
                                    cfName, DatabaseDescriptor.getCompactionThreshold());
                        }
                    } else {
                        logger.warn("rymERROR: cannot get rewrite data of {} during erasure coding", sstableHash);
                    }

                } else {
                    // [In progress of parity update], update the related sstables, there are two
                    // cases:
                    // 1. For the parity update sstable, replace the ECMetadata
                    // 2. For the non-updated sstable, just replace the files
                    String currentSSTHash = entry.getKey();
                    int sstIndex = message.payload.ecMetadataContent.sstHashIdList.indexOf(currentSSTHash);
                    SSTableReader oldECSSTable = StorageService.instance.globalSSTHashToECSSTable.get(currentSSTHash);
                    if (sstIndex == message.payload.ecMetadataContent.targetIndex) {
                        // replace ec sstable

                        DataForRewrite dataForRewrite = StorageService.instance.globalSSTMap.get(sstableHash);
                        if (dataForRewrite != null) {

                            String fileNamePrefix = dataForRewrite.fileNamePrefix;
                            final LifecycleTransaction updateTxn = cfs.getTracker()
                                    .tryModify(Collections.singletonList(oldECSSTable), OperationType.COMPACTION);
                            if (updateTxn != null) {
                                cfs.replaceSSTable(message.payload, cfs, fileNamePrefix, updateTxn);
                            } else {
                                logger.debug("rymERROR: failed to get transactions for the sstables, we will try it later");
                            }
                        } else {
                            logger.warn("rymERROR: cannot get rewrite data of {} during parity update", sstableHash);
                        }

                    } else {
                        // Just replace the files
                        SSTableReader.loadECMetadata(message.payload, oldECSSTable.descriptor);
                    }

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
                logger.debug(
                        "Aborted rewrite sstables for at least one table in cfs {}, check server logs for more information.",
                        cfName);
                break;
            case 2:
                logger.error(
                        "Failed marking some sstables compacting in cfs {}, check server logs for more information.",
                        cfName);
        }
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

    private class SSTableReaderComparator implements Comparator<SSTableReader> {

        @Override
        public int compare(SSTableReader o1, SSTableReader o2) {
            return o1.first.getToken().compareTo(o2.first.getToken());
        }

    }
}
