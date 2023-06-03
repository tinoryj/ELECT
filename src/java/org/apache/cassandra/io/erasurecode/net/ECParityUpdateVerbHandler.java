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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type.Collection;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.erasurecode.ErasureCoderOptions;
import org.apache.cassandra.io.erasurecode.ErasureEncoder;
import org.apache.cassandra.io.erasurecode.NativeRSEncoder;
import org.apache.cassandra.io.erasurecode.net.ECMetadata.ECMetadataContent;
import org.apache.cassandra.io.erasurecode.net.ECParityUpdate.SSTableContentWithHashID;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ForwardingInfo;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.shaded.netty.buffer.ByteBuf;

import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

public class ECParityUpdateVerbHandler implements IVerbHandler<ECParityUpdate> {
    public static final ECParityUpdateVerbHandler instance = new ECParityUpdateVerbHandler();
    private static final Logger logger = LoggerFactory.getLogger(ECParityUpdateVerbHandler.class);
    private static List<InetAddressAndPort> parityNodes = new ArrayList<InetAddressAndPort>();


    /**
     * Receives update data from the primary node, and performs the following steps:
     * 1. First consume the new data list, perform parity update
     * 2. If new data is empty while the old data is not empty, add old data to the global queue
     */
    @Override
    public void doVerb(Message<ECParityUpdate> message) throws IOException {
        ForwardingInfo forwardTo = message.forwardTo();
        if (forwardTo != null) {
            forwardToLocalNodes(message, forwardTo);
            logger.debug("rymDebug: this is a forwarding header");
        }

        ECParityUpdate parityUpdateData = message.payload;
        if(parityNodes.isEmpty()){
            parityNodes = parityUpdateData.parityNodes;
        }
        
        InetAddressAndPort primaryNode = message.from();


        // Add recieved data to the global map
        if (parityUpdateData.isOldSSTable) {
            // Initialize the isRequestParityCode
            parityUpdateData.sstable.isRequestParityCode = false;
            if (StorageService.instance.globalOldSSTablesQueueForParityUpdateMap.contains(primaryNode)) {
                StorageService.instance.globalOldSSTablesQueueForParityUpdateMap.get(primaryNode)
                        .add(parityUpdateData.sstable);
            } else {
                StorageService.instance.globalOldSSTablesQueueForParityUpdateMap.put(primaryNode,
                        new ConcurrentLinkedQueue<SSTableContentWithHashID>(Collections.singleton(parityUpdateData.sstable)));
            }
            logger.debug("rymDebug: [Parity Update] Get a old sstable ({}) from primary node {}", parityUpdateData.sstable.sstHash, primaryNode);
        } else {
            if (StorageService.instance.globalNewSSTablesQueueForParityUpdateMap.contains(primaryNode)) {
                StorageService.instance.globalNewSSTablesQueueForParityUpdateMap.get(primaryNode)
                        .add(parityUpdateData.sstable);
            } else {
                StorageService.instance.globalNewSSTablesQueueForParityUpdateMap.put(primaryNode,
                        new ConcurrentLinkedQueue<SSTableContentWithHashID>(Collections.singleton(parityUpdateData.sstable)));
            }
            
            logger.debug("rymDebug: [Parity Update] Get a new sstable ({}) from primary node {}", parityUpdateData.sstable.sstHash, primaryNode);
        }
        
           
        
    }

    private static List<InetAddressAndPort> getParityNodes() {
        return parityNodes;
    }




    public static Runnable getParityUpdateRunnable() {
        return new ParityUpdateRunnable();
    }


    private static class ParityUpdateRunnable implements Runnable {

        @Override
        public void run() {
            String keyspaceName = "ycsb";
            int codeLength = StorageService.getErasureCodeLength();
            List<InetAddressAndPort> parityNodes = getParityNodes();
            
            // Map<String, ByteBuffer[]> sstHashToParityCodeMap = new HashMap<String, ByteBuffer[]>();
    
            // if(parityUpdateData.oldSSTables.size() < parityUpdateData.newSSTables.size()) {
            //     logger.debug("rymERROR: new sstable num should not more than old sstables.");
            // }
    
            // Step 1: [For old sstable] First we need read parity code locally and from peer parity nodes
            for (Map.Entry<InetAddressAndPort, ConcurrentLinkedQueue<SSTableContentWithHashID>> entry : StorageService.instance.globalOldSSTablesQueueForParityUpdateMap.entrySet()) {

                for (SSTableContentWithHashID oldSSTContentWithHash : entry.getValue()) {
                    if (!oldSSTContentWithHash.isRequestParityCode) {

                        String oldSSTHash = oldSSTContentWithHash.sstHash;
                        logger.debug("rymDebug: we need get parity codes for sstable ({})", oldSSTHash);
                        String stripID = StorageService.instance.globalSSTHashToStripID.get(oldSSTHash);
                        if (stripID == null) {
                            logger.debug("rymERROR: In node {}, we cannot get strip id for sstHash {}, this hash is from primary node {}, the old sstable map is {}, new sstable map is {}",
                                    FBUtilities.getBroadcastAddressAndPort(),
                                    oldSSTHash,
                                    entry.getKey(),
                                    StorageService.instance.globalOldSSTablesQueueForParityUpdateMap,
                                    StorageService.instance.globalNewSSTablesQueueForParityUpdateMap);
                        }

                        retrieveParityCodeForOldSSTable(oldSSTHash, stripID, codeLength);
                        oldSSTContentWithHash.isRequestParityCode = true;

                    }

                }

            }

            if( StorageService.instance.globalOldSSTablesQueueForParityUpdateMap.size() < StorageService.instance.globalNewSSTablesQueueForParityUpdateMap.size()) {
                logger.debug("rymERROR: new sstable count ({}) is more than the old count ({})!", StorageService.instance.globalNewSSTablesQueueForParityUpdateMap.size(),
                                                                                                  StorageService.instance.globalOldSSTablesQueueForParityUpdateMap.size());
            }

 
            // Step 2: Perform parity update
            for (Map.Entry<InetAddressAndPort, ConcurrentLinkedQueue<SSTableContentWithHashID>> entry : StorageService.instance.globalOldSSTablesQueueForParityUpdateMap.entrySet()) {
                
                // get oldReplicaNodes
                InetAddressAndPort primaryNode = entry.getKey();
                List<InetAddressAndPort> oldReplicaNodes = StorageService.instance.getReplicaNodesWithPortFromPrimaryNode(primaryNode, keyspaceName);

                ConcurrentLinkedQueue<SSTableContentWithHashID> newSSTableQueue = StorageService.instance.globalNewSSTablesQueueForParityUpdateMap.get(primaryNode);
                ConcurrentLinkedQueue<SSTableContentWithHashID> oldSSTableQueue = entry.getValue();

                if( oldSSTableQueue.size() < newSSTableQueue.size()) {
                    logger.debug("rymERROR: new sstable count ({}) is more than the old count ({})!", newSSTableQueue.size(),
                                                                                                      oldSSTableQueue.size());
                }
                
                // Case1: Consume old data with new data firstly.
                // In this case, old replica nodes are the same to new replica nodes
                while (!oldSSTableQueue.isEmpty() && !newSSTableQueue.isEmpty()) {

                    SSTableContentWithHashID newSSTable = newSSTableQueue.poll();
                    SSTableContentWithHashID oldSSTable = oldSSTableQueue.poll();
                    if(!oldSSTable.isRequestParityCode) {
                        logger.debug("rymDebug: we need get parity codes for sstable ({})", oldSSTable.sstHash);
                        String stripID = StorageService.instance.globalSSTHashToStripID.get(oldSSTable.sstHash);
                        if (stripID == null) {
                            logger.debug("rymERROR: In node {}, we cannot get strip id for sstHash {}, this hash is from primary node {}, the old sstable map is {}, new sstable map is {}",
                                    FBUtilities.getBroadcastAddressAndPort(),
                                    oldSSTable.sstHash,
                                    entry.getKey(),
                                    StorageService.instance.globalOldSSTablesQueueForParityUpdateMap,
                                    StorageService.instance.globalNewSSTablesQueueForParityUpdateMap);
                        }
                        retrieveParityCodeForOldSSTable(oldSSTable.sstHash, stripID, codeLength);
                    }

                    // For safety, we should make sure the parity code is ready
                    waitUntilParityCodesReader(oldSSTable.sstHash);

                    logger.debug("rymDebug: [Parity update case 1] we update old sstable ({}) with new sstable ({})",
                            oldSSTable.sstHash, newSSTable.sstHash);
                    // ByteBuffer oldData = oldSSTable.sstContent;
                    // ByteBuffer newData = newSSTable.sstContent;
                    // codeLength = Stream.of(codeLength, newSSTable.sstContentSize,
                    // oldSSTable.sstContentSize).max(Integer::compareTo).orElse(codeLength);
                    String oldStripID = StorageService.instance.globalSSTHashToStripID.get(oldSSTable.sstHash);
                    Stage.ERASURECODE.maybeExecuteImmediately(new ErasureCodeUpdateRunnable(oldSSTable,
                            newSSTable,
                            StorageService.instance.globalSSTHashToParityCodeMap.get(oldSSTable.sstHash),
                            StorageService.instance.globalECMetadataMap.get(oldStripID).sstHashIdList
                                    .indexOf(oldSSTable.sstHash),
                            codeLength,
                            parityNodes,
                            oldReplicaNodes,
                            oldReplicaNodes));
                }

                if (!newSSTableQueue.isEmpty()) {
                    logger.debug("rymERROR: The new sstables are not completely consumed!!!");
                }

                // Case2: If old data is not completely consumed, we select sstables from
                // globalRecvQueues
                while (!oldSSTableQueue.isEmpty()) {
                    if (oldSSTableQueue.iterator().next().isRequestParityCode) {

                        if (StorageService.instance.globalRecvQueues.containsKey(primaryNode)) {
                            ECMessage msg = StorageService.instance.globalRecvQueues.get(primaryNode).poll();

                            if (StorageService.instance.globalRecvQueues.get(primaryNode).size() == 0) {
                                StorageService.instance.globalRecvQueues.remove(primaryNode);
                            }

                            SSTableContentWithHashID oldSSTable = oldSSTableQueue.poll();
                            waitUntilParityCodesReader(oldSSTable.sstHash);

                            SSTableContentWithHashID newSSTable = new SSTableContentWithHashID(
                                    msg.ecMessageContent.sstHashID, msg.sstContent);
                            String oldStripID = StorageService.instance.globalSSTHashToStripID.get(oldSSTable.sstHash);
                            // codeLength = Stream.of(codeLength, newSSTable.sstContentSize,
                            // oldSSTable.sstContentSize).max(Integer::compareTo).orElse(codeLength);

                            logger.debug("rymDebug: [Parity update case 2] we update old sstable ({}) with new sstable ({})",
                                    oldSSTable.sstHash, newSSTable.sstHash);
                            Stage.ERASURECODE.maybeExecuteImmediately(new ErasureCodeUpdateRunnable(oldSSTable,
                                    newSSTable,
                                    StorageService.instance.globalSSTHashToParityCodeMap.get(oldSSTable.sstHash),
                                    StorageService.instance.globalECMetadataMap.get(oldStripID).sstHashIdList
                                            .indexOf(oldSSTable.sstHash),
                                    codeLength,
                                    parityNodes,
                                    oldReplicaNodes,
                                    oldReplicaNodes));
                            // oldSSTablesIterator.remove();
                        } else {
                            break;
                        }
                    } else {
                        continue;
                    }
                }

                // String logString = "rymDebug: Insight the globalRecvQueues";
                // for (Map.Entry<InetAddressAndPort, ConcurrentLinkedQueue<ECMessage>> entry : StorageService.instance.globalRecvQueues.entrySet()) {
                //     String str = entry.getKey().toString() + " has " + entry.getValue().size() + "elements";
                //     logString += str;
                // }
                // logger.debug(logString);

                // Case3: Old data still not completely consumed, we have to padding zero
                while (!oldSSTableQueue.isEmpty()) {
                    if (oldSSTableQueue.iterator().next().isRequestParityCode) {

                        SSTableContentWithHashID oldSSTable = oldSSTableQueue.poll();
                        waitUntilParityCodesReader(oldSSTable.sstHash);

                        ByteBuffer newSSTContent = ByteBuffer.allocateDirect(codeLength);
                        SSTableContentWithHashID newSSTable = new SSTableContentWithHashID(
                                ECNetutils.stringToHex(String.valueOf(newSSTContent.hashCode())),
                                newSSTContent);
                        // codeLength = Stream.of(codeLength, newSSTable.sstContentSize,
                        // oldSSTable.sstContentSize).max(Integer::compareTo).orElse(codeLength);

                        logger.debug("rymDebug: [Parity update case 3] we update old sstable ({}) with new sstable ({})",
                                oldSSTable.sstHash, newSSTable.sstHash);
                        String oldStripID = StorageService.instance.globalSSTHashToStripID.get(oldSSTable.sstHash);
                        Stage.ERASURECODE.maybeExecuteImmediately(new ErasureCodeUpdateRunnable(oldSSTable,
                                newSSTable,
                                StorageService.instance.globalSSTHashToParityCodeMap.get(oldSSTable.sstHash),
                                StorageService.instance.globalECMetadataMap.get(oldStripID).sstHashIdList
                                        .indexOf(oldSSTable.sstHash),
                                codeLength,
                                parityNodes,
                                oldReplicaNodes,
                                oldReplicaNodes));
                    } else {
                        continue;
                    }
                }
            }

        }

    }


    
    private static void retrieveParityCodeForOldSSTable(String oldSSTHash, String stripID, int codeLength) {

        String localParityCodeDir = ECNetutils.getLocalParityCodeDir();

        // read ec_metadata from memory, get the needed parity hash list
        List<String> parityHashList = null;
        try {
            parityHashList = StorageService.instance.globalECMetadataMap.get(stripID).parityHashList;
        } catch (Exception e) {
            logger.debug("rymERROR: failed to get ecMetadata for stripID {}", stripID);
        }
        ByteBuffer[] parityCodes = new ByteBuffer[parityHashList.size()];
        // get the needed parity code locally
        String parityCodeFileName = localParityCodeDir + parityHashList.get(0);
        ByteBuffer localParityCode;
        try {
            localParityCode = ByteBuffer.wrap(ECNetutils.readBytesFromFile(parityCodeFileName));
            // delete local parity code file
            ECNetutils.deleteFileByName(parityCodeFileName);

            if (codeLength == 0)
                codeLength = localParityCode.capacity();

            for (int i = 0; i < parityHashList.size(); i++) {
                parityCodes[i] = ByteBuffer.allocateDirect(localParityCode.capacity());
            }
            parityCodes[0].put(localParityCode);
            parityCodes[0].rewind();

            // get old parity codes from old sstable hash
            StorageService.instance.globalSSTHashToParityCodeMap.put(oldSSTHash, parityCodes);

            // get the needed parity code remotely, send a parity code request
            for (int i = 1; i < parityHashList.size(); i++) {
                ECRequestParity request = new ECRequestParity(parityHashList.get(i), oldSSTHash, i);
                request.requestParityCode(parityNodes.get(i));
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }



    // [WARNING!] Make sure to avoid dead loops
    private static void waitUntilParityCodesReader(String sstHash) {
        int retryCount = 0;

        ByteBuffer[] parityCodes = StorageService.instance.globalSSTHashToParityCodeMap.get(sstHash);
        if(parityCodes == null) {
            logger.debug("rymERROR: We cannot get parity codes for sstable {}", sstHash);
        }
 
        while (!checkParityCodesAreReady(parityCodes)) {
            try {
                if(retryCount < 10) {
                    Thread.sleep(2);
                    retryCount++;
                } else {
                    throw new IllegalStateException(String.format("ERROR: cannot retrieve the remote parity codes for sstHash (%s)", sstHash));
                }
                
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    private static boolean checkParityCodesAreReady(ByteBuffer[] parityCodes) {
        for(ByteBuffer buf : parityCodes) {
            if(buf.limit() - buf.position() == 0) {
                return false;
            }
        }
        return true;
    }

    private static void forwardToLocalNodes(Message<ECParityUpdate> originalMessage, ForwardingInfo forwardTo) {
        Message.Builder<ECParityUpdate> builder = Message.builder(originalMessage)
                .withParam(ParamType.RESPOND_TO, originalMessage.from())
                .withoutParam(ParamType.FORWARD_TO);

        boolean useSameMessageID = forwardTo.useSameMessageID(originalMessage.id());
        // reuse the same Message if all ids are identical (as they will be for 4.0+
        // node originated messages)
        Message<ECParityUpdate> message = useSameMessageID ? builder.build() : null;

        forwardTo.forEach((id, target) -> {
            Tracing.trace("Enqueuing forwarded write to {}", target);
            MessagingService.instance().send(useSameMessageID ? message : builder.withId(id).build(), target);
        });
    }


    
    /** [CASSANDRAEC]
     * To support perform erasure coding with multiple threads, we implement the following Runnable class
     * @param ecDataNum the value of k
     * @param ecParity the value of m
     * @param messages the input data to be processed, length equal to ecDataNum
     */
    private static  class ErasureCodeUpdateRunnable implements Runnable {
        private final int ecDataNum = DatabaseDescriptor.getEcDataNodes();
        private final int ecParityNum = DatabaseDescriptor.getParityNodes();
        private final SSTableContentWithHashID oldSSTable;
        private final SSTableContentWithHashID newSSTable;
        private final ByteBuffer[] parityCodes;
        private final int targetDataIndex;
        private final int codeLength;
        private final List<InetAddressAndPort> parityNodes;
        private final List<InetAddressAndPort> oldRelicaNodes;
        private final List<InetAddressAndPort> newRelicaNodes;

        ErasureCodeUpdateRunnable(SSTableContentWithHashID oldSSTable,
                                  SSTableContentWithHashID newSSTable,
                                  ByteBuffer[] oldParityCodes,
                                  int targetDataIndex, int codeLength, List<InetAddressAndPort> parityNodes,
                                  List<InetAddressAndPort> oldRelicaNodes,
                                  List<InetAddressAndPort> newRelicaNodes) {
            this.oldSSTable = oldSSTable;
            this.newSSTable = newSSTable;
            this.parityCodes = oldParityCodes;
            this.targetDataIndex = targetDataIndex;
            this.codeLength = codeLength;
            this.parityNodes = parityNodes;
            this.oldRelicaNodes = oldRelicaNodes;
            this.newRelicaNodes = newRelicaNodes;
        }

        @Override
        public void run() {
            

            ErasureCoderOptions ecOptions = new ErasureCoderOptions(ecDataNum, ecParityNum);
            ErasureEncoder encoder = new NativeRSEncoder(ecOptions);

            logger.debug("rymDebug: let's start computing erasure coding");

            // Encoding input and output
            ByteBuffer[] oldData = new ByteBuffer[1];
            ByteBuffer[] newData = new ByteBuffer[1];

            // prepare old data
            oldData[0] = ByteBuffer.allocateDirect(codeLength);
            oldData[0].put(oldSSTable.sstContent);
            int oldRemaining = oldData[0].remaining();
            if(oldRemaining>0) {
                byte[] zeros = new byte[oldRemaining];
                oldData[0].put(zeros);
            }
            oldData[0].rewind();

            // Prepare new data
            newData[0] = ByteBuffer.allocateDirect(codeLength);
            newData[0].put(newSSTable.sstContent);
            int newRemaining = newData[0].remaining();
            if(newRemaining>0) {
                byte[] zeros = new byte[newRemaining];
                newData[0].put(zeros);
            }
            newData[0].rewind();

            ByteBuffer[] newParityCodes = new ByteBuffer[parityCodes.length];
            for(int i = 0; i < newParityCodes.length; i++) {
                newParityCodes[i] = ByteBuffer.allocateDirect(codeLength);
            }


            // 0: old data, 1: new data, m is old parity codes
            ByteBuffer[] dataUpdate = new ByteBuffer[2 + parityCodes.length];
            for(int i=0;i < dataUpdate.length;i++) {
                dataUpdate[i] = ByteBuffer.allocateDirect(codeLength);
            }
            // fill this buffer
            dataUpdate[0] = oldData[0];
            dataUpdate[0].rewind();
            dataUpdate[1] = newData[0];
            dataUpdate[1].rewind();

            for(int i = 2; i< dataUpdate.length; i++) {
                dataUpdate[i] = parityCodes[i-2];
                dataUpdate[i].rewind();
            }

            // Encode update
            try {
                encoder.encodeUpdate(dataUpdate, newParityCodes, targetDataIndex);
            } catch (IOException e) {
                logger.error("rymERROR: Perform erasure code error", e);
            }

            
            // generate parity hash code
            List<String> parityHashList = new ArrayList<String>();
            for(ByteBuffer parityCode : newParityCodes) {
                parityHashList.add(ECNetutils.stringToHex(String.valueOf(parityCode.hashCode())));
            }

            // record first parity code to current node
            try {
                String localParityCodeDir = ECNetutils.getLocalParityCodeDir();
                FileChannel fileChannel = FileChannel.open(Paths.get(localParityCodeDir, parityHashList.get(0)),
                                                            StandardOpenOption.WRITE,
                                                             StandardOpenOption.CREATE);
                fileChannel.write(newParityCodes[0]);
                fileChannel.close();
                // logger.debug("rymDebug: parity code file created: {}", parityCodeFile.getName());
            } catch (IOException e) {
                logger.error("rymERROR: Perform erasure code error", e);
            }


            // sync encoded data to parity nodes
            ECParityNode ecParityNode = new ECParityNode(null, null, 0);
            ecParityNode.distributeCodedDataToParityNodes(newParityCodes, parityNodes, parityHashList);

            // update ECMetadata and distribute it
            // get old ECMetadata content 
            String stripID = StorageService.instance.globalSSTHashToStripID.get(oldSSTable.sstHash);
            ECMetadataContent oldMetadata = StorageService.instance.globalECMetadataMap.get(stripID);
            // update the isParityUpdate, sstHashIdList, parityHashList, replication nodes, stripID
            ECMetadata ecMetadata = new ECMetadata(stripID, oldMetadata);
            ecMetadata.updateAndDistributeMetadata(parityHashList, true,
                                                   oldSSTable.sstHash, newSSTable.sstHash, targetDataIndex,
                                                   oldRelicaNodes, newRelicaNodes);

        }

    }
    
}
