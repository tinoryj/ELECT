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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.erasurecode.net.ECNetutils.ByteObjectConversion;
import org.apache.cassandra.io.erasurecode.net.ECParityUpdate.SSTableContentWithHashID;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.db.TypeSizes.sizeof;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is to generate ECMetadata and distribute metadata to the replica nodes
 * @param stripeId the global unique id of ECMetadata, generated from the {@value sstHashList}
 * @param ecMetadataContent the content of ECMetadata
 * @param ecMetadataContentBytes the serialized data of ecMetadataContent
 * @param ecMetadataSize the size of the content of ecMetadata
 * 
 * ecMetadataContent is consist of the following parameters:
 * @param keyspace
 * @param cfName
 * @param sstHashList the hash code list of the data blocks
 * @param parityHashList the hash code list of the parity blocks
 * @param primaryNodes 
 * @param secondaryNodes
 * @param parityNodes Note that the parity nodes are the same among each entry
 * 
 * 
 * There are two key methods:
 * @method generateMetadata
 * @method distributeEcMetadata
 */

public class ECMetadata implements Serializable {
    // TODO: improve the performance
    public String stripeId;
    public ECMetadataContent ecMetadataContent;

    public byte[] ecMetadataContentBytes;
    public int ecMetadataContentBytesSize;

    
    private static final Logger logger = LoggerFactory.getLogger(ECMetadata.class);
    public static final Serializer serializer = new Serializer();

    public static class ECMetadataContent implements Serializable {
        
        public String keyspace;
        public String cfName;
        public List<String> sstHashIdList;
        public List<String> parityHashList;
        public Map<String, List<InetAddressAndPort>> sstHashIdToReplicaMap;
        public List<InetAddressAndPort> primaryNodes;
        public Set<InetAddressAndPort> secondaryNodes;
        public List<InetAddressAndPort> parityNodes;
        
        // The following properties is only for parity update
        public boolean isParityUpdate = false;
        public int targetIndex = 0;
        public String oldSSTHash = "";

        public ECMetadataContent(String ks, String cf, List<String> sstHashIdList, List<String> parityHashList,
        List<InetAddressAndPort> primaryNodes, Set<InetAddressAndPort> secondaryNodes, List<InetAddressAndPort> parityNodes,
        Map<String, List<InetAddressAndPort>> sstHashIdToReplicaMap) {
            this.keyspace = ks;
            this.cfName = cf;
            this.sstHashIdList = sstHashIdList;
            this.parityHashList = parityHashList;
            this.primaryNodes = primaryNodes;
            this.secondaryNodes = secondaryNodes;
            this.parityNodes = parityNodes;
            this.sstHashIdToReplicaMap = sstHashIdToReplicaMap;
        }
    }

    public ECMetadata(String stripeId, ECMetadataContent ecMetadataContent) {
        this.stripeId = stripeId;
        this.ecMetadataContent = ecMetadataContent;
    }

    /**
     * This method is to generate the metadata for the given messages
     * @param messages this is the data block for erasure coding, this size is equal to k
     * @param parityCode this is the parity block for erasure coding, size is m
     * @param parityHashes
     */
    public void generateAndDistributeMetadata(ECMessage[] messages, List<String> parityHashes) {
        logger.debug("rymDebug: this generateMetadata method");
        // get stripe id, sst content hashes and primary nodes
        String connectedSSTHash = "";
        for(ECMessage msg : messages) {
            String sstContentHash = msg.ecMessageContent.sstHashID;
            this.ecMetadataContent.sstHashIdList.add(sstContentHash);
            this.ecMetadataContent.sstHashIdToReplicaMap.putIfAbsent(sstContentHash, msg.ecMessageContent.replicaNodes);
            connectedSSTHash += sstContentHash;
            this.ecMetadataContent.primaryNodes.add(msg.ecMessageContent.replicaNodes.get(0));
        }
        
        this.stripeId = String.valueOf(connectedSSTHash.hashCode());
        this.ecMetadataContent.keyspace = messages[0].ecMessageContent.keyspace;
        this.ecMetadataContent.cfName = messages[0].ecMessageContent.cfName;

        // generate parity code hash
        this.ecMetadataContent.parityHashList = parityHashes;

        // get related nodes
        // if everything goes well, each message has the same parity code
        this.ecMetadataContent.parityNodes.addAll(messages[0].ecMessageContent.parityNodes);

        // initialize the secondary nodes
        for(ECMessage msg : messages) {

            for(int i = 1; i < msg.ecMessageContent.replicaNodes.size();i++) {
                this.ecMetadataContent.secondaryNodes.add(msg.ecMessageContent.replicaNodes.get(i));
            }
            // for(InetAddressAndPort pns : msg.ecMessageContent.replicaNodes) {
            //     if(!this.ecMetadataContent.primaryNodes.contains(pns))
            //         this.ecMetadataContent.secondaryNodes.add(pns);
            // }
        }

        try {
            this.ecMetadataContentBytes = ByteObjectConversion.objectToByteArray((Serializable) this.ecMetadataContent);
            this.ecMetadataContentBytesSize = this.ecMetadataContentBytes.length;
            if(this.ecMetadataContentBytes.length == 0) {
                logger.error("rymERROR: no metadata content"); 
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        for(String sstHash : this.ecMetadataContent.sstHashIdList) {
            StorageService.instance.globalSSTHashToStripID.put(sstHash, this.stripeId);
            if(StorageService.instance.globalPairtyUpdateSSTableWaitForErasureCodingReadyMap.containsKey(sstHash)) {
                // we move this sstable to the globalOldSSTablesQueueForParityUpdateMap
                InetAddressAndPort primaryNode = this.ecMetadataContent.sstHashIdToReplicaMap.get(sstHash).get(0);
                SSTableContentWithHashID oldSSTableForParityUpdate = StorageService.instance.globalPairtyUpdateSSTableWaitForErasureCodingReadyMap.get(sstHash);
                StorageService.instance.globalOldSSTablesQueueForParityUpdateMap.get(primaryNode).add(oldSSTableForParityUpdate);

                int codeLength = StorageService.getErasureCodeLength();
                ECParityUpdateVerbHandler.retrieveParityCodeForOldSSTable(sstHash, this.stripeId, codeLength);

            }
            logger.debug("rymDebug:[ErasureCoding] In node {}, we map sstHash {} to stripID {}", FBUtilities.getBroadcastAddressAndPort(),
                                                                                                 sstHash,
                                                                                                 this.stripeId);
        }
        

        // dispatch to related nodes
        distributeECMetadata(this);

    }

    /**
     * This method is to update the metadata for the given messages
     * @param parityHashes
     */
    public void updateAndDistributeMetadata(List<String> newParityHashes, boolean isParityUpdate, 
                                            String oldSSTHash, String newSSTHash, int targetIndex,
                                            List<InetAddressAndPort> oldReplicaNodes, List<InetAddressAndPort> newReplicaNodes) {
        logger.debug("rymDebug: this update ECMetadata method, we update old sstable ({}) with new sstable ({})", oldSSTHash, newSSTHash);
        // update isParityUpdate
        this.ecMetadataContent.isParityUpdate = isParityUpdate;
        // update the old sstable hash
        this.ecMetadataContent.oldSSTHash = oldSSTHash;
        // update sstable hash list
        this.ecMetadataContent.sstHashIdList.set(targetIndex, newSSTHash);
        // update parity code hash
        this.ecMetadataContent.parityHashList = newParityHashes;
        // modify sstHashIdToReplicaMap
        this.ecMetadataContent.sstHashIdToReplicaMap.put(newSSTHash, newReplicaNodes);
        this.ecMetadataContent.sstHashIdToReplicaMap.remove(oldSSTHash);
        // update the target index
        this.ecMetadataContent.targetIndex = targetIndex;

        if(!oldReplicaNodes.equals(newReplicaNodes)) {
            logger.warn("rymDebug: new replication nodes {} are different from old replication nodes {}",
                             newReplicaNodes, oldReplicaNodes);
            // update primary node list
            this.ecMetadataContent.primaryNodes.set(targetIndex, newReplicaNodes.get(0));

            // update secondary node list
            this.ecMetadataContent.secondaryNodes = new HashSet<InetAddressAndPort>();
            // update secondary nodes
            for(Map.Entry<String, List<InetAddressAndPort>> entry : this.ecMetadataContent.sstHashIdToReplicaMap.entrySet()) {
                for (int i = 1; i < entry.getValue().size(); i++) {
                    this.ecMetadataContent.secondaryNodes.add(entry.getValue().get(i));
                }
            }
            
        }

        // remove the ECMetadata from memory
        // StorageService.instance.globalECMetadataMap.remove(this.stripeId);

        // update strip id
        String connectedSSTHash = "";
        for(String sstHash : this.ecMetadataContent.sstHashIdList) {
            connectedSSTHash += sstHash;
        }
        String oldStripId = this.stripeId;
        this.stripeId = ECNetutils.stringToHex(String.valueOf(connectedSSTHash.hashCode()));
        logger.debug("rymDebug: Update old strip id ({}) with a new one ({})", oldStripId, this.stripeId);



        try {
            this.ecMetadataContentBytes = ByteObjectConversion.objectToByteArray((Serializable) this.ecMetadataContent);
            this.ecMetadataContentBytesSize = this.ecMetadataContentBytes.length;
            if(this.ecMetadataContentBytes.length == 0) {
                logger.error("rymERROR: no metadata content"); 
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        for(String sstHash : this.ecMetadataContent.sstHashIdList) {
            StorageService.instance.globalSSTHashToStripID.put(sstHash, this.stripeId);

            
            logger.debug("rymDebug:[Parity Update] In node {}, we map sstHash {} to stripID {}", FBUtilities.getBroadcastAddressAndPort(),
                                                                                                 sstHash,
                                                                                                 this.stripeId);
        }
        // StorageService.instance.globalSSTHashToStripID.remove(oldSSTHash);
        

        // dispatch to related nodes
        distributeECMetadata(this);

    }


    /**
     * [In parity] Distribute ecMetadata to secondary nodes
     */
    public void distributeECMetadata(ECMetadata ecMetadata) {
        logger.debug("rymDebug: [In parity node ({})] This distributeEcMetadata method, we should send stripId ({}) with sstables list ({}) to node ({}), the sstHashToRelicaMap is ({})",
                    FBUtilities.getBroadcastAddressAndPort(), ecMetadata.stripeId, ecMetadata.ecMetadataContent.sstHashIdList, ecMetadata.ecMetadataContent.secondaryNodes, 
                    ecMetadata.ecMetadataContent.sstHashIdToReplicaMap);
        Message<ECMetadata> message = Message.outWithFlag(Verb.ECMETADATA_REQ, ecMetadata, MessageFlag.CALL_BACK_ON_FAILURE);
        
        // send to secondary nodes 
        int rf = 3;
        logger.debug("rymDebug: For strip id ({}), we should record ecSSTable ({}) times in total", ecMetadata.stripeId, DatabaseDescriptor.getEcDataNodes() * (rf-1));
        for (InetAddressAndPort node : ecMetadata.ecMetadataContent.secondaryNodes) {
            MessagingService.instance().send(message, node);
            // if(node.equals(FBUtilities.getBroadcastAddressAndPort())) {
            //     MessagingService.instance().send(message, node);
            // }
        }

        // send to remote parity nodes
        // for (InetAddressAndPort node : ecMetadata.ecMetadataContent.parityNodes) {
        //     if(!node.equals(FBUtilities.getBroadcastAddressAndPort())) {
        //         MessagingService.instance().send(message, node);
        //     }
        // }

        // store ecMetadata locally
        StorageService.instance.globalECMetadataMap.put(ecMetadata.stripeId, ecMetadata.ecMetadataContent);
        logger.debug("rymDebug: store stripID {} in node {}", ecMetadata.stripeId, FBUtilities.getBroadcastAddressAndPort());
    }

    public static final class Serializer implements IVersionedSerializer<ECMetadata>{

        @Override
        public void serialize(ECMetadata t, DataOutputPlus out, int version) throws IOException {
            
            out.writeUTF(t.stripeId);
            out.writeInt(t.ecMetadataContentBytesSize);
            out.write(t.ecMetadataContentBytes);            
        }

        @Override
        public ECMetadata deserialize(DataInputPlus in, int version) throws IOException {
            // TODO: Correct data types, and revise the Constructor
            String stripeId = in.readUTF();
            int ecMetadataContentBytesSize = in.readInt();
            byte[] ecMetadataContentBytes = new byte[ecMetadataContentBytesSize];
            in.readFully(ecMetadataContentBytes);

            try {
                ECMetadataContent eMetadataContent = (ECMetadataContent) ByteObjectConversion.byteArrayToObject(ecMetadataContentBytes);
                return new ECMetadata(stripeId, eMetadataContent);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                logger.error("ERROR: get sstables in bytes error!");
            }
            return null;
        }

        @Override
        public long serializedSize(ECMetadata t, int version) {
            long size = sizeof(t.stripeId) + 
                        sizeof(t.ecMetadataContentBytesSize) +
                        t.ecMetadataContentBytesSize;
            return size;
        }

    }

}
