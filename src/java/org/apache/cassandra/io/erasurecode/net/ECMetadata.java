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
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.erasurecode.net.ECNetutils.ByteObjectConversion;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.db.TypeSizes.sizeof;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
        public List<String> parityCodeHashList;
        public Map<String, List<InetAddressAndPort>> sstHashIdToReplicaMap;
        public List<InetAddressAndPort> primaryNodes;
        public Set<InetAddressAndPort> relatedNodes; // e.g. secondary nodes or parity nodes
        
        public ECMetadataContent(String ks, String cf, List<String> sstHashIdList, List<String> parityCodeHashList,
        List<InetAddressAndPort> primaryNodes, Set<InetAddressAndPort> relatedNodes, 
        Map<String, List<InetAddressAndPort>> sstHashIdToReplicaMap) {
            this.keyspace = ks;
            this.cfName = cf;
            this.sstHashIdList = sstHashIdList;
            this.parityCodeHashList = parityCodeHashList;
            this.primaryNodes = primaryNodes;
            this.relatedNodes = relatedNodes;
            this.sstHashIdToReplicaMap = sstHashIdToReplicaMap;
        }
    }

    public ECMetadata(String stripeId, ECMetadataContent ecMetadataContent) {
        this.stripeId = stripeId;
        this.ecMetadataContent = ecMetadataContent;
    }

    public void generateMetadata(ECMessage[] messages, ByteBuffer[] parityCode, List<String> parityHashes) {
        logger.debug("rymDebug: this generateMetadata method");
        // get stripe id, sst content hashes and primary nodes
        String connectedSSTHash = "";
        for(ECMessage msg : messages) {
            String sstContentHash = msg.sstHashID;
            this.ecMetadataContent.sstHashIdList.add(sstContentHash);
            this.ecMetadataContent.sstHashIdToReplicaMap.putIfAbsent(sstContentHash, msg.replicaNodes);
            connectedSSTHash += sstContentHash;
            this.ecMetadataContent.primaryNodes.add(msg.replicaNodes.get(0));
        }
        
        this.stripeId = String.valueOf(connectedSSTHash.hashCode());
        this.ecMetadataContent.keyspace = messages[0].keyspace;
        this.ecMetadataContent.cfName = messages[0].cfName;

        // generate parity code hash
        this.ecMetadataContent.parityCodeHashList = parityHashes;

        // get related nodes
        // if everything goes well, each message has the same parity code
        for(InetAddressAndPort pns : messages[0].parityNodes) {
            this.ecMetadataContent.relatedNodes.add(pns);
        }
        // also need to add related nodes
        for(ECMessage msg : messages) {
            for(InetAddressAndPort pns : msg.replicaNodes) {
                if(!this.ecMetadataContent.primaryNodes.contains(pns))
                    this.ecMetadataContent.relatedNodes.add(pns);
            }
        }

        try {
            this.ecMetadataContentBytes = ByteObjectConversion.objectToByteArray((Serializable) this.ecMetadataContent);
            this.ecMetadataContentBytesSize = this.ecMetadataContentBytes.length;
            if(this.ecMetadataContentBytes.length == 0) {
                logger.error("rymError: no metadata content"); 
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        

        // dispatch to related nodes
        distributeEcMetadata(this);

    }

    public void distributeEcMetadata(ECMetadata ecMetadata) {
        logger.debug("rymDebug: this distributeEcMetadata method");
        Message<ECMetadata> message = Message.outWithFlag(Verb.ECMETADATA_REQ, ecMetadata, MessageFlag.CALL_BACK_ON_FAILURE);
        for (InetAddressAndPort node : ecMetadata.ecMetadataContent.relatedNodes) {
            if(!node.equals(FBUtilities.getBroadcastAddressAndPort())) {
                MessagingService.instance().send(message, node);
            }
        }
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
