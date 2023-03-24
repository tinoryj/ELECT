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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.db.TypeSizes.sizeof;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECMetadata {

    private String stripeId;
    private List<String> sstContentHashList;
    private List<String> parityCodeHashList;

    private List<InetAddressAndPort> primaryNodes;
    private Set<InetAddressAndPort> relatedNodes; // e.g. secondary nodes or parity nodes
    public static final ECMetadata instance = new ECMetadata();
    
    private static final Logger logger = LoggerFactory.getLogger(ECMetadata.class);
    public static final Serializer serializer = new Serializer();

    public ECMetadata() {
        this.stripeId = "";
        this.sstContentHashList = new ArrayList<String>();
        this.parityCodeHashList = new ArrayList<String>();
        this.primaryNodes = new ArrayList<InetAddressAndPort>();
        this.relatedNodes = new HashSet<InetAddressAndPort>();
    }

    public void generateMetadata(ECMessage[] messages, ByteBuffer[] parityCode, List<String> parityHashes) {
        // get stripe id, sst content hashes and primary nodes
        String connectedSSTHash = "";
        for(ECMessage msg : messages) {
            String sstContentHash = String.valueOf(msg.sstContent.hashCode());
            this.sstContentHashList.add(sstContentHash);
            connectedSSTHash += sstContentHash;
            this.primaryNodes.add(msg.replicationEndpoints.get(0));
        }
        this.stripeId = String.valueOf(connectedSSTHash.hashCode());

        // generate parity code hash
        this.parityCodeHashList = parityHashes;

        // get related nodes
        // if everything goes well, each message has the same parity code
        for(InetAddressAndPort pns : messages[0].parityNodes) {
            this.relatedNodes.add(pns);
        }
        // also need to add related nodes
        for(ECMessage msg : messages) {
            for(InetAddressAndPort pns : msg.replicationEndpoints) {
                if(!this.primaryNodes.contains(pns))
                    this.relatedNodes.add(pns);
            }
        }
        
        // dispatch to related nodes
        distributeEcMetadata(this);

    }

    public void distributeEcMetadata(ECMetadata ecMetadata) {
        Message<ECMetadata> message = Message.outWithFlag(Verb.ERASURECODE_REQ, ecMetadata, MessageFlag.CALL_BACK_ON_FAILURE);
        for (InetAddressAndPort node : ecMetadata.relatedNodes) {
            MessagingService.instance().send(message, node);
        }
    }

    public static final class Serializer implements IVersionedSerializer<ECMetadata>{

        @Override
        public void serialize(ECMetadata t, DataOutputPlus out, int version) throws IOException {
            
            out.writeUTF(t.stripeId);
            out.writeUTF(t.sstContentHashList.toString());
            out.writeUTF(t.parityCodeHashList.toString());
            out.writeUTF(t.primaryNodes.toString());
            out.writeUTF(t.relatedNodes.toString());
            
        }

        @Override
        public ECMetadata deserialize(DataInputPlus in, int version) throws IOException {
            // TODO: Correct data types, and revise the Constructor
            String stripeId = in.readUTF();
            String sstContentHashList = in.readUTF();
            String parityCodeHashList = in.readUTF();
            String primaryNodes = in.readUTF();
            String relatedNodes = in.readUTF();


            return new ECMetadata();
        }

        @Override
        public long serializedSize(ECMetadata t, int version) {
            long size = sizeof(t.stripeId) + sizeof(t.sstContentHashList.toString()) + sizeof(t.parityCodeHashList.toString())+ 
            sizeof(t.primaryNodes.toString()) + sizeof(t.relatedNodes.toString());
            return size;
        }

    }

}
