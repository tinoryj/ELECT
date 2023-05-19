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

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.CollationElementIterator;
import java.io.IOException;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.erasurecode.net.ECNetutils.ByteObjectConversion;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import java.io.Serializable;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.Output;
import org.apache.cassandra.utils.FBUtilities;
import org.checkerframework.dataflow.qual.TerminatesExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tartarus.snowball.TestApp;

import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.db.TypeSizes.sizeofLong;

public class ECMessage implements Serializable {

    public static final Serializer serializer = new Serializer();


    private static AtomicInteger GLOBAL_COUNTER = new AtomicInteger(0);

    public ECMessageContent ecMessageContent;
    public byte[] ecMessageContentInBytes;
    public int ecMessageContentInBytesSize;

    public static class ECMessageContent implements Serializable {
        public final ByteBuffer sstContent;
        public final int sstSize;
        public final String sstHashID;
        public final String keyspace;
        public final String cfName;
        public final int ecDataNum;
        public final int rf;

        public final int ecParityNum;

        public List<InetAddressAndPort> replicaNodes;
        public List<InetAddressAndPort> parityNodes;

        public ECMessageContent(ByteBuffer sstContent, String sstHashID, String keyspace, String cfName,
                List<InetAddressAndPort> replicaNodes) {

            this.sstContent = sstContent;
            this.sstSize = sstContent.remaining();
            this.sstHashID = sstHashID;
            this.keyspace = keyspace;
            this.cfName = cfName;
            this.ecDataNum = DatabaseDescriptor.getEcDataNodes();
            this.ecParityNum = DatabaseDescriptor.getParityNodes();
            this.rf = Keyspace.open(keyspace).getReplicationStrategy().getReplicationFactor().allReplicas;

            this.replicaNodes = new ArrayList<InetAddressAndPort>(replicaNodes);
            this.parityNodes = new ArrayList<InetAddressAndPort>();
}
    }




    public ECMessage(ECMessageContent ecMessageContent) {
        this.ecMessageContent = ecMessageContent;
    }

    protected static Output output;
    public static final Logger logger = LoggerFactory.getLogger(ECMessage.class);

    /**
     * This method sends selected sstables to parity nodes for EC/
     * 
     * @param sstContent selected sstables
     * @param k          number of parity nodes
     * @param ks         keyspace name of sstables
     * @throws UnknownHostException
     *                              TODO List
     *                              1. implement Verb.ERASURECODE_REQ
     *                              2. implement responsehandler
     */
    public void sendSSTableToParity() throws UnknownHostException {
        logger.debug("rymDebug: this is sendSelectedSSTables");

        // create a Message for sstContent
        Message<ECMessage> message = null;
        // GLOBAL_COUNTER++;

        getTargetEdpoints(this);

        // for (InetAddressAndPort ep : this.replicaNodes) {
        //     this.repEpsString += ep.toString() + ",";
        // }

        // for (InetAddressAndPort ep : this.parityNodes) {
        //     this.parityNodesString += ep.toString() + ",";
        // }

        try {
            this.ecMessageContentInBytes = ByteObjectConversion.objectToByteArray((Serializable) this.ecMessageContent);
            this.ecMessageContentInBytesSize = this.ecMessageContentInBytes.length;
            if(this.ecMessageContentInBytesSize == 0) {
                logger.error("rymERROR: ecMessageInBytesSize is 0"); 
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        if (this.ecMessageContent.parityNodes != null) {
            // logger.debug("target endpoints are : {}", this.parityNodes);
            message = Message.outWithFlag(Verb.ERASURECODE_REQ, this, MessageFlag.CALL_BACK_ON_FAILURE);
            MessagingService.instance().sendSSTContentWithoutCallback(message, this.ecMessageContent.parityNodes.get(0));
        } else {
            logger.debug("targetEndpoints is null");
        }
    }

    /*
     * Get target nodes, use the methods related to nodetool.java and status.java
     */
    protected static void getTargetEdpoints(ECMessage ecMessage) throws UnknownHostException {

        // get all live nodes
        List<InetAddressAndPort> liveEndpoints = new ArrayList<>(Gossiper.instance.getLiveMembers());

        // logger.debug("rymDebug: All living nodes are {}", liveEndpoints);
        // logger.debug("rymDebug: ecMessage.replicaNodes is {}", ecMessage.replicaNodes);

        // select parity nodes from live nodes, suppose all nodes work healthy
        int n = liveEndpoints.size();
        InetAddressAndPort primaryNode = ecMessage.ecMessageContent.replicaNodes.get(0);
        int primaryNodeIndex = liveEndpoints.indexOf(primaryNode);

        int startIndex = ((primaryNodeIndex + n - (GLOBAL_COUNTER.getAndIncrement() % ecMessage.ecMessageContent.ecDataNum+1))%n);
        for (int i = startIndex; i < ecMessage.ecMessageContent.ecParityNum+startIndex; i++) {
            int index = i%n;
            if(index==primaryNodeIndex) {
                index = (index+1)%n;
                i++;
            }
            ecMessage.ecMessageContent.parityNodes.add(liveEndpoints.get(index));
            if(i == (ecMessage.ecMessageContent.ecParityNum + startIndex)
                  && ecMessage.ecMessageContent.parityNodes.size() < ecMessage.ecMessageContent.ecParityNum) {

                startIndex++;
            }
        }
        // logger.debug("rymDebug: ecMessage.parityNodes is {}", ecMessage.parityNodes);

    }

    public static final class Serializer implements IVersionedSerializer<ECMessage> {

        @Override
        public void serialize(ECMessage ecMessage, DataOutputPlus out, int version) throws IOException {
            // TODO: reduce (de)serialize cost
            // logger.debug("rymDebug: [Load] the length of sstContent buffer is: {}" , ecMessage.sstSize);
            // logger.debug("rymDebug: [Load] the size of = {}" , sizeofLong(ecMessage.sstContent));
            // out.writeUTF(ecMessage.sstContent);

            ////////////////////////////////////////////////
            // out.writeUTF(ecMessage.sstHashID);
            // out.writeUTF(ecMessage.keyspace);
            // out.writeUTF(ecMessage.cfName);
            // out.writeUTF(ecMessage.repEpsString);
            // out.writeUTF(ecMessage.parityNodesString);

            // out.writeInt(ecMessage.sstSize);
            // byte[] buf = new byte[ecMessage.sstSize];
            // ecMessage.sstContent.get(buf);
            // out.write(buf);
            ///////////////////////////////////////////////

            out.writeInt(ecMessage.ecMessageContentInBytesSize);
            out.write(ecMessage.ecMessageContentInBytes);
            // logger.debug("rymDebug: [serialize] write successfully", buf.length);
        }

        @Override
        public ECMessage deserialize(DataInputPlus in, int version) throws IOException {
            // String sstContent = in.readUTF();
            // String sstContent = "sstContentTest";

            ///////////////////////////////////////////
            // String sstHashID = in.readUTF();
            // String ks = in.readUTF();
            // String cf = in.readUTF();
            // String repEpsString = in.readUTF();
            // String parityNodesString = in.readUTF();



            // // logger.debug("rymDebug: deserialize.ecMessage.sstHashID is {},ks is: {}, cf is {},repEpString is {},parityNodes are: {}"
            // // , sstHashID,ks, cf,repEpsString,parityNodesString);

            // int sstSize = in.readInt();
            // byte[] buf = new byte[sstSize];
            // in.readFully(buf);
            // ByteBuffer sstContent = ByteBuffer.wrap(buf);

            // // logger.debug("rymDebug: deserialize.sstSize is {}, sstContent length is {}", sstSize, sstContent.remaining());

            // List<InetAddressAndPort> replicaNodes = new ArrayList<InetAddressAndPort>();
            // for (String ep : repEpsString.split(",")) {
            //     replicaNodes.add(InetAddressAndPort.getByName(ep.substring(1)));
            // }
            /////////////////////////////////////////////////////////////

            int ecMessageContentInBytesSize = in.readInt();
            byte[] ecMessageContentInBytes = new byte[ecMessageContentInBytesSize];
            in.readFully(ecMessageContentInBytes);
            
            ECMessageContent ecMessage;
            try {
                ecMessage = (ECMessageContent) ByteObjectConversion.byteArrayToObject(ecMessageContentInBytes);
                return new ECMessage(ecMessage);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public long serializedSize(ECMessage ecMessage, int version) {

            // logger.debug("rymDebug: serializedSize.ecMessage.sstHashID is {},ks is: {}, cf is {},repEpString is {},parityNodes are: {}"
            // , ecMessage.sstHashID,ecMessage.keyspace, ecMessage.cfName,ecMessage.repEpsString,ecMessage.parityNodesString);
            // logger.debug("rymDebug: [Cacl] the length of sstContent.size is: {}" , ecMessage.sstSize);
            // logger.debug("rymDebug: [Cacl] the ecMessage.sstContent.remaining() = {}" , ecMessage.sstContent.remaining());
            
            //////////////////////////////////////
            // long size = ecMessage.sstSize + sizeof(ecMessage.sstSize) +
            //             sizeof(ecMessage.sstHashID) + 
            //             sizeof(ecMessage.keyspace) + 
            //             sizeof(ecMessage.cfName) + 
            //             sizeof(ecMessage.parityNodesString) + 
            //             sizeof(ecMessage.repEpsString);
            //////////////////////////////////////////////////
            
            long size = ecMessage.ecMessageContentInBytesSize +
                        sizeof(ecMessage.ecMessageContentInBytesSize);

            return size;

        }

    }

}
