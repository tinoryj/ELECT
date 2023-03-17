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
import java.io.IOException;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import java.util.Random;
import java.util.Set;
import java.util.ArrayList;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.cassandra.db.TypeSizes.sizeof;

public final class ECMessage {

    public static final Serializer serializer = new Serializer();
    final String sstContent;
    final String keyspace;
    final String key;
    final String table;
    final int k;
    final int rf;
    final int m;
    final InetAddressAndPort primaryNode;
    List<InetAddressAndPort> secondaryNodes = null;
    List<InetAddressAndPort> parityNodes = null;
    private static int GLOBAL_COUNTER = 0;
    

    public ECMessage(String sstContent, String keyspace, String table, String key) {
        this.sstContent = sstContent;
        this.keyspace = keyspace;
        this.table = table;
        this.key = key;
        this.k = DatabaseDescriptor.getEcDataNodes();
        this.m = DatabaseDescriptor.getParityNodes();
        this.rf = Keyspace.open(keyspace).getReplicationStrategy().getReplicationFactor().allReplicas;
        this.primaryNode = FBUtilities.getBroadcastAddressAndPort();
    }

    protected static Output output;
    private static final Logger logger = LoggerFactory.getLogger(ECMessage.class);

    /**
     * This method sends selected sstables to parity nodes for EC/
     * 
     * @param sstContent selected sstables
     * @param k         number of parity nodes
     * @param ks        keyspace name of sstables
     * @param table     cf name of sstables
     * @param key       one of the key in sstables
     * @throws UnknownHostException
     *                              TODO List
     *                              1. implement Verb.ERASURECODE_REQ
     *                              2. implement responsehandler
     */
    public void sendSelectedSSTables() throws UnknownHostException {
        logger.debug("rymDebug: this is sendSelectedSSTables");

        // create a Message for sstContent
        Message<ECMessage> message = null;
        GLOBAL_COUNTER++;
        // get target endpoints
        getTargetEdpoints(this);

        if (this.parityNodes != null) {
            logger.debug("target endpoints are : {}", this.parityNodes);
            // setup message
            message = Message.outWithFlag(Verb.ERASURECODE_REQ, this, MessageFlag.CALL_BACK_ON_FAILURE);
            logger.debug("rymDebug: This is dumped message: {}", message);
            MessagingService.instance().sendSSTContentWithoutCallback(message, this.parityNodes.get(0));
        } else {
            logger.debug("targetEndpoints is null");
        }
    }

    /*
     * Get target nodes, use the methods related to nodetool.java and status.java
     */
    protected static void getTargetEdpoints(ECMessage ecMessage) throws UnknownHostException {

        logger.debug("rymDebug: this is getTargetEdpoints, keyspace is: {}, table name is: {}, key is {}",
                ecMessage.keyspace, ecMessage.table, ecMessage.key);
        // get all live nodes
        List<InetAddressAndPort> liveEndpoints = new ArrayList<>(Gossiper.instance.getLiveMembers());
        // get replication nodes for given keyspace and table
        List<String> replicationEndpoints = StorageService.instance.getNaturalEndpointsWithPort(ecMessage.keyspace,
                ecMessage.table, ecMessage.key);
        logger.debug("rymDebug: getTargetEdpoints.replicationEndpoints is {}", replicationEndpoints);
        

        for (String rep : replicationEndpoints) {
            InetAddressAndPort ep = InetAddressAndPort.getByName(rep);
            if(!ep.equals(ecMessage.primaryNode)) {
                ecMessage.secondaryNodes.add(ep);
            }
        }
        
        logger.debug("rymDebug: candidates are {}", liveEndpoints);
        
        // select parity nodes from live nodes, suppose all nodes work healthy
        int n = liveEndpoints.size();
        int primaryNodeIndex = liveEndpoints.indexOf(ecMessage.primaryNode);
        int startIndex = ((primaryNodeIndex + n - (GLOBAL_COUNTER % ecMessage.k +1))%n);
        for (int i = startIndex; i < ecMessage.m+startIndex; i++) {
            ecMessage.parityNodes.add(liveEndpoints.get(i%n));
        }
    }

    public static final class Serializer implements IVersionedSerializer<ECMessage> {

        @Override
        public void serialize(ECMessage ecMessage, DataOutputPlus out, int version) throws IOException {
            // TODO: something may need to ensure, could be test
            out.writeUTF(ecMessage.sstContent);
            out.writeLong(ecMessage.k);
        }

        @Override
        public ECMessage deserialize(DataInputPlus in, int version) throws IOException {
            String sstContent = in.readUTF();
            return new ECMessage(sstContent, null, null, null);
        }

        @Override
        public long serializedSize(ECMessage ecMessage, int version) {
            long size = sizeof(ecMessage.sstContent) + sizeof(ecMessage.k) + sizeof(ecMessage.rf) + sizeof(ecMessage.m) + 
            sizeof(ecMessage.keyspace) + sizeof(ecMessage.table) + sizeof(ecMessage.key);
            // + sizeof(ecMessage.keyspace) + sizeof(ecMessage.key) +
            // sizeof(ecMessage.table);

            return size;

        }

    }

}
