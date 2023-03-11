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

package org.apache.cassandra.utils.erasurecode.net;

import java.net.UnknownHostException;
import java.io.IOException;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import java.util.Random;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.ImmutableSet;




import static org.apache.cassandra.db.TypeSizes.sizeof;

public final class ECMessage {

    public static final Serializer serializer = new Serializer();
    final String    byteChunk;
    final long      k;
    final String    keyspace;
    final String    key;
    final String    table;

    public ECMessage(String byteChunk, long k, String keyspace, String table, String key) {
        this.byteChunk = byteChunk;
        this.k = k;
        this.keyspace = keyspace;
        this.key = key;
        this.table = table;
    }
    


    protected static Output output;
    private static InetAddressAndPort targetEndpoint = null;
    //public static final ECNetSend instance = new ECNetSend();
    
    private static final Logger logger = LoggerFactory.getLogger(ECNetSend.class);


    /**
     * This method sends selected sstables to parity nodes for EC/
     * 
     * @param byteChunk selected sstables
     * @param k number of parity nodes
     * @param ks keyspace name of sstables
     * @param table cf name of sstables
     * @param key one of the key in sstables
     * @throws UnknownHostException
     * 
     */
    /*
     * TODO List
     * 1. implement Verb.ERASURECODE_REQ
     * 2. implement responsehandler
     */
    public void sendSelectedSSTables() throws UnknownHostException {
        logger.debug("rymDebug: this is sendSelectedSSTables");

        // create a Message for byteChunk
        Message<ECMessage> message = null;
        // get target endpoints
        getTargetEdpoints(this);
        
        if(targetEndpoint != null) {
            logger.debug("target endpoints are : {}", targetEndpoint);
            // setup message
            message = Message.outWithFlag(Verb.ERASURECODE_REQ, this, MessageFlag.CALL_BACK_ON_FAILURE);
            logger.debug("rymDebug: This is dumped message: {}", message);
            MessagingService.instance().sendSSTContentWithoutCallback(message, targetEndpoint);
        } else {
            logger.debug("targetEndpoints is null");
        }
    }

    /*
     * Get target nodes, use the methods related to nodetool.java and status.java
     */
    public static void getTargetEdpoints(ECMessage ecMessage) throws UnknownHostException {
        
        logger.debug("rymDebug: this is getTargetEdpoints, keyspace is: {}, table name is: {}, key is {}",
        ecMessage.keyspace, ecMessage.table, ecMessage.key);
        
        Set<InetAddressAndPort> liveEndpoints = Gossiper.instance.getLiveMembers();
        List<InetAddressAndPort> endpoints = new ArrayList<>(liveEndpoints);
        Set<InetAddressAndPort> ringMembers = Gossiper.instance.getLiveTokenOwners();
        logger.debug("rymDebug: get All endpoints: {}, ring members is: {}", endpoints, ringMembers);
        List<String> naturalEndpoints = StorageService.instance.getNaturalEndpointsWithPort(ecMessage.keyspace, ecMessage.table, ecMessage.key);
        logger.debug("rymDebug: getTargetEdpoints.naturalEndpoints is {}", naturalEndpoints);
        
        for(String ep : naturalEndpoints) {
            endpoints.remove(InetAddressAndPort.getByName(ep));
        }
        logger.debug("rymDebug: candidates are {}", endpoints);

        // randomly select an endpoint    
        int rand = (new Random().nextInt(endpoints.size()));
        targetEndpoint = endpoints.get(rand);
    }




    public static final class Serializer implements IVersionedSerializer<ECMessage> {

        @Override
        public void serialize(ECMessage ecMessage, DataOutputPlus out, int version) throws IOException {
            // TODO: something may need to ensure, could be test
            out.writeUTF(ecMessage.byteChunk);
            out.writeLong(ecMessage.k);
        }

        @Override
        public ECMessage deserialize(DataInputPlus in, int version) throws IOException {
            String byteChunk = in.readUTF();
            long   k = in.readLong();
            return new ECMessage(byteChunk, k, null, null, null);
        }

        @Override
        public long serializedSize(ECMessage ecMessage, int version) {
            long size = sizeof(ecMessage.byteChunk) + sizeof(ecMessage.k);
            // + sizeof(ecMessage.keyspace) + sizeof(ecMessage.key) +  sizeof(ecMessage.table);

            return size;
            
        }
        
    }
    
}
