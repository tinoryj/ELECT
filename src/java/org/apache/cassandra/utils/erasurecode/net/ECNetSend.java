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

import java.io.IOException;
import java.net.UnknownHostException;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.INodeProbeFactory;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedMap;
import java.util.logging.LogRecord;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;

import ch.qos.logback.classic.selector.servlet.LoggerContextFilter;



/*
 * Inter-node communication for selected SSTables during redundancy
 * transition.
 */

public class ECNetSend {
    private static final String host = "127.0.0.1";
    private static final String port = "7199";
    protected static Output output;
    private static INodeProbeFactory nodeProbeFactory;
    private static Map<String, String> tokensToEndpoints;
    private static SortedMap<String, SortedMap<String, InetAddressAndPort>> dcs;
    private static List<InetAddressAndPort> targetEndpoints = null;
    
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
    public static void sendSelectedSSTables(ECMessage ecMessage) throws UnknownHostException {
        logger.debug("rymDebug: this is sendSelectedSSTables");

        // create a Message for byteChunk
        Message<ECMessage> message = null;
        // get target endpoints
        // getTargetEdpoints(ecMessage.k, ecMessage.keyspace, ecMessage.table, ecMessage.key);
        // targetEndpoints.add(InetAddressAndPort.getByName("172.31.7.104"));
        
        getTargetEdpoints(ecMessage);
        

        if(targetEndpoints != null) {
            logger.debug("target endpoints are : {}", targetEndpoints);
            // setup message
            message = Message.outWithFlag(Verb.ERASURECODE_REQ, ecMessage, MessageFlag.CALL_BACK_ON_FAILURE);

            for (InetAddressAndPort ep : targetEndpoints) {
                // isAlive?
                // send to destination
                MessagingService.instance().sendSSTContentWithoutCallback(message, ep);
            }

        } else {
            logger.debug("targetEndpoints is null");
        }

    }

    /*
     * Get target nodes, use the methods related to nodetool.java and status.java
     */
    private static void getTargetEdpoints(ECMessage ecMessage) {
        
        logger.debug("rymDebug: this is getTargetEdpoints, keyspace is: {}, table name is: {}, key is {}",
        ecMessage.keyspace, ecMessage.table, ecMessage.key);
        
        ImmutableSet<InetAddressAndPort> immutableEndpoints = Gossiper.instance.getEndpoints();
        List<InetAddressAndPort> endpoints = new ArrayList<>(immutableEndpoints);
        logger.debug("rymDebug: get All endpoints: {}", endpoints);
        List<String> naturalEndpoints = StorageService.instance.getNaturalEndpointsWithPort(ecMessage.keyspace, ecMessage.table, ecMessage.key);
        logger.debug("and replica related endpoints: {}", naturalEndpoints);

        
        for(String ep : naturalEndpoints) {
            try {
                endpoints.remove(InetAddressAndPort.getByName(ep));
            } catch (UnknownHostException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        logger.debug("rymDebug: candidates are {}", endpoints);
        
        int k = (int) ecMessage.k;
        int randArr[] = new int[k];
        int t = 0;
        while (t < k) {
            int rand = (new Random().nextInt(endpoints.size()) + 1);
            boolean isRandExist = false;
            for (int j = 0; j < randArr.length; j++) {
                if (randArr[j] == rand) {
                    isRandExist = true;
                    break;
                }
            }
            if (isRandExist == false) {
                randArr[t] = rand;
                t++;
            }
        }

        for(int i=0;i<k;i++) {
            targetEndpoints.add(endpoints.get(randArr[i]-1));
        }
    }

}