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
    private static void getTargetEdpoints(ECMessage ecMessage) throws UnknownHostException {
        
        ImmutableSet<InetAddressAndPort> immutableEndpoints = Gossiper.instance.getEndpoints();
        List<String> naturalEndpoints = StorageService.instance.getNaturalEndpointsWithPort(ecMessage.keyspace, ecMessage.table, ecMessage.key);
        List<InetAddressAndPort> endpoints = new ArrayList<>(immutableEndpoints);

        
        logger.debug("rymDebug: get All endpoints: {}, and replica related endpoints: {}", endpoints, naturalEndpoints);

        
        for(String ep : naturalEndpoints) {
            endpoints.remove(InetAddressAndPort.getByName(ep));
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

    // private static void getTargetEdpoints(long k, String ks, String table, String key) {
    //     logger.debug("This is getTargetEndpoints!");
    //     try (NodeProbe probe = connect()) {
    //         logger.debug("Already get a probe client!");
    //         targetEndpoints = execute(probe, k, ks, table, key);
    //         if (probe.isFailed())
    //             throw new RuntimeException("nodetool failed, check server logs");

    //     } catch (Exception e) {
    //         throw new RuntimeException("Error while closing JMX connection", e);
    //     }
    // }

    // private static NodeProbe connect() {
    //     logger.debug("rymDebug: start connect()");
    //     NodeProbe nodeClient = null;
    //     try {
    //         nodeClient = nodeProbeFactory.create(host, parseInt(port));
    //         nodeClient.setOutput(output);
    //     } catch (IOException | SecurityException e) {
    //         Throwable rootCause = Throwables.getRootCause(e);
    //         output.err.println(format("nodetool: Failed to connect to '%s:%s' - %s: '%s'.", host, port,
    //                 rootCause.getClass().getSimpleName(), rootCause.getMessage()));
    //         System.exit(1);
    //     }
    //     logger.debug("rymDebug: successfully connected!");
    //     return nodeClient;
    // }

    protected static List<InetAddressAndPort> execute(NodeProbe probe, long k, String ks, String table, String key)
            throws UnknownHostException {
        logger.debug("rymDebug: this is execute");
        tokensToEndpoints = probe.getTokenToEndpointMap(true);
        dcs = NodeTool.getEndpointByDcWithPort(probe, tokensToEndpoints);
        // More tokens than nodes (aka vnodes)?
        // if (dcs.size() < tokensToEndpoints.size())
        //     isTokenPerNode = false;
        logger.debug("rymDebug: get tokensToEndpointsMap:{} and dcs: {}", tokensToEndpoints, dcs);
      
        EndpointSnitchInfoMBean epSnitchInfo = probe.getEndpointSnitchInfoProxy();
        // InetAddress localIp = FBUtilities.getJustBroadcastAddress();
        String localdc = epSnitchInfo.getDatacenter();

        /*
         * Select target nodes, first get the node list sorted by token of local dc,
         * then get target nodes according to rf and parity nodes number.
         */
        SortedMap<String, InetAddressAndPort> tokenToEndpointsSortedMap = dcs.get(localdc);

        // get replication node
        List<String> replicaEndpoints = probe.getEndpointsWithPort(key, table, key);
        List<InetAddressAndPort> candidates = new ArrayList(tokenToEndpointsSortedMap.values());
        //candidates.removeAll(replicaEndpoints);
        
        for(String ep : replicaEndpoints) {
            candidates.remove(InetAddressAndPort.getByName(ep));
        }
        logger.debug("rymDebug: candidates are {}", candidates);

        int randArr[] = new int[(int) k];
        int t = 0;
        while (t < k) {
            int rand = (new Random().nextInt(candidates.size()) + 1);
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

        List<InetAddressAndPort> results = new ArrayList<>();
        for(int i=0;i<k;i++) {
            results.add(candidates.get(randArr[i]-1));
        }
        return results;

        // int sz = tokenToEndpointsSortedMap.size();
        // for (Entry<String, String> tokenToEndpoint :
        // tokenToEndpointsSortedMap.entrySet()) {
        // if(tokenToEndpoint.getKey() != token)
        // cnt++;
        // else
        // break;
        // }

        // if(cnt+rf<sz && cnt+sz+k<=sz) {
        // // select nodes from [cnt+rf-1, cnt+rf+k-1]

        // } else if (cnt+rf<sz && cnt+sz+k>sz) {
        // // select nodes from [cnt+rf-1, sz-1] and [0, k+rf-sz-1]
        // } else {
        // // select nodes from [0, ]
        // }
    }

}