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

import java.io.FileNotFoundException;
import java.io.IOError;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.INodeProbeFactory;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.Output;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.SetHostStatWithPort;
import org.apache.cassandra.utils.FBUtilities;

import java.util.SortedMap;
import org.tartarus.snowball.ext.porterStemmer;

import java.util.Map.Entry;
import com.google.common.base.Throwables;

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
    private static SortedMap<String, SortedMap<String, InetAddress>> dcs;

    private static boolean isTokenPerNode = true;

    /*
     * Send selected sstables to a specific node
     */
    public static void sendSelectedSSTables(ByteBuffer byteChunk, int k) {

        // target node is in local datacenter
        Collection<NodeInfo> localDC = null;
        // no-local datacenter, grouped by dc
        Map<String, Collection<NodeInfo>> dcGroups = null;
        // create a Message for byteChunk
        Message<ByteBuffer> message = null;

    }

    /*
     * Get target nodes, use the methods related to nodetool.java and status.java
     */

    private static List<InetAddress> getTargetNodes(int k, int rf, String token) {
        List<InetAddress> targetEndpoints;

        try (NodeProbe probe = connect()) {
            targetEndpoints = execute(probe, k, rf, token);
            if (probe.isFailed())
                throw new RuntimeException("nodetool failed, check server logs");

        } catch (Exception e) {
            throw new RuntimeException("Error while closing JMX connection", e);
        }

        return targetEndpoints;
    }

    private static NodeProbe connect() {
        NodeProbe nodeClient = null;
        try {
            nodeClient = nodeProbeFactory.create(host, parseInt(port));
            nodeClient.setOutput(output);
        } catch (IOException | SecurityException e) {
            Throwable rootCause = Throwables.getRootCause(e);
            output.err.println(format("nodetool: Failed to connect to '%s:%s' - %s: '%s'.", host, port,
                    rootCause.getClass().getSimpleName(), rootCause.getMessage()));
            System.exit(1);
        }
        return nodeClient;
    }

    protected static List<InetAddress> execute(NodeProbe probe, int k, int rf, String token)
            throws UnknownHostException {
        tokensToEndpoints = probe.getTokenToEndpointMap(true);
        dcs = NodeTool.getEndpointByDcWithPort(probe, tokensToEndpoints);
        // More tokens than nodes (aka vnodes)?
        if (dcs.size() < tokensToEndpoints.size())
            isTokenPerNode = false;

        // get current dc, primary node token, replication factor, and parity nodes
        // number
        EndpointSnitchInfoMBean epSnitchInfo = probe.getEndpointSnitchInfoProxy();
        // InetAddress localIp = FBUtilities.getJustBroadcastAddress();
        String localdc = epSnitchInfo.getDatacenter();

        /*
         * Select target nodes, first get the node list sorted by token of local dc,
         * then get target nodes according to rf and parity nodes number.
         */
        SortedMap<String, InetAddress> tokenToEndpointsSortedMap = dcs.get(localdc);

        // get replication node
        String ks = "";
        String table = "";
        String key = "";
        List<InetAddress> replicaEndpoints = probe.getEndpoints(key, table, key);
        List<InetAddress> candidates = new ArrayList(tokenToEndpointsSortedMap.values());
        candidates.removeAll(replicaEndpoints);

        int randArr[] = new int[k];
        int i = 0;
        while (i < k) {
            int rand = (new Random().nextInt(candidates.size()) + 1);
            boolean isRandExist = false;
            for (int j = 0; j < randArr.length; j++) {
                if (randArr[j] == rand) {
                    isRandExist = true;
                    break;
                }
            }
            if (isRandExist == false) {
                randArr[i] = rand;
                i++;
            }
        }

        List<InetAddress> results = new ArrayList<>();
        for(int t=0;t<k;t++) {
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