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
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECCompactionVerbHandler implements IVerbHandler<ECCompaction> {
    /*
     * Secondary nodes receive compaction signal from primary nodes and trigger
     * compaction
     */
    
    private static final Logger logger = LoggerFactory.getLogger(ECCompaction.class);
    public static final ECCompactionVerbHandler instance = new ECCompactionVerbHandler();
    @Override
    public void doVerb(Message<ECCompaction> message) throws IOException {
        String sstHash = message.payload.sstHash;
        String ksName = message.payload.ksName;
        String startToken = message.payload.startToken;
        String endToken = message.payload.endToken;
        InetAddress localAddress = FBUtilities.getJustBroadcastAddress();
        List<InetAddress> replicaNodes = StorageService.instance.getNaturalEndpointsForToken(ksName, startToken);
        logger.debug("rymDebug: compaction handler, localAddress is {}, replicaNodes is {}", localAddress, replicaNodes);
        int index = replicaNodes.indexOf(localAddress);
        String cfName = message.payload.cfName + String.valueOf(index);
        logger.debug("rymDebug: Received compaction request for {}/{}/{}/{}",
         sstHash, ksName, message.payload.cfName, String.valueOf(index));
        
        

        //TODO: get sstContent and do compaction
        ColumnFamilyStore cfs = Keyspace.open(ksName).getColumnFamilyStore(cfName);
        
        Collection<Range<Token>> tokenRanges = StorageService.instance.createRepairRangeFrom(startToken, endToken);
        try {
            cfs.forceCompactionForTokenRange(tokenRanges);
        } catch (ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
