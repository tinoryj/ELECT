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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.erasurecode.net.ECNetutils.DecoratedKeyComparator;
import org.apache.cassandra.io.erasurecode.net.ECSyncSSTable.SSTablesInBytes;
import org.apache.cassandra.net.ForwardingInfo;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ECSyncSSTableVerbHandler implements IVerbHandler<ECSyncSSTable>{
    public static final ECSyncSSTableVerbHandler instance = new ECSyncSSTableVerbHandler();
    private static final Logger logger = LoggerFactory.getLogger(ECSyncSSTableVerbHandler.class);

    private static AtomicInteger GLOBAL_COUNTER = new AtomicInteger(0);



    public static class DataForRewrite {
        public final List<DecoratedKey> sourceKeys;
        // public final SSTablesInBytes sstInBytes;
        public String fileNamePrefix;

        public DataForRewrite(List<DecoratedKey> sourceKeys, String fileNamePrefix) {
            this.sourceKeys = sourceKeys;
            // this.sstInBytes = sstInBytes;
            this.fileNamePrefix = fileNamePrefix;
        }
    }

    @Override
    public void doVerb(Message<ECSyncSSTable> message) throws IOException {
        // logger.debug("rymDebug: this is ECSyncSSTableVerbHandler");
        // Check if there were any forwarding headers in this message
        ForwardingInfo forwardTo = message.forwardTo();
        if (forwardTo != null) {
            forwardToLocalNodes(message, forwardTo);
            // logger.debug("rymDebug: this is a forwarding header");
        }  

        // collect sstcontent
        List<String> allKey = message.payload.allKey;
        SSTablesInBytes sstInBytes = message.payload.sstInBytes;
        String cfName = message.payload.targetCfName;

        // Get all keys 
        List<DecoratedKey> sourceKeys = new ArrayList<DecoratedKey>();
        for(String key : allKey) {
            sourceKeys.add(StorageService.instance.getKeyFromPartition("ycsb", cfName, key));
        }
        // Collections.sort(sourceKeys, new DecoratedKeyComparator());

        // Get sstales in byte.
        // TODO: save the recieved data to a certain location based on the keyspace name and cf name
        String hostName = InetAddress.getLocalHost().getHostName();
        int fileCount = GLOBAL_COUNTER.getAndIncrement();
        String dataForRewriteDir = ECNetutils.getDataForRewriteDir();
        // the full name is user.dir/data/tmp/${HostName}-${COUNTER}-XXX.db
        String fileNamePrefix = hostName + "-" + String.valueOf(fileCount) + "-";
        String tmpFileName = dataForRewriteDir + fileNamePrefix;
        String filterFileName = tmpFileName + "Filter.db";
        ECNetutils.writeBytesToFile(filterFileName, sstInBytes.sstFilter);
        String indexFileName = tmpFileName + "Index.db";
        ECNetutils.writeBytesToFile(indexFileName, sstInBytes.sstIndex);
        String statsFileName = tmpFileName + "Statistics.db";
        ECNetutils.writeBytesToFile(statsFileName, sstInBytes.sstStats);
        String summaryFileName = tmpFileName + "Summary.db";
        ECNetutils.writeBytesToFile(summaryFileName, sstInBytes.sstSummary);

        StorageService.instance.globalSSTMap.putIfAbsent(message.payload.sstHashID, 
                                                         new DataForRewrite(sourceKeys, fileNamePrefix));
    


        // logger.debug("rymDebug: message is from {}, globalSSTMap size is {}, received key num is {}, targetCfName is {}, sstHash is {}", 
        //              message.from(),
        //              StorageService.instance.globalSSTMap.size(), 
        //              message.payload.allKey.size(),
        //              message.payload.targetCfName,
        //              message.payload.sstHashID);
    }


    private static void forwardToLocalNodes(Message<ECSyncSSTable> originalMessage, ForwardingInfo forwardTo) {
        Message.Builder<ECSyncSSTable> builder = Message.builder(originalMessage)
                .withParam(ParamType.RESPOND_TO, originalMessage.from())
                .withoutParam(ParamType.FORWARD_TO);

        boolean useSameMessageID = forwardTo.useSameMessageID(originalMessage.id());
        // reuse the same Message if all ids are identical (as they will be for 4.0+
        // node originated messages)
        Message<ECSyncSSTable> message = useSameMessageID ? builder.build() : null;

        forwardTo.forEach((id, target) -> {
            Tracing.trace("Enqueuing forwarded write to {}", target);
            MessagingService.instance().send(useSameMessageID ? message : builder.withId(id).build(), target);
        });
    }
    public static void main(String[] args) throws Exception {
        String hostName = InetAddress.getLocalHost().getHostName();
        logger.debug(hostName);
    }
}
