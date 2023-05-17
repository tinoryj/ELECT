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
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.erasurecode.ErasureCoderOptions;
import org.apache.cassandra.io.erasurecode.ErasureEncoder;
import org.apache.cassandra.io.erasurecode.NativeRSEncoder;
import org.apache.cassandra.io.erasurecode.net.ECMetadata.ECMetadataContent;
import org.apache.cassandra.io.erasurecode.net.ECParityUpdate.SSTableContentWithHashID;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ForwardingInfo;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;

public class ECParityUpdateVerbHandler implements IVerbHandler<ECParityUpdate> {
    public static final ECParityUpdateVerbHandler instance = new ECParityUpdateVerbHandler();
    private static final Logger logger = LoggerFactory.getLogger(ECMessage.class);


    /**
     * Receives update data from the primary node, and performs the following steps:
     * 1. First consume the new data list, perform parity update
     * 2. If new data is empty while the old data is not empty, add old data to the global queue
     */
    @Override
    public void doVerb(Message<ECParityUpdate> message) throws IOException {
        ForwardingInfo forwardTo = message.forwardTo();
        if (forwardTo != null) {
            forwardToLocalNodes(message, forwardTo);
            logger.debug("rymDebug: this is a forwarding header");
        }

        ECParityUpdate parityUpdateData = message.payload;
        List<InetAddressAndPort> parityNode = parityUpdateData.parityNodes;
        
        // Map<String, ByteBuffer[]> sstHashToParityCodeMap = new HashMap<String, ByteBuffer[]>();
        String localParityCodeDir = ECNetutils.getLocalParityCodeDir();

        // read parity code locally and from peer parity nodes
        // TODO: check that parity code blocks are all ready
        for (SSTableContentWithHashID sstContentWithHash: parityUpdateData.oldSSTables) {
            String sstHash = sstContentWithHash.sstHash;
            String stripID = StorageService.instance.globalSSTHashToStripID.get(sstHash);

            
            // read ec_metadata from memory, get the needed parity hash list
            List<String> parityHashList = StorageService.instance.globalECMetadataMap.get(stripID).parityHashList;
            ByteBuffer[] parityCodes = new ByteBuffer[parityHashList.size()];
            // get the needed parity code locally
            ByteBuffer localParityCode =
                 ByteBuffer.wrap(ECNetutils.readBytesFromFile(localParityCodeDir + parityHashList.get(0)));


            // TODO: check the length
            // if(localParityCode.capacity() > entry.getValue().capacity()) {
            //     oldSSTablesWithStripID.codeLength = localParityCode.capacity();
            //     // padding zero for new sst content
            // } else {
            //     oldSSTablesWithStripID.codeLength = oldSSTablesWithStripID.sstContent.capacity();
            // }

            
            for(int i = 0; i < parityHashList.size(); i++) {
                parityCodes[i] = ByteBuffer.allocate(localParityCode.capacity());
            }
            parityCodes[0].put(localParityCode);
            parityCodes[0].rewind();

            StorageService.instance.globalSSTHashToParityCodeMap.put(sstHash, parityCodes);
            
            // get the needed parity code remotely, send a parity code request
            for (int i = 1; i < parityHashList.size(); i++) {
                ECRequestParity request = new ECRequestParity(parityHashList.get(i), sstHash, i);
                request.requestParityCode(parityNode.get(i));
            }            
        }

        // consume the new data
        for (SSTableContentWithHashID newSSTable: parityUpdateData.newSSTables) {
            ByteBuffer newData = newSSTable.sstContent;
            // OldSSTablesWithStripID obj = parityUpdateData.oldSSTablesWithStripIDs.get(parityUpdateData.oldSSTablesWithStripIDs.size() - 1);
            SSTableContentWithHashID oldSSTable = parityUpdateData.oldSSTables.get(0);
            Stage.ERASURECODE.maybeExecuteImmediately(new ErasureCodeUpdateRunnable(newData,
                                                                                    StorageService.instance.globalSSTHashToParityCodeMap.get(oldSSTable.sstHash),
                                                                                    StorageService.instance.globalECMetadataMap.get(oldSSTable.sstHash).sstHashIdList.indexOf(oldSSTable.sstHash), 
                                                                                    oldSSTable.sstContentSize));

            // TODO: remove the processed entry
        }
        
    }

    private static void forwardToLocalNodes(Message<ECParityUpdate> originalMessage, ForwardingInfo forwardTo) {
        Message.Builder<ECParityUpdate> builder = Message.builder(originalMessage)
                .withParam(ParamType.RESPOND_TO, originalMessage.from())
                .withoutParam(ParamType.FORWARD_TO);

        boolean useSameMessageID = forwardTo.useSameMessageID(originalMessage.id());
        // reuse the same Message if all ids are identical (as they will be for 4.0+
        // node originated messages)
        Message<ECParityUpdate> message = useSameMessageID ? builder.build() : null;

        forwardTo.forEach((id, target) -> {
            Tracing.trace("Enqueuing forwarded write to {}", target);
            MessagingService.instance().send(useSameMessageID ? message : builder.withId(id).build(), target);
        });
    }


    
    /** [CASSANDRAEC]
     * To support perform erasure coding with multiple threads, we implement the following Runnable class
     * @param ecDataNum the value of k
     * @param ecParity the value of m
     * @param messages the input data to be processed, length equal to ecDataNum
     */
    private static  class ErasureCodeUpdateRunnable implements Runnable {
        private final int ecDataNum = DatabaseDescriptor.getEcDataNodes();
        private final int ecParityNum = DatabaseDescriptor.getParityNodes();
        private final ByteBuffer newSSTContent;
        private final ByteBuffer[] parityCodes;
        private final int targetDataIndex;
        private final int codeLength;

        ErasureCodeUpdateRunnable(ByteBuffer newSSTContent, ByteBuffer[] oldParityCodes, int targetDataIndex, int codeLength) {
            this.newSSTContent = newSSTContent;
            this.parityCodes = oldParityCodes;
            this.targetDataIndex = targetDataIndex;
            this.codeLength = codeLength;
        }

        @Override
        public void run() {
            

            ErasureCoderOptions ecOptions = new ErasureCoderOptions(ecDataNum, ecParityNum);
            ErasureEncoder encoder = new NativeRSEncoder(ecOptions);

            logger.debug("rymDebug: let's start computing erasure coding");

            // Encoding input and output
            ByteBuffer[] newData = new ByteBuffer[1];

            // Prepare input data
            newData[0] = ByteBuffer.allocateDirect(codeLength);
            newData[0].put(newSSTContent);
            int remaining = newData[0].remaining();
            if(remaining>0) {
                byte[] zeros = new byte[remaining];
                newData[0].put(zeros);
            }
            newData[0].rewind();


            // Encode
            try {
                encoder.encodeUpdate(newData, parityCodes, targetDataIndex);
            } catch (IOException e) {
                logger.error("rymERROR: Perform erasure code error", e);
            }

            
            // generate parity hash code
            List<String> parityHashList = new ArrayList<String>();
            for(ByteBuffer parityCode : parityCodes) {
                parityHashList.add(ECNetutils.stringToHex(String.valueOf(parityCode.hashCode())));
            }

            // record first parity code to current node
            try {
                String localParityCodeDir = ECNetutils.getLocalParityCodeDir();
                FileChannel fileChannel = FileChannel.open(Paths.get(localParityCodeDir, parityHashList.get(0)),
                                                            StandardOpenOption.WRITE,
                                                             StandardOpenOption.CREATE);
                fileChannel.write(parityCodes[0]);
                fileChannel.close();
                // logger.debug("rymDebug: parity code file created: {}", parityCodeFile.getName());
            } catch (IOException e) {
                logger.error("rymERROR: Perform erasure code error", e);
            }

            // TODO: delete local parity code file


            // // sync encoded data to parity nodes
            // ECParityNode ecParityNode = new ECParityNode(null, null, 0);
            // ecParityNode.distributeCodedDataToParityNodes(parityCodes, messages[0].parityNodes, parityHashList);

            // // Transform to ECMetadata and dispatch to related nodes
            // // ECMetadata ecMetadata = new ECMetadata("", "", "", new ArrayList<String>(),new ArrayList<String>(),
            // //             new ArrayList<InetAddressAndPort>(), new HashSet<InetAddressAndPort>(), new HashMap<String, List<InetAddressAndPort>>());
            // ECMetadata ecMetadata = new ECMetadata("", new ECMetadataContent("", "", new ArrayList<String>(),new ArrayList<String>(),
            //                                        new ArrayList<InetAddressAndPort>(), new HashSet<InetAddressAndPort>(), new ArrayList<InetAddressAndPort>(),
            //                                     new HashMap<String, List<InetAddressAndPort>>()));
            // ecMetadata.generateMetadata(messages, parity, parityHashList);
        }

    }
    
}
