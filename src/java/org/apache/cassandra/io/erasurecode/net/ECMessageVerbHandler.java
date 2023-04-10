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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.io.erasurecode.ErasureCoderOptions;
import org.apache.cassandra.io.erasurecode.ErasureEncoder;
import org.apache.cassandra.io.erasurecode.NativeRSEncoder;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ForwardingInfo;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.tracing.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.unix.Buffer;

public class ECMessageVerbHandler implements IVerbHandler<ECMessage> {

    public static final ECMessageVerbHandler instance = new ECMessageVerbHandler();
    private static final Logger logger = LoggerFactory.getLogger(ECMessage.class);
    private static final String parityCodeDir = System.getProperty("user.dir")+"/data/parityHashes/";

    private static ConcurrentHashMap<InetAddressAndPort, Queue<ECMessage>>recvQueues = new ConcurrentHashMap<InetAddressAndPort, Queue<ECMessage>>();

    private void respond(Message<?> respondTo, InetAddressAndPort respondToAddress) {
        Tracing.trace("Enqueuing response to {}", respondToAddress);
        MessagingService.instance().send(respondTo.emptyResponse(), respondToAddress);
    }

    private void failed() {
        Tracing.trace("Payload application resulted in ECTimeout, not replying");
    }

    /*
     * TODO list:
     * 1. Collect k SST contents;
     * 2. Compute erasure coding locally;
     * 3. Send code to another parity nodes
     */
    @Override
    public void doVerb(Message<ECMessage> message) throws IOException {
        ByteBuffer sstContent = message.payload.sstContent;
        int ec_data_num = message.payload.ecDataNum;

        
        for (String ep : message.payload.parityNodesString.split(",")) {
            message.payload.parityNodes.add(InetAddressAndPort.getByName(ep.substring(1)));
        }

        logger.debug("rymDebug: get new message!!! message is from: {}, primaryNode is {}, parityNodes is {}",
         message.from(), message.payload.replicaNodes.get(0), message.payload.parityNodes);

        // check if there were any forwarding headers in this message
        ForwardingInfo forwardTo = message.forwardTo();
        if (forwardTo != null)
            forwardToLocalNodes(message, forwardTo);

        InetAddressAndPort primaryNode = message.payload.replicaNodes.get(0);
        // Once we have k different sstContent, do erasure coding locally
        if(!recvQueues.containsKey(primaryNode)) {
            Queue<ECMessage> recvQueue = new LinkedList<ECMessage>();
            recvQueue.add(message.payload);
            recvQueues.put(primaryNode, recvQueue);
        }
        else {
            recvQueues.get(primaryNode).add(message.payload);
        }
        
        logger.debug("rymDebug: recvQueues is {}", recvQueues);

        if(recvQueues.size()>=message.payload.ecDataNum) {
            logger.debug("rymDebug: sstContents are enough to do erasure coding: recvQueues is {}", recvQueues);
            ECMessage tmpArray[] = new ECMessage[message.payload.ecDataNum];
            //traverse the recvQueues
            int i = 0;
            for (InetAddressAndPort msg : recvQueues.keySet()) {
                tmpArray[i] = recvQueues.get(msg).poll();
                if(recvQueues.get(msg).size() == 0) {
                    recvQueues.remove(msg);
                }
                if (i == message.payload.ecDataNum - 1) 
                    break;
                i++;
            }
            // compute erasure coding locally;
            Stage.ERASURECODE.maybeExecuteImmediately(new ErasureCodeRunnable(tmpArray));
        }

        Tracing.trace("recieved sstContent is: {}, ec_data_num is: {}, sourceEdpoint is: {}, header is: {}",
                sstContent, ec_data_num, message.from(), message.header);
    }

    private static void forwardToLocalNodes(Message<ECMessage> originalMessage, ForwardingInfo forwardTo) {
        Message.Builder<ECMessage> builder = Message.builder(originalMessage)
                .withParam(ParamType.RESPOND_TO, originalMessage.from())
                .withoutParam(ParamType.FORWARD_TO);

        boolean useSameMessageID = forwardTo.useSameMessageID(originalMessage.id());
        // reuse the same Message if all ids are identical (as they will be for 4.0+
        // node originated messages)
        Message<ECMessage> message = useSameMessageID ? builder.build() : null;

        forwardTo.forEach((id, target) -> {
            Tracing.trace("Enqueuing forwarded write to {}", target);
            MessagingService.instance().send(useSameMessageID ? message : builder.withId(id).build(), target);
        });
    }

    


    private static  class ErasureCodeRunnable implements Runnable {
        private final int ecDataNum;
        private final int ecParityNum;
        private final ECMessage[] messages;

        ErasureCodeRunnable(ECMessage[] message) {
            this.ecDataNum = message[0].ecDataNum;
            this.ecParityNum = message[0].ecParityNum;
            this.messages = message;
        }

        @Override
        public void run() {
            if(messages.length != ecDataNum) {
                logger.error("rymError: message length is not equal to ecDataNum");
            }
            int codeLength = messages[0].sstSize;
            for (ECMessage msg : messages) {
                codeLength = codeLength < msg.sstSize? msg.sstSize : codeLength;
             }
            ErasureCoderOptions ecOptions = new ErasureCoderOptions(ecDataNum, ecParityNum);
            ErasureEncoder encoder = new NativeRSEncoder(ecOptions);

            logger.debug("rymDebug: let's start computing erasure coding");

            // Encoding input and output
            ByteBuffer[] data = new ByteBuffer[ecDataNum];
            ByteBuffer[] parity = new ByteBuffer[ecParityNum];

            // Prepare input data
            for (int i = 0; i < messages.length; i++) {
                data[i] = ByteBuffer.allocateDirect(codeLength);
                // data[i].put((byte)0);
                // data[i].flip();
                // data[i].position(data[i].limit());
                // data[i].limit(data[i].capacity());
                data[i].put(messages[i].sstContent);
                logger.debug("rymDebug: remaining data is {}, codeLength is {}", data[i].remaining(), codeLength);
                int remaining = data[i].remaining();
                if(remaining>0) {
                    byte[] zeros = new byte[remaining];
                    data[i].put(zeros);
                }
                // data[i].put(new byte[data[i].remaining()]);
                logger.debug("rymDebug: message[{}].sstconetent {}, data[{}] is: {}",
                 i, messages[i].sstContent,i, data[i]);
                data[i].rewind();
            }

            // Prepare parity data
            for (int i = 0; i < ecParityNum; i++) {
                parity[i] = ByteBuffer.allocateDirect(codeLength);
            }

            // Encode
            try {
                encoder.encode(data, parity);
            } catch (IOException e) {
                logger.error("rymError: Perform erasure code error", e);
            }

            
            // generate parity hash code
            List<String> parityHashCode = new ArrayList<String>();
            for(ByteBuffer parityCode : parity) {
                parityHashCode.add(String.valueOf(parityCode.hashCode()));
            }

            // record first parity code to current node
            try {
                
                FileChannel fileChannel = FileChannel.open(Paths.get(parityCodeDir, parityHashCode.get(0)),
                                                            StandardOpenOption.WRITE,
                                                             StandardOpenOption.CREATE);
                fileChannel.write(parity[0]);
                fileChannel.close();
                // File parityCodeFile = new File(parityCodeDir + parityHashCode.get(0));
                // if (!parityCodeFile.exists()) {
                //     parityCodeFile.createNewFile();
                // }
                // FileWriter fileWritter = new FileWriter(parityCodeFile.getAbsolutePath(),true);
                // fileWritter.write(parity[0].toString());
                // fileWritter.close();
                // logger.debug("rymDebug: parity code file created: {}", parityCodeFile.getName());
            } catch (IOException e) {
                logger.error("rymError: Perform erasure code error", e);
            }


            // sync encoded data to parity nodes
            ECParityNode.instance.distributeEcDataToParityNodes(parity, messages[0].parityNodes, parityHashCode);

            // Transform to ECMetadata and dispatch to related nodes
            ECMetadata.instance.generateMetadata(messages, parity, parityHashCode);
        }

    }


    public static void main(String[] args) {
        int codeLength = 1000;
        ByteBuffer data = ByteBuffer.allocateDirect(codeLength);
        ByteBuffer src = ByteBuffer.allocateDirect(900);
        data.put(src);
        int remaining = codeLength - data.remaining();
        logger.debug("rymDebug: remaining: {}, data.remaining {}", remaining, data.remaining());
        if (data.remaining() > 0) {
            byte[] zeros = new byte[data.remaining()];
            data.put(zeros);
        }
        logger.debug("rymDebug: remaining: {}, capacity is {}, position is {}",
                    data.remaining(),data.capacity(),data.position());

        byte[] bb = { 10, 20, 30 };
        ByteBuffer byteBuffer = ByteBuffer.wrap(bb);
        logger.debug("rymDebug: remaining element is {}, capacity is {}, position is {}", 
                    byteBuffer.remaining(), byteBuffer.capacity(), byteBuffer.position());

    }



}
