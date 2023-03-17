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
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

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
import org.checkerframework.checker.units.qual.C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECMessageVerbHandler implements IVerbHandler<ECMessage> {

    public static final ECMessageVerbHandler instance = new ECMessageVerbHandler();
    private static final Logger logger = LoggerFactory.getLogger(ECMessage.class);
    private static final int MAX_RECV_QUEUE_SIZE = 100;
    private static LinkedBlockingQueue<ECMessage> recvQueue = new LinkedBlockingQueue<>(MAX_RECV_QUEUE_SIZE);
    private static final String parityCodeDir = System.getProperty("user.dir")+"/data/";

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
        String sstContent = message.payload.sstContent;
        long k = message.payload.k;

        logger.debug("rymDebug: get new message!!! sstContent is {}, k is {}, m is {}, replicationEndpoints is {}, parityNode is {}",
         sstContent, k, message.payload.m, message.payload.replicationEndpoints,message.payload.parityNodes);

        // check if there were any forwarding headers in this message
        ForwardingInfo forwardTo = message.forwardTo();
        if (forwardTo != null)
            forwardToLocalNodes(message, forwardTo);

        // InetAddressAndPort respondToAddress = message.respondTo();
        // ToDo: collect k SST contents;
        try {
            recvQueue.put(message.payload);
            logger.debug("recieved sstContent is: {}, k is: {}, sourceEdpoint is: {}", sstContent, k, message.from());
            if (recvQueue.size() >= message.payload.rf) {
                ECMessage tmpArray[] = new ECMessage[message.payload.rf];
                for (int i = 0; i < message.payload.rf; i++) {
                    tmpArray[i] = recvQueue.take();
                }
                // compute erasure coding locally;
                Stage.ERASURECODE.maybeExecuteImmediately(new ErasureCodeRunnable(tmpArray));
            }

        } catch (InterruptedException  e) {
            e.printStackTrace();
        }

        Tracing.trace("recieved sstContent is: {}, k is: {}, sourceEdpoint is: {}, header is: {}",
                sstContent, k, message.from(), message.header);
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
        private final int replicaFactor;
        private final int k;
        private final ECMessage[] messages;

        ErasureCodeRunnable(ECMessage[] message) {
            this.replicaFactor = message[0].rf;
            this.k = message[0].k;
            this.messages = message;
        }

        @Override
        public void run() {
            int codeLength = messages[0].sstContent.getBytes().length;
            ErasureCoderOptions ecOptions = new ErasureCoderOptions(replicaFactor, k);
            ErasureEncoder encoder = new NativeRSEncoder(ecOptions);

            logger.debug("rymDebug: let's start computing erasure coding");

            // Encoding input and output
            ByteBuffer[] data = new ByteBuffer[replicaFactor];
            ByteBuffer[] parity = new ByteBuffer[k];

            // Prepare input data
            for (int i = 0; i < messages.length; i++) {
                data[i] = ByteBuffer.allocateDirect(codeLength);
                data[i].put(messages[i].sstContent.getBytes());
                data[i].rewind();
            }

            // Prepare parity data
            for (int i = 0; i < k; i++) {
                parity[i] = ByteBuffer.allocateDirect(codeLength);
            }

            // Encode
            try {
                encoder.encode(data, parity);
            } catch (IOException e) {
                logger.error("rymError: Perform erasure code error", e);
            }

            // record first parity code to current node
            int parityHashCode = parity[0].hashCode();
            String parityCode = parity[0].toString();
            try {
                File parityCodeFile = new File(parityCodeDir + String.valueOf(parityHashCode));
                if (!parityCodeFile.exists()) {
                    parityCodeFile.createNewFile();
                }
                FileWriter fileWritter = new FileWriter(parityCodeFile.getName(),true);
                fileWritter.write(parityCode);
                fileWritter.close();
            } catch (IOException e) {
                logger.error("rymError: Perform erasure code error", e);
            }

            // sync encoded data to parity nodes, need callbacks
            




            // Transform to ECMetadata
            ECMetadata ecMetadata = ECMetadata.instance.generateMetadata(messages);




            // distribute ec metadata to all related nodes
            distributeEcMetadata(ecMetadata);




        }

        public void distributeEcDataToParityNodes(ByteBuffer[] parity) {
            
        }

        // public void sendSelectedSSTables() throws UnknownHostException {
        //     logger.debug("rymDebug: this is sendSelectedSSTables");
    
        //     // create a Message for sstContent
        //     Message<ECMessage> message = null;
        //     // get target endpoints
        //     getTargetEdpoints(this);
    
        //     if (targetEndpoint != null) {
        //         logger.debug("target endpoints are : {}", targetEndpoint);
        //         // setup message
        //         message = Message.outWithFlag(Verb.ERASURECODE_REQ, this, MessageFlag.CALL_BACK_ON_FAILURE);
        //         logger.debug("rymDebug: This is dumped message: {}", message);
        //         MessagingService.instance().sendSSTContentWithoutCallback(message, targetEndpoint);
        //     } else {
        //         logger.debug("targetEndpoints is null");
        //     }
        // }

        private void distributeEcMetadata(ECMetadata ecMetadata) {

        }


    }



}
