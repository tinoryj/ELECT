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

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ForwardingInfo;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.tracing.Tracing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECVerbHandler implements IVerbHandler<ECMessage>{

    public static final ECVerbHandler instance = new ECVerbHandler();
    private static final Logger logger = LoggerFactory.getLogger(ECNetSend.class);


    private void respond(Message<?> respondTo, InetAddressAndPort respondToAddress)
    {
        Tracing.trace("Enqueuing response to {}", respondToAddress);
        MessagingService.instance().send(respondTo.emptyResponse(), respondToAddress);
    }

    private void failed()
    {
        Tracing.trace("Payload application resulted in ECTimeout, not replying");
    }

    @Override
    public void doVerb(Message<ECMessage> message) throws IOException {
        String byteChunk = message.payload.byteChunk;
        long   k = message.payload.k;

        logger.debug("rymDebug: get new message!!! sstContent is {}, parameter k is {}", byteChunk, k);

        
        

        // check if there were any forwarding headers in this message
        ForwardingInfo forwardTo = message.forwardTo();
        if(forwardTo != null)
            forwardToLocalNodes(message, forwardTo);
        
        //InetAddressAndPort respondToAddress = message.respondTo();
        // TODO: receive data and do something
        Tracing.trace("recieved byteChunk is: {}, k is: {}, sourceEdpoint is: {}, header is: {}",
                        byteChunk, k, message.from(), message.header);
    }


    private static void forwardToLocalNodes(Message<ECMessage> originalMessage, ForwardingInfo forwardTo)
    {
        Message.Builder<ECMessage> builder =
            Message.builder(originalMessage)
                   .withParam(ParamType.RESPOND_TO, originalMessage.from())
                   .withoutParam(ParamType.FORWARD_TO);

        boolean useSameMessageID = forwardTo.useSameMessageID(originalMessage.id());
        // reuse the same Message if all ids are identical (as they will be for 4.0+ node originated messages)
        Message<ECMessage> message = useSameMessageID ? builder.build() : null;

        forwardTo.forEach((id, target) ->
        {
            Tracing.trace("Enqueuing forwarded write to {}", target);
            MessagingService.instance().send(useSameMessageID ? message : builder.withId(id).build(), target);
        });
    }
    
}
