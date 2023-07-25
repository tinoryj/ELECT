/*
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
import java.lang.module.ResolutionException;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.db.TypeSizes.sizeof;

public class ResponseLSMTreeRecovery {
    private static final Logger logger = LoggerFactory.getLogger(ResponseLSMTreeRecovery.class);
    public static final Serializer serializer = new Serializer();
    public final String rawCfPath;

    public ResponseLSMTreeRecovery(String rawCfPath) {
        this.rawCfPath = rawCfPath;
    }

    
    public static void sendRecoveryIsReadySignal(InetAddressAndPort target, String rawCfPath) {

        ResponseLSMTreeRecovery msg = new ResponseLSMTreeRecovery(rawCfPath);
        Message<ResponseLSMTreeRecovery> message = Message.outWithFlag(Verb.ECREQUESTDATA_REQ, msg, MessageFlag.CALL_BACK_ON_FAILURE);
        MessagingService.instance().send(message, target);

    }

    public static final class Serializer implements IVersionedSerializer<ResponseLSMTreeRecovery> {

        @Override
        public void serialize(ResponseLSMTreeRecovery t, DataOutputPlus out, int version) throws IOException {
            out.writeUTF(t.rawCfPath);
        }

        @Override
        public ResponseLSMTreeRecovery deserialize(DataInputPlus in, int version) throws IOException {
            String rawCfPath = in.readUTF();
            return new ResponseLSMTreeRecovery(rawCfPath);
        }

        @Override
        public long serializedSize(ResponseLSMTreeRecovery t, int version) {
            long size = sizeof(t.rawCfPath);
            return size;
        }
        
    }
}
