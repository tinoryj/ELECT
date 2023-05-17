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
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import static org.apache.cassandra.db.TypeSizes.sizeof;
public class ECResponseParity {
    public static final Serializer serializer = new Serializer();

    public final String parityHash;
    public final String sstHash;
    public final byte[] parityCode;
    public final int parityCodeSize;
    public final int parityIndex;

    public ECResponseParity(String parityHash, String sstHash, byte[] parityCode, int parityIndex) {
        this.parityHash = parityHash;
        this.sstHash = sstHash;
        this.parityCode = parityCode;
        this.parityCodeSize = parityCode.length;
        this.parityIndex = parityIndex;
    }


    public void responseParity(InetAddressAndPort target) {
        
        Message<ECResponseParity> message = Message.outWithFlag(Verb.ECRESPONSEPARITY_REQ, this, MessageFlag.CALL_BACK_ON_FAILURE);
        MessagingService.instance().send(message, target);
    }

    public static final class Serializer implements IVersionedSerializer<ECResponseParity> {

        @Override
        public void serialize(ECResponseParity t, DataOutputPlus out, int version) throws IOException {
            out.writeUTF(t.parityHash);
            out.writeUTF(t.sstHash);
            out.writeInt(t.parityIndex);
            out.writeInt(t.parityCodeSize);
            out.write(t.parityCode);


        }

        @Override
        public ECResponseParity deserialize(DataInputPlus in, int version) throws IOException {
            String parityHash = in.readUTF();
            String sstHash = in.readUTF();
            int parityIndex = in.readInt();
            int parityCodeSize = in.readInt();
            byte[] parityCode = new byte[parityCodeSize];
            in.readFully(parityCode);
            return new ECResponseParity(parityHash, sstHash, parityCode, parityIndex);
        }

        @Override
        public long serializedSize(ECResponseParity t, int version) {
            long size = sizeof(t.parityHash) +
                        sizeof(t.sstHash) + 
                        sizeof(t.parityIndex) +
                        sizeof(t.parityCodeSize) +
                        t.parityCodeSize;
            return size;
        }
        
    }


}
