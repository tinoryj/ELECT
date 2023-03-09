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

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.db.TypeSizes.sizeof;

public final class ECMessage {

    public static final Serializer serializer = new Serializer();
    final String    byteChunk;
    final long      k;
    final String    keyspace;
    final String    key;
    final String    table;

    public ECMessage(String byteChunk, long k, String keyspace, String key, String table) {
        this.byteChunk = byteChunk;
        this.k = k;
        this.keyspace = keyspace;
        this.key = key;
        this.table = table;
    }


    public static final class Serializer implements IVersionedSerializer<ECMessage> {

        @Override
        public void serialize(ECMessage ecMessage, DataOutputPlus out, int version) throws IOException {
            // TODO: something may need to ensure, could be test
            out.writeUTF(ecMessage.byteChunk);
            out.writeLong(ecMessage.k);
        }

        @Override
        public ECMessage deserialize(DataInputPlus in, int version) throws IOException {
            String byteChunk = in.readUTF();
            long   k = in.readLong();
            return new ECMessage(byteChunk, k, null, null, null);
        }

        @Override
        public long serializedSize(ECMessage ecMessage, int version) {
            long size = sizeof(ecMessage.byteChunk) + sizeof(ecMessage.k);
            // + sizeof(ecMessage.keyspace) + sizeof(ecMessage.key) +  sizeof(ecMessage.table);

            
            return size;
            
        }
        
    }
    
}
