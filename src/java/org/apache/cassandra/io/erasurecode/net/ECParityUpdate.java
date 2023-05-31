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
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.nio.ByteBuffer;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Verb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.erasurecode.net.ECNetutils.ByteObjectConversion;

import static org.apache.cassandra.db.TypeSizes.sizeof;


/**
 * This class contains information of parity update and implements the methods for sending parity update signals
 * @param oldSSTables  Map<String, ByteBuffer>
 * @param newSSTables  Map<String, ByteBuffer>
 * @param parityNode
 */
public final class ECParityUpdate implements Serializable {
    public static final Serializer serializer = new Serializer();
    public static final Logger logger = LoggerFactory.getLogger(ECParityUpdate.class);

    
    public final List<SSTableContentWithHashID> oldSSTables;
    public byte[] oldSSTablesInBytes;
    public int oldSSTablesInBytesSize;

    public final List<SSTableContentWithHashID> newSSTables;
    public byte[] newSSTablesInBytes;
    public int newSSTablesInBytesSize;

    public final List<InetAddressAndPort> parityNodes;
    public byte[] parityNodesInBytes;
    public int parityNodesInBytesSize;


    public ECParityUpdate(List<SSTableContentWithHashID> oldSSTables, List<SSTableContentWithHashID> newSSTables,
                            List<InetAddressAndPort> parityNodes) {
        this.oldSSTables = oldSSTables;
        this.newSSTables = newSSTables;
        this.parityNodes = parityNodes;
    }


    public static class SSTableContentWithHashID implements Serializable {
        public final String sstHash;
        public final ByteBuffer sstContent;
        public final int sstContentSize;
        public SSTableContentWithHashID(String sstHash, ByteBuffer sstContent) {
            this.sstHash = sstHash;
            this.sstContent = sstContent;
            this.sstContentSize = sstContent.capacity();
        }
    }


    // Send SSTables to a specific node
    public void sendParityUpdateSignal() {

        try {

            this.newSSTablesInBytes = ByteObjectConversion.objectToByteArray((Serializable) this.newSSTables);
            this.newSSTablesInBytesSize = this.newSSTablesInBytes.length;
            
            this.oldSSTablesInBytes = ByteObjectConversion.objectToByteArray((Serializable) this.oldSSTables);
            this.oldSSTablesInBytesSize = this.oldSSTablesInBytes.length;

            this.parityNodesInBytes = ByteObjectConversion.objectToByteArray((Serializable) this.parityNodes);
            this.parityNodesInBytesSize = this.parityNodesInBytes.length;

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


        Message<ECParityUpdate> message = Message.outWithFlag(Verb.ECPARITYUPDATE_REQ, this, MessageFlag.CALL_BACK_ON_FAILURE);
        MessagingService.instance().send(message, this.parityNodes.get(0));
    }


    public static final class Serializer implements IVersionedSerializer<ECParityUpdate> {

        @Override
        public void serialize(ECParityUpdate t, DataOutputPlus out, int version) throws IOException {

            out.writeInt(t.newSSTablesInBytesSize);
            out.write(t.newSSTablesInBytes);
            out.writeInt(t.oldSSTablesInBytesSize);
            out.write(t.oldSSTablesInBytes);
            out.writeInt(t.parityNodesInBytesSize);
            out.write(t.parityNodesInBytes);
        }

        @Override
        public ECParityUpdate deserialize(DataInputPlus in, int version) throws IOException {
            
            int newSSTablesInBytesSize = in.readInt();
            byte[] newSSTablesInBytes = new byte[newSSTablesInBytesSize];
            in.readFully(newSSTablesInBytes);

            int oldSSTablesInBytesSize = in.readInt();
            byte[] oldSSTablesInBytes = new byte[oldSSTablesInBytesSize];
            in.readFully(oldSSTablesInBytes);

            int parityNodesInBytesSize = in.readInt();
            byte[] parityNodesInBytes = new byte[parityNodesInBytesSize];
            in.readFully(parityNodesInBytes);
            
            try {

                List<SSTableContentWithHashID> newSSTables = (List<SSTableContentWithHashID>) ByteObjectConversion.byteArrayToObject(newSSTablesInBytes);
                List<SSTableContentWithHashID> oldSSTables = (List<SSTableContentWithHashID>) ByteObjectConversion.byteArrayToObject(oldSSTablesInBytes);
                List<InetAddressAndPort> parityNodes = (List<InetAddressAndPort>) ByteObjectConversion.byteArrayToObject(parityNodesInBytes);

                return new ECParityUpdate(oldSSTables, newSSTables, parityNodes);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            return null;

        }

        @Override
        public long serializedSize(ECParityUpdate t, int version) {
            long size = sizeof(t.newSSTablesInBytesSize) +
                        t.newSSTablesInBytesSize  +
                        sizeof(t.oldSSTablesInBytesSize) +
                        t.oldSSTablesInBytesSize +
                        sizeof(t.parityNodesInBytesSize) +
                        t.parityNodesInBytesSize;
            return size;
        }


    }

}
