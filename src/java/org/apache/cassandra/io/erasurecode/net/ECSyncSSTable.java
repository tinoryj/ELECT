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
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.erasurecode.net.utils.ByteObjectConversion;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.FBUtilities;
import org.checkerframework.checker.units.qual.A;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.cassandra.db.TypeSizes.sizeof;


public class ECSyncSSTable {
    public static final Serializer serializer = new Serializer();
    public byte[] sstContent;
    public int sstSize;
    public final Iterable<DecoratedKey> allKey;
    public final String sstHashID;
    public static ByteObjectConversion<Iterable<DecoratedKey>> converter = new ByteObjectConversion<Iterable<DecoratedKey>>();

    
    public static final Logger logger = LoggerFactory.getLogger(ECMessage.class);

    public ECSyncSSTable(Iterable<DecoratedKey> allKey, String sstHashID) {
        // this.sstContent = sstContent;
        // this.sstSize = sstContent.remaining();
        this.allKey = allKey;
        this.sstHashID = sstHashID;
    }

    public void sendSSTableToSecondary(List<InetAddressAndPort> replicaNodes) {
        try {
            this.sstContent = converter.toByteArray(this.allKey);
            this.sstSize = this.sstContent.length;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Message<ECSyncSSTable> message = null;
        InetAddressAndPort locaIP = FBUtilities.getBroadcastAddressAndPort();
        if (replicaNodes != null) {
            for(InetAddressAndPort node : replicaNodes) {
                if(!node.equals(locaIP)) {
                    message = Message.outWithFlag(Verb.ECSYNCSSTABLE_REQ, this, MessageFlag.CALL_BACK_ON_FAILURE);
                    MessagingService.instance().sendSSTContentWithoutCallback(message, node);
                }
            }
        } else {
            logger.debug("rymError: replicaNodes is null!!");
        }
    }

    public static final class Serializer implements IVersionedSerializer<ECSyncSSTable> {

        @Override
        public void serialize(ECSyncSSTable t, DataOutputPlus out, int version) throws IOException {
            out.writeUTF(t.sstHashID);
            out.writeInt(t.sstSize);
            // byte[] buf = new byte[t.sstSize];
            // t.sstContent.get(buf);
            out.write(t.sstContent);
        }
    
        @Override
        public ECSyncSSTable deserialize(DataInputPlus in, int version) throws IOException {
            String sstHashID = in.readUTF();
            int sstSize = in.readInt();
            byte[] sstContent = new byte[sstSize];
            in.readFully(sstContent);
            Iterable<DecoratedKey> allKey = new ArrayList<DecoratedKey>();
            try {
                allKey = converter.fromByteArray(sstContent);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            // ByteBuffer sstContent = ByteBuffer.wrap(buf);

            return new ECSyncSSTable(allKey, sstHashID);
        }
    
        @Override
        public long serializedSize(ECSyncSSTable t, int version) {
            long size = t.sstSize + sizeof(t.sstSize) + sizeof(t.sstHashID);
            return size;
    
        }
    
    }

}


