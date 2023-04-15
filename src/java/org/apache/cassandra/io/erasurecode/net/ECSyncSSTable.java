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
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
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
    public final List<DecoratedKey> allKey;
    public final String sstHashID;
    public static ByteObjectConversion<List<DecoratedKey>> keyConverter = new ByteObjectConversion<List<DecoratedKey>>();
    public static ByteObjectConversion<List<InetAddressAndPort>> ipConverter = new ByteObjectConversion<List<InetAddressAndPort>>();

    
    public static final Logger logger = LoggerFactory.getLogger(ECMessage.class);

    public ECSyncSSTable(List<DecoratedKey> allKey, String sstHashID) {
        // this.sstContent = sstContent;
        // this.sstSize = sstContent.remaining();
        this.allKey = new ArrayList<>(allKey);
        this.sstHashID = sstHashID;
    }

    public void sendSSTableToSecondary(List<InetAddressAndPort> replicaNodes) throws Exception {
        try {
            logger.debug("rymDebug: try to serialize allKey, allKey num is {}, keys are {}", this.allKey.size(), this.allKey);
            this.sstSize = keyConverter.toByteArray(this.allKey).length;
            logger.debug("rymDebug: ECSyncSSTable size is {}",this.sstSize);
            this.sstContent = new byte[this.sstSize];
            this.sstContent = keyConverter.toByteArray(this.allKey);
            logger.debug("rymDebug: ECSyncSSTable sstContent is {}, size is {}", this.sstContent, this.sstContent.length);

            // logger.debug("rymDebug: ECSyncSSTable key to bytes length is {}", converter.toByteArray(this.allKey).length);
            // this.sstContent = Arrays.copyOf(converter.toByteArray(this.allKey), this.sstSize);
        } catch (Exception e) {
            logger.error("rymError: cannot get the bytes array from key!!!, error info {}", e);
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
            out.write(t.sstContent);
        }
    
        @Override
        public ECSyncSSTable deserialize(DataInputPlus in, int version) throws IOException {
            String sstHashID = in.readUTF();
            int sstSize = in.readInt();
            byte[] sstContent = new byte[sstSize];
            in.readFully(sstContent);
            List<DecoratedKey> allKey = new ArrayList<DecoratedKey>();
            try {
                allKey = keyConverter.fromByteArray(sstContent);
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

    public static void main(String[] args) {

        List<InetAddressAndPort> eps = new ArrayList<InetAddressAndPort>();
        eps.add(FBUtilities.getBroadcastAddressAndPort());
        try {
            byte[] epsByte =  ipConverter.toByteArray(eps);
            logger.debug("eps is {}, eps byte is {}, length is {}", eps, epsByte, epsByte.length);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // byte[] srcArray = new byte[]{1, 2, 3, 4};
        // byte[] destArray = new byte[srcArray.length];
        // System.arraycopy(srcArray, 0, destArray, 0, srcArray.length);
        // logger.debug("destArray is {}", destArray);
    }

}


