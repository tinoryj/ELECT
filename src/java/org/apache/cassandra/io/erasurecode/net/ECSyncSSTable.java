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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.erasurecode.net.ECNetutils.ByteObjectConversion;
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
    public final List<String> allKey;
    public final String sstHashID;
    public final String targetCfName;
    
    public final SSTablesInBytes sstInBytes;

    public byte[] sstContent;
    public int sstSize;
    public byte[] allKeysInBytes;
    public int allKeysInBytesSize;

    
    // public static ByteObjectConversion<List<DecoratedKey>> keyConverter = new ByteObjectConversion<List<DecoratedKey>>();
    // public static ByteObjectConversion<List<InetAddressAndPort>> ipConverter = new ByteObjectConversion<List<InetAddressAndPort>>();

    
    public static final Logger logger = LoggerFactory.getLogger(ECMessage.class);

    public static class SSTablesInBytes{
        // all keys of Data.db in String
        //public final List<String> allKey;
        // Filter.db in bytes
        public final byte[] sstFilter;
        // Index.db in bytes
        public final byte[] sstIndex;
        // Statistics.db in bytes
        public final byte[] sstStats;

        public SSTablesInBytes(byte[] sstFilter, byte[] sstIndex, byte[] sstStats) 
        {
            //this.allKey = allKey;
            this.sstFilter = sstFilter;
            this.sstIndex = sstIndex;
            this.sstStats = sstStats;
        }
    };

    public ECSyncSSTable(String sstHashID, String targetCfName, List<String> allKey, SSTablesInBytes sstInBytes) {
        this.allKey = new ArrayList<>(allKey);
        this.sstHashID = sstHashID;
        this.targetCfName = targetCfName;
        this.sstInBytes = sstInBytes;
    }

    public void sendSSTableToSecondary(InetAddressAndPort rpn) throws Exception {
        try {
            this.sstContent = ByteObjectConversion.objectToByteArray((Serializable) this.sstInBytes);
            this.sstSize = this.sstContent.length;
            this.allKeysInBytes = ByteObjectConversion.objectToByteArray((Serializable) this.allKey);
            this.allKeysInBytesSize = this.allKeysInBytes.length;
            // logger.debug("rymDebug: try to serialize allKey, allKey num is {}", this.allKey.size());
            logger.debug("rymDebug: ECSyncSSTable size is {}",this.sstSize);


            //logger.debug("rymDebug: ECSyncSSTable sstContent is {}, size is {}", this.sstContent, this.sstContent.length);

        } catch (Exception e) {
            logger.error("rymError: cannot get the bytes array from key!!!, error info {}", e);
        }
        Message<ECSyncSSTable> message = null;
        InetAddressAndPort locaIP = FBUtilities.getBroadcastAddressAndPort();
        if (rpn != null && !rpn.equals(locaIP)) {
            message = Message.outWithFlag(Verb.ECSYNCSSTABLE_REQ, this, MessageFlag.CALL_BACK_ON_FAILURE);
            MessagingService.instance().sendSSTContentWithoutCallback(message, rpn);
        } else {
            logger.debug("rymError: replicaNodes is null!!");
        }
    }

    public static final class Serializer implements IVersionedSerializer<ECSyncSSTable> {

        @Override
        public void serialize(ECSyncSSTable t, DataOutputPlus out, int version) throws IOException {
            out.writeUTF(t.sstHashID);
            out.writeUTF(t.targetCfName);
            out.writeInt(t.sstSize);
            out.write(t.sstContent);
            out.write(t.allKeysInBytesSize);
            out.write(t.allKeysInBytes);
        }
    
        @Override
        public ECSyncSSTable deserialize(DataInputPlus in, int version) throws IOException {
            String sstHashID = in.readUTF();
            String targetCfName = in.readUTF();
            int sstSize = in.readInt();
            byte[] sstContent = new byte[sstSize];
            in.readFully(sstContent);
            int allKeysInBytesSize = in.readInt();
            byte[] allKeysInBytes = new byte[allKeysInBytesSize];
            in.readFully(allKeysInBytes);

            List<String> allKey = new ArrayList<String>();
           
            try {
                // allKey = keyConverter.fromByteArray(sstContent);
                allKey = (List<String>) ByteObjectConversion.byteArrayToObject(sstContent);
                SSTablesInBytes sstInBytes = (SSTablesInBytes) ByteObjectConversion.byteArrayToObject(sstContent);
                
                return new ECSyncSSTable(sstHashID, targetCfName, allKey, sstInBytes);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                logger.error("ERROR: get sstables in bytes error!");
            }
            // ByteBuffer sstContent = ByteBuffer.wrap(buf);
            return null;
            
        }
    
        @Override
        public long serializedSize(ECSyncSSTable t, int version) {
            long size = t.sstSize + sizeof(t.sstSize) +
                        t.allKeysInBytesSize + sizeof(t.allKeysInBytesSize) +
                        sizeof(t.sstHashID) + sizeof(t.targetCfName);
            return size;
    
        }
    
    }
    
}


