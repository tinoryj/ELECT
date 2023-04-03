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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.db.TypeSizes.sizeof;

public class ECCompaction {
    String sstHash;
    String ksName;
    String cfName;
    String key;
    String startToken;
    String endToken;
    public static final Serializer serializer = new Serializer();

    private static final Logger logger = LoggerFactory.getLogger(ECMetadata.class);

    public ECCompaction(String sstHash, String ksName, String cfName, String key,
                        String startToken, String endToken) {
        this.sstHash = sstHash;
        this.ksName = ksName;
        this.cfName = cfName;
        this.key = key;
        this.startToken = startToken;
        this.endToken = endToken;
    }

    public void synchronizeCompaction(List<InetAddressAndPort> replicaNodes){
        logger.debug("rymDebug: this synchronizeCompaction method, replicaNodes: {}, local node is {} ",
         replicaNodes, FBUtilities.getBroadcastAddressAndPort());
        Message<ECCompaction> message = Message.outWithFlag(Verb.ECCOMPACTION_REQ, this, MessageFlag.CALL_BACK_ON_FAILURE);
        // send compaction request to all secondary nodes
        for (int i=1; i < replicaNodes.size();i++){
            if(!replicaNodes.get(i).equals(FBUtilities.getBroadcastAddressAndPort()))
                MessagingService.instance().send(message, replicaNodes.get(i));
        }
    }

    public static final class Serializer implements IVersionedSerializer<ECCompaction> {

        @Override
        public void serialize(ECCompaction t, DataOutputPlus out, int version) throws IOException {
            out.writeUTF(t.sstHash);
            out.writeUTF(t.ksName);
            out.writeUTF(t.cfName);
            out.writeUTF(t.key);
            out.writeUTF(t.startToken);
            out.writeUTF(t.endToken);
        }

        @Override
        public ECCompaction deserialize(DataInputPlus in, int version) throws IOException {
            String sstHash = in.readUTF();
            String ksName = in.readUTF();
            String cfName = in.readUTF();
            String key = in.readUTF();
            String startToken = in.readUTF();
            String endToken = in.readUTF();
            return new ECCompaction(sstHash, ksName, cfName, key, startToken, endToken);
        }

        @Override
        public long serializedSize(ECCompaction t, int version) {
            long size = sizeof(t.sstHash) + sizeof(t.ksName) + sizeof(t.cfName) + sizeof(t.key)
                        + sizeof(t.startToken) + sizeof(t.endToken);
            return size;
        }

    }

    public static void main(String[] args) throws IOException {
        // 将数据写入输出流
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        ByteBuffer buffer1 = ByteBuffer.wrap(new byte[]{1, 2, 3});
        ByteBuffer buffer2 = ByteBuffer.wrap(new byte[]{4, 5, 6, 7, 8});
        ByteBuffer buffer3 = ByteBuffer.wrap(new byte[]{9, 10});
        String strOut = "test";

        ByteBuffer[] buffers = new ByteBuffer[]{buffer1, buffer2, buffer3};

        dos.writeInt(buffers.length);

        for (ByteBuffer buffer : buffers) {
            dos.writeInt(buffer.remaining());
            dos.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        }
        dos.writeUTF(strOut);

        dos.flush();

        // 从输入流中读取数据
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);

        int numBuffers = dis.readInt();
        ByteBuffer[] receivedBuffers = new ByteBuffer[numBuffers];

        for (int i = 0; i < numBuffers; i++) {
            int length = dis.readInt();
            logger.debug("read length is: {}", length);
            byte[] bufferData = new byte[length];
            dis.readFully(bufferData);
            receivedBuffers[i] = ByteBuffer.wrap(bufferData);
        }
        String strIn = dis.readUTF();
        logger.debug("strIn is: {}", strIn);

        // 验证数据是否正确传输
        System.out.println(Arrays.toString(buffers));
        System.out.println(Arrays.toString(receivedBuffers));
    }

}
