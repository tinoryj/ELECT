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

package org.apache.cassandra.io.erasurecode.alibaba;

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
import org.apache.cassandra.io.erasurecode.net.ECMetadata;
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

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.Bucket;
import com.aliyun.oss.model.CannedAccessControlList;
import com.aliyun.oss.model.CreateBucketRequest;
import com.aliyun.oss.model.ListBucketsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectAcl;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.PutObjectRequest;

import static org.apache.cassandra.db.TypeSizes.sizeof;

public class OSSAccess {
    String sstHash;
    String ksName;
    String cfName;
    String key;
    String startToken;
    String endToken;
    public static final Serializer serializer = new Serializer();

    private static final Logger logger = LoggerFactory.getLogger(ECMetadata.class);
    String accessKeyId = "yourAccessKeyId";
    String accessKeySecret = "yourAccessKeySecret";
    // STS安全令牌SecurityToken。
    String securityToken = "yourSecurityToken";
    CredentialsProvider credentialsProvider;

    public OSSAccess(String sstHash, String ksName, String cfName, String key,
            String startToken, String endToken) {

        // STS临时访问密钥AccessKey ID和AccessKey Secret。

        // 使用代码嵌入的STS临时访问密钥和安全令牌配置访问凭证。
        CredentialsProvider credentialsProvider = new DefaultCredentialProvider(accessKeyId, accessKeySecret,
                securityToken);
        this.sstHash = sstHash;
        this.ksName = ksName;
        this.cfName = cfName;
        this.key = key;
        this.startToken = startToken;
        this.endToken = endToken;
    }

    public void synchronizeCompaction(List<InetAddressAndPort> replicaNodes) {
        logger.debug("rymDebug: this synchronizeCompaction method, replicaNodes: {}, local node is {} ",
                replicaNodes, FBUtilities.getBroadcastAddressAndPort());
        Message<OSSAccess> message = Message.outWithFlag(Verb.ECCOMPACTION_REQ, this, MessageFlag.CALL_BACK_ON_FAILURE);
        // send compaction request to all secondary nodes
        for (int i = 1; i < replicaNodes.size(); i++) {
            if (!replicaNodes.get(i).equals(FBUtilities.getBroadcastAddressAndPort()))
                MessagingService.instance().send(message, replicaNodes.get(i));
        }
    }

    public static final class Serializer implements IVersionedSerializer<OSSAccess> {

        @Override
        public void serialize(OSSAccess t, DataOutputPlus out, int version) throws IOException {
            out.writeUTF(t.sstHash);
            out.writeUTF(t.ksName);
            out.writeUTF(t.cfName);
            out.writeUTF(t.key);
            out.writeUTF(t.startToken);
            out.writeUTF(t.endToken);
        }

        @Override
        public OSSAccess deserialize(DataInputPlus in, int version) throws IOException {
            String sstHash = in.readUTF();
            String ksName = in.readUTF();
            String cfName = in.readUTF();
            String key = in.readUTF();
            String startToken = in.readUTF();
            String endToken = in.readUTF();
            return new OSSAccess(sstHash, ksName, cfName, key, startToken, endToken);
        }

        @Override
        public long serializedSize(OSSAccess t, int version) {
            long size = sizeof(t.sstHash) +
                    sizeof(t.ksName) +
                    sizeof(t.cfName) +
                    sizeof(t.key) +
                    sizeof(t.startToken) +
                    sizeof(t.endToken);
            return size;
        }

    }

}
