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
import static org.apache.cassandra.db.TypeSizes.sizeof;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.common.auth.*;
import com.aliyun.oss.common.comm.Protocol;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.PutObjectResult;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.BucketInfo;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;

import java.io.FileInputStream;
import java.io.InputStream;

public class OSSAccess {
    private static final Logger logger = LoggerFactory.getLogger(OSSAccess.class);

    private static String endpoint = "http://oss-cn-chengdu.aliyuncs.com";
    private static String bucketName = "elect-cloud";
    public static OSS ossClient;

    public OSSAccess() throws com.aliyuncs.exceptions.ClientException {
        EnvironmentVariableCredentialsProvider credentialsProvider = CredentialsProviderFactory
                .newEnvironmentVariableCredentialsProvider();

        ClientBuilderConfiguration conf = new ClientBuilderConfiguration();
        conf.setMaxConnections(200);
        conf.setSocketTimeout(10000);
        conf.setConnectionTimeout(10000);
        conf.setMaxErrorRetry(5);
        conf.setProtocol(Protocol.HTTP);
        conf.setUserAgent("aliyun-sdk-java");

        this.ossClient = new OSSClientBuilder().build(endpoint, credentialsProvider, conf);
    }

    public boolean uploadFileToOSS(String targetFilePath) {
        try {
            InputStream inputStream = new FileInputStream(targetFilePath);
            // 创建PutObjectRequest对象。
            String objectName = targetFilePath.substring(targetFilePath.lastIndexOf("/") + 1);
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectName, inputStream);
            // 创建PutObject请求。
            PutObjectResult result = ossClient.putObject(putObjectRequest);
        } catch (OSSException oe) {
            System.out.println("Caught an OSSException, which means your request made it to OSS, "
                    + "but was rejected with an error response for some reason.");
            System.out.println("Error Message:" + oe.getErrorMessage());
            System.out.println("Error Code:" + oe.getErrorCode());
            System.out.println("Request ID:" + oe.getRequestId());
            System.out.println("Host ID:" + oe.getHostId());
        } catch (ClientException ce) {
            System.out.println("Caught an ClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with OSS, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message:" + ce.getMessage());
        } finally {
            if (ossClient != null) {
                ossClient.shutdown();
            }
        }
        return true;
    }

    public void cleanUpOSS() {
        ossClient.shutdown();
    }

    public static void main(String[] args) throws Exception {
        OSSAccess ossAccess = new OSSAccess();
        try {
            // Check if the bucket exists.
            if (ossAccess.ossClient.doesBucketExist(bucketName)) {
                System.out.println("Target Bucket is created：" + bucketName);
                BucketInfo info = ossAccess.ossClient.getBucketInfo(bucketName);
                System.out.println("Bucket " + bucketName + "的信息如下：");
                System.out.println("\t数据中心：" + info.getBucket().getLocation());
                System.out.println("\t创建时间：" + info.getBucket().getCreationDate());
                System.out.println("\t用户标志：" + info.getBucket().getOwner());
            } else {
                System.out.println("您的Bucket不存在，创建Bucket：" + bucketName + "。");
                // 创建Bucket。详细请参看“SDK手册 > Java-SDK > 管理Bucket”。
                // 链接地址是：https://help.aliyun.com/document_detail/oss/sdk/java-sdk/manage_bucket.html?spm=5176.docoss/sdk/java-sdk/init
                ossAccess.ossClient.createBucket(bucketName);
            }

        } catch (OSSException oe) {
            System.out.println("Caught an OSSException, which means your request made it to OSS, "
                    + "but was rejected with an error response for some reason.");
            System.out.println("Error Message:" + oe.getErrorMessage());
            System.out.println("Error Code:" + oe.getErrorCode());
            System.out.println("Request ID:" + oe.getRequestId());
            System.out.println("Host ID:" + oe.getHostId());
        } catch (ClientException ce) {
            System.out.println("Caught an ClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with OSS, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message:" + ce.getMessage());
        } finally {
            if (ossAccess.ossClient != null) {
                ossAccess.ossClient.shutdown();
            }
        }
    }
}
