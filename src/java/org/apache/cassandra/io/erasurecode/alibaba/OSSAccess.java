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
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class OSSAccess {
    private static final Logger logger = LoggerFactory.getLogger(OSSAccess.class);

    private static String endpoint = "http://oss-cn-chengdu.aliyuncs.com";
    private static String bucketName = "elect-cloud";
    private static String localIP = FBUtilities.getBroadcastAddressAndPort().toString(false);
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

    public static boolean uploadFileToOSS(String targetFilePath) {
        try {
            InputStream inputStream = new FileInputStream(targetFilePath);
            // 创建PutObjectRequest对象。
            String objectName = targetFilePath.replace('/', '_') + localIP;
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectName, inputStream);
            // 创建PutObject请求。
            PutObjectResult result = ossClient.putObject(putObjectRequest);
        } catch (OSSException oe) {
            logger.error("OSS Error Message:" + oe.getErrorMessage() + "\nError Code:" + oe.getErrorCode()
                    + "\nRequest ID:" + oe.getRequestId());
            return false;
        } catch (ClientException ce) {
            logger.error("OSS Internet Error Message:" + ce.getMessage());
            return false;
        } catch (Throwable e) {
            logger.error("Get file input stream error:");
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean uploadFileToOSS(String targetFilePath, byte[] content) {
        try {
            String objectName = targetFilePath.replace('/', '_') + localIP;
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectName,
                    new ByteArrayInputStream(content));
            PutObjectResult result = ossClient.putObject(putObjectRequest);
        } catch (OSSException oe) {
            logger.error("OSS Error Message:" + oe.getErrorMessage() + "\nError Code:" + oe.getErrorCode()
                    + "\nRequest ID:" + oe.getRequestId());
            return false;
        } catch (ClientException ce) {
            logger.error("OSS Internet Error Message:" + ce.getMessage());
            return false;
        } catch (Throwable e) {
            logger.error("Get file input stream error:");
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean uploadFileToOSS(String targetFilePath, String content) {
        try {
            String objectName = targetFilePath.replace('/', '_') + localIP;
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectName,
                    new ByteArrayInputStream(content.getBytes()));
            PutObjectResult result = ossClient.putObject(putObjectRequest);
        } catch (OSSException oe) {
            logger.error("OSS Error Message:" + oe.getErrorMessage() + "\nError Code:" + oe.getErrorCode()
                    + "\nRequest ID:" + oe.getRequestId());
            return false;
        } catch (ClientException ce) {
            logger.error("OSS Internet Error Message:" + ce.getMessage());
            return false;
        } catch (Throwable e) {
            logger.error("Get file input stream error:");
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean downloadFileFromOSS(String originalFilePath, String targetStorePath) {
        try {
            ossClient.getObject(
                    new GetObjectRequest(bucketName, originalFilePath.replace('/', '_') + localIP),
                    new File(targetStorePath));
        } catch (OSSException oe) {
            logger.error("OSS Error Message:" + oe.getErrorMessage() + "\nError Code:" + oe.getErrorCode()
                    + "\nRequest ID:" + oe.getRequestId());
            return false;
        } catch (ClientException ce) {
            logger.error("OSS Internet Error Message:" + ce.getMessage());
            return false;
        }
        return true;
    }

    public boolean deleteSingleFileInOSS(String targetFilePath) {
        try {
            boolean found = ossClient.doesObjectExist(bucketName,
                    targetFilePath.replace('/', '_') + localIP);
            if (found) {
                logger.debug("Found target file " + targetFilePath + " in bucket, delete it now");
                ossClient.deleteObject(bucketName,
                        targetFilePath.replace('/', '_') + localIP);
                return true;
            } else {
                logger.error("Could not found target file " + targetFilePath + " in bucket");
                return false;
            }
        } catch (OSSException oe) {
            logger.error("OSS Error Message:" + oe.getErrorMessage() + "\nError Code:" + oe.getErrorCode()
                    + "\nRequest ID:" + oe.getRequestId());
            return false;
        } catch (ClientException ce) {
            logger.error("OSS Internet Error Message:" + ce.getMessage());
            return false;
        }
    }

    public void listingFilesInOSS() {
        // Listing all files from OSS bucket
        try {
            ObjectListing objectListing = ossClient.listObjects(bucketName);
            List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
            List<String> keys = new ArrayList<String>();
            for (OSSObjectSummary s : sums) {
                logger.debug("Existing file on OSS: {}", s.getKey());
                keys.add(s.getKey());
            }
        } catch (OSSException oe) {
            logger.error("OSS Error Message:" + oe.getErrorMessage() + "\nError Code:" + oe.getErrorCode()
                    + "\nRequest ID:" + oe.getRequestId());
        } catch (ClientException ce) {
            logger.error("OSS Internet Error Message:" + ce.getMessage());
        }
    }

    public void cleanUpOSS() {
        // Delete all files from OSS bucket
        try {
            ObjectListing objectListing = ossClient.listObjects(bucketName);
            List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
            List<String> keys = new ArrayList<String>();
            for (OSSObjectSummary s : sums) {
                logger.debug("Existing file on OSS: {}", s.getKey());
                keys.add(s.getKey());
            }
            DeleteObjectsResult deleteObjectsResult = ossClient
                    .deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keys).withEncodingType("url"));
            List<String> deletedObjects = deleteObjectsResult.getDeletedObjects();
            try {
                for (String obj : deletedObjects) {
                    String deleteObj = URLDecoder.decode(obj, "UTF-8");
                    logger.debug("Delete file: {}", deleteObj);
                }
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        } catch (OSSException oe) {
            logger.error("OSS Error Message:" + oe.getErrorMessage() + "\nError Code:" + oe.getErrorCode()
                    + "\nRequest ID:" + oe.getRequestId());
        } catch (ClientException ce) {
            logger.error("OSS Internet Error Message:" + ce.getMessage());
        }
    }

    public void closeOSS() {
        ossClient.shutdown();
    }

    public static void main(String[] args) throws Exception {
        OSSAccess ossAccess = new OSSAccess();
        try {
            // Check if the bucket exists then update/download/delete.
            if (ossAccess.ossClient.doesBucketExist(bucketName)) {
                System.out.println("Target Bucket is created：" + bucketName);
                BucketInfo info = ossAccess.ossClient.getBucketInfo(bucketName);
                System.out.println("Bucket " + bucketName + " Info：");
                System.out.println("\tData Center: " + info.getBucket().getLocation());
                System.out.println("\tCreate Time: " + info.getBucket().getCreationDate());
                System.out.println("\tUser Info: " + info.getBucket().getOwner());

                // ossAccess.uploadFileToOSS("/home/tinoryj/Projects/CassandraEC/README.md");
                // ossAccess.downloadFileFromOSS("/home/tinoryj/Projects/CassandraEC/README.md",
                // "/home/tinoryj/Projects/CassandraEC/README.md.d");
                // ossAccess.downloadFileFromOSS("/home/tinoryj/Projects/CassandraEC/README.md",
                // "/home/tinoryj/Projects/CassandraEC/README.md.d");
                ossAccess.cleanUpOSS();
            } else {
                System.out.println("Bucket not exist, creating：" + bucketName);
                ossAccess.ossClient.createBucket(bucketName);
            }
        } catch (OSSException oe) {
            logger.error("OSS Error Message:" + oe.getErrorMessage() + "\nError Code:" + oe.getErrorCode()
                    + "\nRequest ID:" + oe.getRequestId());
        } catch (ClientException ce) {
            logger.error("OSS Internet Error Message:" + ce.getMessage());
        } finally {
            if (ossAccess.ossClient != null) {
                ossAccess.closeOSS();
                System.out.println("OSS closed.");
            }
        }
    }
}
