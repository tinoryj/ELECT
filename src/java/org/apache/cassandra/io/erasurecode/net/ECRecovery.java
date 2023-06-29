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
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.erasurecode.ErasureCoderOptions;
import org.apache.cassandra.io.erasurecode.ErasureDecoder;
import org.apache.cassandra.io.erasurecode.NativeRSDecoder;
import org.apache.cassandra.io.erasurecode.net.ECNetutils.ByteObjectConversion;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.compiler.STParser.compoundElement_return;

public class ECRecovery {
    private static final Logger logger = LoggerFactory.getLogger(ECRecovery.class);
    public static final ECRecovery instance = new ECRecovery();


    public static void recoveryDataFromErasureCodes(final String sstHash) throws Exception {
        logger.debug("rymDebug: This is recovery for sstHash ({}), the replica nodes are ({})", sstHash);
        
        int k = DatabaseDescriptor.getEcDataNodes();
        int m = DatabaseDescriptor.getParityNodes();

        // Step 1: Get the ECSSTable from global map and get the ecmetadata
        SSTableReader sstable = StorageService.instance.globalSSTHashToECSSTableMap.get(sstHash);
        if(sstable == null) 
            throw new NullPointerException(String.format("rymERROR: Cannot get ECSSTable (%s)", sstHash));

        String ecMetadataFile = sstable.descriptor.filenameFor(Component.EC_METADATA);

        byte[] ecMetadataInBytes = ECNetutils.readBytesFromFile(ecMetadataFile);
        ECMetadata ecMetadata = (ECMetadata) ByteObjectConversion.byteArrayToObject(ecMetadataInBytes);

        // Step 2: Request the coding blocks from related nodes
        int codeLength = StorageService.getErasureCodeLength();
        retrieveErasureCodesForRecovery(ecMetadata, sstHash, codeLength, k, m);

        ByteBuffer[] recoveryOriginalSrc = StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash);

        if(recoveryOriginalSrc == null) {
            throw new NullPointerException(String.format("rymERROR: we cannot get erasure code for sstable (%s)", sstHash));
        }

        int[] decodeIndexes = waitUntilRequestCodesReady(recoveryOriginalSrc, sstHash, k);

        int eraseIndex = ecMetadata.ecMetadataContent.sstHashIdList.indexOf(sstHash);
        int[] eraseIndexes = { eraseIndex };


        // Step 3: Decode the raw data
        ErasureCoderOptions ecOptions = new ErasureCoderOptions(k, m);
        ErasureDecoder decoder = new NativeRSDecoder(ecOptions);
        ByteBuffer[] output = new ByteBuffer[1];
        output[0] = ByteBuffer.allocateDirect(codeLength);
        decoder.decode(recoveryOriginalSrc, decodeIndexes, eraseIndexes, output);

        // Step 4: record the raw data locally
        SSTableReader.loadRawData(output[0], sstable.descriptor);

        // Step 5: send the raw data to the peer secondary nodes
        List<InetAddressAndPort> replicaNodes = ecMetadata.ecMetadataContent.sstHashIdToReplicaMap.get(sstHash);
        if(replicaNodes == null) {
            throw new NullPointerException(String.format("rymERROR: we cannot get replica nodes for sstable (%s)", sstHash));
        }

        // Step 6: send the raw data to the peer secondary nodes
        byte[] sstContent = new byte[output[0].remaining()];
        output[0].get(sstContent);
        InetAddressAndPort localIP = FBUtilities.getBroadcastAddressAndPort();
        for (int i = 1; i< replicaNodes.size(); i++) {
            if(!replicaNodes.get(i).equals(localIP)){
                String cfName = "usertable" + i;
                ECRecoveryForSecondary recoverySignal = new ECRecoveryForSecondary(sstHash, sstContent, cfName);
                recoverySignal.sendDataToSecondaryNode(replicaNodes.get(i));
            }
        }

        // TODO: Wait until all data is ready.
        Thread.sleep(5000);
        logger.debug("rymDebug: recovery for sstHash is done!");

    }




    /**
     * This method is called when we decide to save a EC strip update signal to the process queue.
     * @param oldSSTHash
     * @param stripID
     * @param codeLength
     */
    private static void retrieveErasureCodesForRecovery(ECMetadata ecMetadata, String oldSSTHash, int codeLength, int k, int m) {

        // Step 0: Initialize the data and parity blocks
        ByteBuffer[] erasureCodes = new ByteBuffer[k];
        for(int i = 0; i < k + m; i++) {
            erasureCodes[i] = ByteBuffer.allocateDirect(codeLength);
        }



        // Step 1: Retrieve the data blocks.
        StorageService.instance.globalSSTHashToErasureCodesMap.put(oldSSTHash, erasureCodes);
        if(ecMetadata.ecMetadataContent.sstHashIdToReplicaMap != null) {
            int index = 0;
            for (Map.Entry<String, List<InetAddressAndPort>> entry : ecMetadata.ecMetadataContent.sstHashIdToReplicaMap.entrySet()) {
                ECRequestData request = new ECRequestData(oldSSTHash, index);
                request.requestData(entry.getValue().get(0));
                index++;
            }
        } else {
            throw new IllegalArgumentException(String.format("rymERROR: sstHashIDToReplicaMap is null!"));
        }

        // Step 2: Retrieve parity blocks.
        if(ecMetadata.ecMetadataContent.parityHashList == null || 
           ecMetadata.ecMetadataContent.parityNodes == null) {
            ECNetutils.printStackTace(String.format("rymERROR: When we are update old sstable (%s), we cannot to get parity hash or parity code for stripID (%s)", oldSSTHash, ecMetadata.stripeId));
        } else {
            // get the needed parity code remotely, send a parity code request
            for (int i = 0; i < ecMetadata.ecMetadataContent.parityHashList.size(); i++) {
                ECRequestParity request = new ECRequestParity(ecMetadata.ecMetadataContent.parityHashList.get(i), oldSSTHash, i + k, true);
                request.requestParityCode(ecMetadata.ecMetadataContent.parityNodes.get(i));
            }

        }

    }



    // [WARNING!] Make sure to avoid dead loops
    public static int[] waitUntilRequestCodesReady(ByteBuffer[] buffers, String oldSSTHash, int k) {
        int retryCount = 0;
        int[] decodeIndexes = new int[k];
        if(buffers != null) {
            while (!checkCodesAreReady(buffers, k)) {
                try {
                    if(retryCount < 5) {
                        Thread.sleep(1000);
                        retryCount++;
                    } else {
                        throw new IllegalStateException(String.format("rymERROR: cannot retrieve the remote codes for sstHash (%s)", oldSSTHash));
                    }
                    
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    break;
                }
            }
        } else {
            throw new NullPointerException(String.format("rymERROR: We cannot get parity codes for sstable %s", oldSSTHash));
        }

        int j = 0;
        for(int i = 0; i < buffers.length; i++) {
            if(j >= k){
                break;
            }

            if(buffers[i].position() != 0) {
                buffers[i].rewind();
                decodeIndexes[j++] = i;
            }
        }

        return decodeIndexes;
    }

    private static boolean checkCodesAreReady(ByteBuffer[] checkBuffers, int k) {
        int readyBlocks = 0;
        for(ByteBuffer buf : checkBuffers) {
            if(buf.position() != 0) {
                readyBlocks++;
            }
            if(readyBlocks >= k){
                return true;
            }
        }
        return false;
    }


}
