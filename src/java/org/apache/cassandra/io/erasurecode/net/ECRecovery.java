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
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.compiler.STParser.compoundElement_return;

public class ECRecovery {
    private static final Logger logger = LoggerFactory.getLogger(ECRecovery.class);


    public void recoveryDataFromErasureCodes(final String sstHash, final InetAddressAndPort recoveryNode) throws Exception {
        
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


        // Step 3: Decode the raw data
        ECNetutils.waitUntilRequestCodesReady(StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash), sstHash);
        ECNetutils.waitUntilRequestCodesReady(StorageService.instance.globalSSTHashToParityCodeMap.get(sstHash), sstHash);
        
        ErasureCoderOptions ecOptions = new ErasureCoderOptions(k, m);
        ErasureDecoder decoder = new NativeRSDecoder(ecOptions);
        ByteBuffer[] recoveryOriginalSrc = new ByteBuffer[k];
        ByteBuffer[] output = new ByteBuffer[1];
        output[0] = ByteBuffer.allocateDirect(codeLength);

        int eraseIndex = ecMetadata.ecMetadataContent.primaryNodes.indexOf(recoveryNode);

        int[] eraseIndexes = { eraseIndex };
        int[] decodeIndexes = { 4, 1, 2, 3 };
        decoder.decode(recoveryOriginalSrc, decodeIndexes, eraseIndexes, output);

        // Step 4: record the raw data locally
        

        // Step 5: send the raw data to the peer secondary nodes
        


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
        for(int i = 0; i < k; i++) {
            erasureCodes[i] = ByteBuffer.allocateDirect(codeLength);
        }

        ByteBuffer[] parityCodes = new ByteBuffer[m];
        for(int i = 0; i < m; i++) {
            parityCodes[i] = ByteBuffer.allocateDirect(codeLength);
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
        StorageService.instance.globalSSTHashToParityCodeMap.put(oldSSTHash, parityCodes);
        if(ecMetadata.ecMetadataContent.parityHashList == null || 
           ecMetadata.ecMetadataContent.parityNodes == null) {
            ECNetutils.printStackTace(String.format("rymERROR: When we are update old sstable (%s), we cannot to get parity hash or parity code for stripID (%s)", oldSSTHash, ecMetadata.stripeId));
        } else {
            // get the needed parity code remotely, send a parity code request
            for (int i = 0; i < ecMetadata.ecMetadataContent.parityHashList.size(); i++) {
                ECRequestParity request = new ECRequestParity(ecMetadata.ecMetadataContent.parityHashList.get(i), oldSSTHash, i);
                request.requestParityCode(ecMetadata.ecMetadataContent.parityNodes.get(i));
            }

        }

    }


}
