/*
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.io.File;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.erasurecode.ErasureCoderOptions;
import org.apache.cassandra.io.erasurecode.ErasureDecoder;
import org.apache.cassandra.io.erasurecode.NativeRSDecoder;
import org.apache.cassandra.io.erasurecode.net.ECMetadata.ECMetadataContent;
import org.apache.cassandra.io.erasurecode.net.ECNetutils.ByteObjectConversion;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class ResponseLSMTreeRecoveryVerbHandler implements IVerbHandler<ResponseLSMTreeRecovery> {
    
    private static final Logger logger = LoggerFactory.getLogger(ResponseLSMTreeRecoveryVerbHandler.class);
    public static final ResponseLSMTreeRecoveryVerbHandler instance = new ResponseLSMTreeRecoveryVerbHandler();
    @Override
    public void doVerb(Message<ResponseLSMTreeRecovery> message) throws FileNotFoundException {
        

        String rawCfPath = message.payload.rawCfPath;

        // read the ec sstables and perform the recovery
        File dataFolder = new File(rawCfPath);

        if(dataFolder.exists() && dataFolder.isDirectory()) {
            File[] files = dataFolder.listFiles();
            for(File file : files) {
                if(file.isFile() && file.getName().contains("EC.db")) {
                    try {
                        recoveryDataFromErasureCodesForLSMTree(file.getName());
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }

        } else {
            throw new FileNotFoundException(String.format("rymERROR: Folder for path (%s) does not exists!", rawCfPath));
        }



    }


    private static void recoveryDataFromErasureCodesForLSMTree(String ecMetadataFile) throws Exception {
        int k = DatabaseDescriptor.getEcDataNodes();
        int m = DatabaseDescriptor.getParityNodes();
        // Step 1: Get the ECSSTable from global map and get the ecmetadata
        byte[] ecMetadataInBytes = ECNetutils.readBytesFromFile(ecMetadataFile);
        logger.debug("rymDebug: [Debug recovery] the size of ecMetadataInBytes is ({})", ecMetadataInBytes.length);
        ECMetadataContent ecMetadataContent = (ECMetadataContent) ByteObjectConversion.byteArrayToObject(ecMetadataInBytes);
        if(ecMetadataContent == null)
            throw new NullPointerException(String.format("rymDebug: [Debug recovery] The ecMetadata for ecMetadataFile ({}) is null!", ecMetadataFile));

        // Step 2: Request the coding blocks from related nodes
        InetAddressAndPort localIp = FBUtilities.getBroadcastAddressAndPort();
        int index = ecMetadataContent.primaryNodes.indexOf(localIp);
        if(index == -1)
            throw new NullPointerException(String.format("rymERROR: we cannot get index from primary node list", ecMetadataContent.primaryNodes));
        String sstHash = ecMetadataContent.sstHashIdList.get(index);       
        int codeLength = StorageService.getErasureCodeLength();
        logger.debug("rymDebug: [Debug recovery] retrieve chunks for sstable ({})", sstHash);
        ECRecovery.retrieveErasureCodesForRecovery(ecMetadataContent, sstHash, codeLength, k, m);
        logger.debug("rymDebug: [Debug recovery] retrieve chunks for ecmetadata ({}) successfully", sstHash);

        // ByteBuffer[] recoveryOriginalSrc = StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash);

        if(StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash) == null) {
            throw new NullPointerException(String.format("rymERROR: we cannot get erasure code for sstable (%s)", sstHash));
        }

        logger.debug("rymDebug: [Debug recovery] wait chunks for sstable ({})", sstHash);
        int[] decodeIndexes = ECRecovery.waitUntilRequestCodesReady(StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash), sstHash, k, ecMetadataContent.zeroChunksNum, codeLength);

        int eraseIndex = ecMetadataContent.sstHashIdList.indexOf(sstHash);
        if(eraseIndex == -1)
            throw new NullPointerException(String.format("rymERROR: we cannot get index for sstable (%s) in ecMetadata, sstHash list is ({})", sstHash, ecMetadataContent.stripeId, ecMetadataContent.sstHashIdList));
        int[] eraseIndexes = { eraseIndex };

        logger.debug("rymDebug: [Debug recovery] When we recovery sstable ({}), the data blocks of strip id ({}) is ready.", sstHash, ecMetadataContent.stripeId);


        // Step 3: Decode the raw data

        ByteBuffer[] recoveryOriginalSrc = new ByteBuffer[k];

        for(int i = 0; i < k; i++) {
            recoveryOriginalSrc[i] = ByteBuffer.allocateDirect(codeLength);
            recoveryOriginalSrc[i].put(StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash)[decodeIndexes[i]]);
            recoveryOriginalSrc[i].rewind();
        }

        ErasureCoderOptions ecOptions = new ErasureCoderOptions(k, m);
        ErasureDecoder decoder = new NativeRSDecoder(ecOptions);
        ByteBuffer[] output = new ByteBuffer[1];
        output[0] = ByteBuffer.allocateDirect(codeLength);
        decoder.decode(recoveryOriginalSrc, decodeIndexes, eraseIndexes, output);

        logger.debug("rymDebug: [Debug recovery] We recovered the raw data of sstable ({}) successfully.", sstHash);


        // Step 4: record the raw data locally
        // int dataFileSize = (int) sstable.getDataFileSize();
        // logger.debug("rymDebug: [Debug recovery] we load the raw sstable content of ({}), the length of decode data is ({}), sstHash is ({}), the data file size is ({}) ", sstable.descriptor, output[0].remaining(), sstable.getSSTableHashID(), dataFileSize);
        // byte[] sstContent = new byte[dataFileSize];
        // output[0].get(sstContent);
        // SSTableReader.loadRawData(sstContent, sstable.descriptor, sstable);

        // debug
        // logger.debug("rymDebug: Recovery sstHashList is ({}), parity hash list is ({}), stripe id is ({}), sstHash to replica map is ({}), sstable hash is ({}), descriptor is ({}), decode indexes are ({}), erase index is ({}), zero chunks are ({})", 
        //              ecMetadataContent.sstHashIdList, ecMetadataContent.parityHashList, ecMetadataContent.stripeId, ecMetadataContent.sstHashIdToReplicaMap, sstable.getSSTableHashID(), sstable.descriptor, decodeIndexes, eraseIndex, ecMetadataContent.zeroChunksNum);


        // Step 5: send the raw data to the peer secondary nodes
        // List<InetAddressAndPort> replicaNodes = ecMetadataContent.sstHashIdToReplicaMap.get(sstHash);
        // if(replicaNodes == null) {
        //     throw new NullPointerException(String.format("rymERROR: we cannot get replica nodes for sstable (%s)", sstHash));
        // }

        // Step 6: send the raw data to the peer secondary nodes
        // InetAddressAndPort localIP = FBUtilities.getBroadcastAddressAndPort();
        // for (int i = 1; i< replicaNodes.size(); i++) {
        //     if(!replicaNodes.get(i).equals(localIP)){
        //         String cfName = "usertable" + i;
        //         ECRecoveryForSecondary recoverySignal = new ECRecoveryForSecondary(sstHash, sstContent, cfName);
        //         recoverySignal.sendDataToSecondaryNode(replicaNodes.get(i));
        //     }
        // }

        // TODO: Wait until all data is ready.
        // Thread.sleep(5000);
        logger.debug("rymDebug: recovery for sstHash ({}) is done!", sstHash);
    }



}
