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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECResponseParityForRecoveryVerbHandler implements IVerbHandler<ECResponseParityForRecovery>{
    public static final ECResponseParityForRecoveryVerbHandler instance = new ECResponseParityForRecoveryVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(ECResponseParityForRecoveryVerbHandler.class);
    @Override
    public void doVerb(Message<ECResponseParityForRecovery> message) throws IOException {
        String sstHash = message.payload.sstHash;
        List<String> parityHashList = message.payload.parityHashList;
        String firstParityNode = message.from().getHostAddress(false);
        String localParityCodeDir = ECNetutils.getLocalParityCodeDir();
        int k = DatabaseDescriptor.getParityNodes();

        logger.debug("rymDebug: Get parity code ({}) from ({}) for sstable ({}), the recovery flag is ({})", parityHashList, message.from(), sstHash);


        for(int i = 0; i < parityHashList.size(); i++) {
            String parityCodeFileName = localParityCodeDir + parityHashList.get(i);
            if(StorageService.ossAccessObj.downloadFileAsByteArrayFromOSS(parityCodeFileName, firstParityNode)) {
                ByteBuffer parityCode = ByteBuffer.wrap(ECNetutils.readBytesFromFile(parityCodeFileName));
                StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash)[k + i].put(parityCode);
            } else {
                throw new FileNotFoundException(String.format("rymERROR: cannot download file (%s) from cloud", parityCodeFileName));
            }
        }

    }

}
