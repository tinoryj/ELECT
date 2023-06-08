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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;


public class ECResponseParityVerbHandler implements IVerbHandler<ECResponseParity>{
    public static final ECResponseParityVerbHandler instance = new ECResponseParityVerbHandler();

    @Override
    public void doVerb(Message<ECResponseParity> message) throws IOException {
        String sstHash = message.payload.sstHash;
        String parityHash = message.payload.parityHash;
        byte[] parityCode = message.payload.parityCode;
        int parityIndex = message.payload.parityIndex;

        // save it to the map
        if(StorageService.instance.globalSSTHashToParityCodeMap.get(sstHash) != null) {
            StorageService.instance.globalSSTHashToParityCodeMap.get(sstHash)[parityIndex].put(parityCode);
            // StorageService.instance.globalSSTHashToParityCodeMap.get(sstHash)[parityIndex].rewind(); 
        } else {
            throw new NullPointerException(String.format("rymERROR: We get parity code (%s) from (%s), but cannot find parity codes for sstable (%s)", parityHash, message.from(), sstHash));
        }

    }

}
