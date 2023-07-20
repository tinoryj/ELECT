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


public class ECResponseDataVerbHandler implements IVerbHandler<ECResponseData>{
    public static final ECResponseDataVerbHandler instance = new ECResponseDataVerbHandler();

    @Override
    public synchronized void doVerb(Message<ECResponseData> message) throws IOException {
        String sstHash = message.payload.sstHash;
        byte[] rawData = message.payload.rawData;
        int index = message.payload.index;

        // save it to the map
        if(StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash) != null) {
            StorageService.instance.globalSSTHashToErasureCodesMap.get(sstHash)[index].put(rawData);
        } else {
            throw new NullPointerException(String.format("rymERROR: We cannot find data blocks for sstable (%s)", message.from(), sstHash));
        }

    }

}
