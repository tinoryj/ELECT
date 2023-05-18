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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;

public class ECRequestParityVerbHandler implements IVerbHandler<ECRequestParity> {
    public static final ECRequestParityVerbHandler instance = new ECRequestParityVerbHandler();

    @Override
    public void doVerb(Message<ECRequestParity> message) throws IOException {
        
        String parityHash = message.payload.parityHash;
        String sstHash = message.payload.sstHash;
        int parityIndex = message.payload.parityIndex;
        String receivedParityCodeDir = ECNetutils.getReceivedParityCodeDir();
        String filePath = receivedParityCodeDir + parityHash;

        byte[] parityCode = ECNetutils.readBytesFromFile(filePath);
        
        
        // response this parityCode to to source node
        ECResponseParity response = new ECResponseParity(parityHash, sstHash, parityCode, parityIndex);
        response.responseParity(message.from());
        
        // delete parity code file locally
        ECNetutils.deleteFileByName(filePath);
    }




}
