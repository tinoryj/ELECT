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
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;

import java.io.File;
import java.io.FileWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECParityNodeVerbHandler implements IVerbHandler<ECParityNode> {


    public static final ECParityNodeVerbHandler instance = new ECParityNodeVerbHandler();

    private static final String parityCodeDir = System.getProperty("user.dir")+"/data/parityHashes/";
    private static final Logger logger = LoggerFactory.getLogger(ECParityNodeVerbHandler.class);

    /*
     * TODO List:
     * 1. Receive erasure code from a parity node, and record it.
     */
    @Override
    public void doVerb(Message<ECParityNode> message) throws IOException {
        logger.debug("rymDebug: Received message: {}", message.payload.parityHash);
        try {
                // File parityCodeFile = new File(parityCodeDir + String.valueOf(message.payload.parityHash));
                FileChannel fileChannel = FileChannel.open(Paths.get(parityCodeDir, message.payload.parityHash),
                                                            StandardOpenOption.WRITE,
                                                             StandardOpenOption.CREATE);
                fileChannel.write(message.payload.parityCode);
                fileChannel.close();
                // if (!parityCodeFile.exists()) {
                //     parityCodeFile.createNewFile();
                // }
                // logger.debug("rymDebug: parityCodeFile.getName is {}", parityCodeFile.getAbsolutePath());
                // FileWriter fileWritter = new FileWriter(parityCodeFile.getAbsolutePath(),true);
                // fileWritter.write(message.payload.parityCode.toString());
                // fileWritter.close();
            } 
        catch (IOException e) {
                logger.error("rymError: Perform erasure code error", e);
            }
    }

    public static void main(String[] args) {
        File parityCodeFile = new File(parityCodeDir + String.valueOf("parityCodeTest"));
        
        if (!parityCodeFile.exists()) {
            try {
                parityCodeFile.createNewFile();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        logger.debug("rymDebug: parityCodeFile.getName is {}", parityCodeFile.getAbsolutePath());
    }

}
