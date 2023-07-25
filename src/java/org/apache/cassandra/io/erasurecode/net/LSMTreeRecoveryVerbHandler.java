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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LSMTreeRecoveryVerbHandler implements IVerbHandler<LSMTreeRecovery> {

    private static final Logger logger = LoggerFactory.getLogger(LSMTreeRecoveryVerbHandler.class);
    @Override
    public void doVerb(Message<LSMTreeRecovery> message) throws IOException {
        String rawCfName = message.payload.rawCfName;
        String targetCfName = message.payload.targetCfName;
        InetAddressAndPort sourceAddress = message.from();
        for (Keyspace keyspace : Keyspace.all()){
            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores()) {

                if(cfs.getColumnFamilyName().equals(targetCfName)) {

                    List<String> dataDirs = cfs.getDataPaths();
                    for(String dir : dataDirs) {
                        if(dir.contains(targetCfName)) {
                            String userName = "yjren";
                            String host = sourceAddress.getHostAddress(false);
                            String targetDir = userName + "@" + host + rawCfName;
                            String script = "rsync -avz --progress -r " + dir + " " + targetDir;
                            ProcessBuilder processBuilder = new ProcessBuilder(script.split(" "));
                            Process process = processBuilder.start();
                            
                            try {
                                int exitCode = process.waitFor();
                                if (exitCode == 0) {
                                    logger.debug("rymDebug: Performing rsync script successfully!");
                                } else {
                                    logger.debug("rymDebug: Failed to perform rsync script!");
                                }
                            } catch (InterruptedException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                            break;
                        }
                    }
                }
                
            }
        }
        // copy the folders to source address
        
        
    }

}
