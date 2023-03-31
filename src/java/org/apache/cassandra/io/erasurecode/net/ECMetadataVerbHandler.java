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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECMetadataVerbHandler implements IVerbHandler<ECMetadata> {
    public static final ECMetadataVerbHandler instance = new ECMetadataVerbHandler();
    private static final String ecMetadataDir = System.getProperty("user.dir") + "/data/ECMetadata/";
    public static List<ECMetadata> ecMetadatas = new ArrayList<ECMetadata>();

    private static final Logger logger = LoggerFactory.getLogger(ECMetadataVerbHandler.class);

    @Override
    public void doVerb(Message<ECMetadata> message) throws IOException {
        // receive metadata and record it to files (append)
        ecMetadatas.add(message.payload);
        logger.debug("rymDebug: received metadata: {}, {},{},{}", message.payload,
                message.payload.sstHashIdList, message.payload.primaryNodes, message.payload.relatedNodes);

        // decode the message
        byte[] byteArray = message.payload.sstHashIdToReplicaMapStr.getBytes();
        ByteArrayInputStream byteInStream = new ByteArrayInputStream(byteArray);
        ObjectInputStream objectInStream = new ObjectInputStream(byteInStream);

        try {
            Map<String, List<InetAddressAndPort>> sstHashIdToReplicaMap = 
            (Map<String, List<InetAddressAndPort>>) objectInStream.readObject();
            
            logger.debug("rymDebug: got sstHashIdToReplicaMap: {} ", sstHashIdToReplicaMap);

            for (Map.Entry<String, List<InetAddressAndPort>> entry : sstHashIdToReplicaMap.entrySet()) {
                String sstableHash = entry.getKey();
                InetAddressAndPort localIP = FBUtilities.getBroadcastAddressAndPort();
                if (!localIP.equals(entry.getValue().get(0)) && entry.getValue().contains(localIP)) {
                    String ks = message.payload.keyspace;
                    String cfName = message.payload.cfName;
                    Collection<ColumnFamilyStore> cfs = Keyspace.open(ks).getColumnFamilyStores();
                    for (ColumnFamilyStore cf : cfs) {
                        if (!cf.name.equals(cfName)) {
                            boolean flag = false;
                            // traverse all the sstables
                            Iterable<SSTableReader> sstables = cf.getSSTables(SSTableSet.LIVE);
                            for (SSTableReader sstable : sstables) {
                                if (sstable.getSSTableHashID().equals(sstableHash)) {
                                    // delete sstables
                                    sstable.replaceDatabyECMetadata(message.payload);
                                    // TODO: send ec_ready signal

                                    flag = true;
                                    break;
                                }
                            }

                            if (!flag) {
                                logger.error("rymError: cannot find this fucking sstable");
                            }
                        }
                    }

                }

            }

        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
