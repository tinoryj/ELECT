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
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

import static org.apache.cassandra.db.TypeSizes.sizeof;

public class LSMTreeRecovery {
    private static final Logger logger = LoggerFactory.getLogger(LSMTreeRecovery.class);
    public static final Serializer serializer = new Serializer();

    public final String rawCfPath;
    public final String targetCfName;


    public LSMTreeRecovery(String rawCfPath, String targetCfName) {
        this.rawCfPath = rawCfPath;
        this.targetCfName = targetCfName;
    }


    public void recoveryLSMTree() {

        InetAddressAndPort localIP = FBUtilities.getBroadcastAddressAndPort();
        Iterable<InetAddressAndPort> allHostsIterable = Iterables.concat(Gossiper.instance.getLiveMembers(),
        Gossiper.instance.getUnreachableMembers());
        List<InetAddressAndPort> allHosts = new ArrayList<InetAddressAndPort>();
        allHostsIterable.forEach(allHosts::add);

        int formerIndex = allHosts.indexOf(localIP) - 1;
        int nextIndex = allHosts.indexOf(localIP) + 1;

        formerIndex = formerIndex < 0 ? allHosts.size() - 1 : formerIndex;
        nextIndex = nextIndex >= allHosts.size() ? 0 : nextIndex;

        try {
            // Recovery usertable0 from the next node's usertable1
            String dataDir0 = Keyspace.open("ycsb").getColumnFamilyStore("usertable0").getDataPaths().get(0);
            LSMTreeRecovery msg0 = new LSMTreeRecovery(dataDir0, "usertable1");
            Message<LSMTreeRecovery> message0 = Message.outWithFlag(Verb.LSMTREERECOVERY_REQ, msg0, MessageFlag.CALL_BACK_ON_FAILURE);
            MessagingService.instance().send(message0, allHosts.get(nextIndex));


            // Recovery usertable1 from the next node's usertable2
            String dataDir1 = Keyspace.open("ycsb").getColumnFamilyStore("usertable1").getDataPaths().get(0);
            LSMTreeRecovery msg1 = new LSMTreeRecovery(dataDir1, "usertable2");
            Message<LSMTreeRecovery> message1 = Message.outWithFlag(Verb.LSMTREERECOVERY_REQ, msg1, MessageFlag.CALL_BACK_ON_FAILURE);
            MessagingService.instance().send(message1, allHosts.get(nextIndex));

            // Recovery usertable2 from the former node's usertable1
            String dataDir2 = Keyspace.open("ycsb").getColumnFamilyStore("usertable2").getDataPaths().get(0);
            LSMTreeRecovery msg2 = new LSMTreeRecovery(dataDir2, "usertable1");
            Message<LSMTreeRecovery> message2 = Message.outWithFlag(Verb.LSMTREERECOVERY_REQ, msg2, MessageFlag.CALL_BACK_ON_FAILURE);
            MessagingService.instance().send(message2, allHosts.get(formerIndex));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }


     public static final class Serializer implements IVersionedSerializer<LSMTreeRecovery> {


        @Override
        public void serialize(LSMTreeRecovery t, DataOutputPlus out, int version) throws IOException {
            out.writeUTF(t.rawCfPath);
            out.writeUTF(t.targetCfName);
        }

        @Override
        public LSMTreeRecovery deserialize(DataInputPlus in, int version) throws IOException {
            String rawCfPath = in.readUTF();
            String targetCfName = in.readUTF();
            return new LSMTreeRecovery(rawCfPath, targetCfName);
        }

        @Override
        public long serializedSize(LSMTreeRecovery t, int version) {
            long size = sizeof(t.rawCfPath) +
                        sizeof(t.targetCfName);

            return size;
        }

     }
}
