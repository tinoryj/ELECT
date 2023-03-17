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

import java.util.List;
import java.util.Map;

import org.apache.cassandra.locator.InetAddressAndPort;

public class ECMetadata {

    private static String stripeId;
    private static List<String> sstContentHashList;
    private static List<String> sstParityHashList;

    private static List<InetAddressAndPort> primaryNodes;
    private static List<InetAddressAndPort> secondaryNodes;
    private static List<InetAddressAndPort> tertiaryNodes;
    private static ECMessage[] messages;
    public static final ECMetadata instance = new ECMetadata();

    public ECMetadata() {
        
    }

    ECMetadata generateMetadata(ECMessage[] messages) {
        this.messages = messages;

        return this;
    }

}
