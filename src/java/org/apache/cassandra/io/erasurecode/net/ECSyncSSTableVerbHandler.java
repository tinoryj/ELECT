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
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECSyncSSTableVerbHandler implements IVerbHandler<ECSyncSSTable>{
    public static final ECSyncSSTableVerbHandler instance = new ECSyncSSTableVerbHandler();
    private static final Logger logger = LoggerFactory.getLogger(ECSyncSSTableVerbHandler.class);
    @Override
    public void doVerb(Message<ECSyncSSTable> message) throws IOException {
        // collect sstcontent
        List<DecoratedKey> keyList = new ArrayList<DecoratedKey>();
        message.payload.allKey.forEach(keyList::add);
        StorageService.instance.globalSSTMap.putIfAbsent(message.payload.sstHashID, 
                                                         keyList);
        logger.debug("rymDebug: globalSSTMap is {}", StorageService.instance.globalSSTMap);
    }
}
