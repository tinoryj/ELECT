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
package org.apache.cassandra.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sasi.memory.KeyRangeIterator;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;

public class ReadCommandVerbHandler implements IVerbHandler<ReadCommand> {
    public static final ReadCommandVerbHandler instance = new ReadCommandVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(ReadCommandVerbHandler.class);

    public synchronized void doVerb(Message<ReadCommand> message) {
        if (StorageService.instance.isBootstrapMode()) {
            throw new RuntimeException("Cannot service reads while bootstrapping!");
        }

        ReadCommand command = message.payload;

        Token tokenForRead = (command instanceof SinglePartitionReadCommand
                ? ((SinglePartitionReadCommand) command).partitionKey().getToken()
                : ((PartitionRangeReadCommand) command).dataRange().keyRange.left.getToken());

        String rawKey = (command instanceof SinglePartitionReadCommand
                ? ((SinglePartitionReadCommand) command).partitionKey().getRawKey(command.metadata())
                : null);
        logger.debug("[Tinoryj] For token = {}, read {} recv side target table = {},", tokenForRead,
                command.isDigestQuery() == true ? "digest" : "data",
                command.metadata().name);

        if (command.metadata().keyspace.equals("ycsb")) {

            Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().id);
            SpeculativeRetryPolicy retry = cfs.metadata().params.speculativeRetry;
            ReplicaPlan.ForTokenRead replicaPlan = ReplicaPlans.forRead(keyspace, tokenForRead,
                    ConsistencyLevel.ALL, retry);

            List<InetAddressAndPort> sendRequestAddresses;
            sendRequestAddresses = replicaPlan.contacts().endpointList();
            if (sendRequestAddresses.size() != 3) {
                logger.debug("[Tinoryj] The replica plan get only {} nodes",
                        sendRequestAddresses.size());
            } else {
                logger.debug("[Tinoryj] For token = {}, recv side generate sendRequestAddresses = {}", tokenForRead,
                        sendRequestAddresses);
            }

            switch (sendRequestAddresses.indexOf(FBUtilities.getBroadcastAddressAndPort())) {
                case 0:
                    command.updateTableMetadata(
                            Keyspace.open("ycsb").getColumnFamilyStore("usertable")
                                    .metadata());
                    ColumnFilter newColumnFilter = ColumnFilter
                            .allRegularColumnsBuilder(command.metadata(), false)
                            .build();
                    command.updateColumnFilter(newColumnFilter);
                    if (command.isDigestQuery() == true) {
                        logger.error("[Tinoryj-ERROR] Remote Should not perform digest query on the primary lsm-tree");
                    }
                    break;
                case 1:
                    command.updateTableMetadata(
                            Keyspace.open("ycsb").getColumnFamilyStore("usertable1")
                                    .metadata());
                    ColumnFilter newColumnFilter1 = ColumnFilter
                            .allRegularColumnsBuilder(command.metadata(), false)
                            .build();
                    command.updateColumnFilter(newColumnFilter1);
                    if (command.isDigestQuery() == false) {
                        logger.debug(
                                "[Tinoryj] Remote Should perform online recovery on the secondary lsm-tree usertable 1");
                        // command.setIsDigestQuery(true);
                        command.setShouldPerformOnlineRecoveryDuringRead(true);
                    }
                    break;
                case 2:
                    command.updateTableMetadata(
                            Keyspace.open("ycsb").getColumnFamilyStore("usertable2")
                                    .metadata());
                    ColumnFilter newColumnFilter2 = ColumnFilter
                            .allRegularColumnsBuilder(command.metadata(), false)
                            .build();
                    command.updateColumnFilter(newColumnFilter2);
                    if (command.isDigestQuery() == false) {
                        logger.debug(
                                "[Tinoryj] Remote Should perform online recovery on the secondary lsm-tree usertable 2");
                        // command.setIsDigestQuery(true);
                        command.setShouldPerformOnlineRecoveryDuringRead(true);
                    }
                    break;
                default:
                    logger.debug("[Tinoryj] Not support replication factor larger than 3");
                    break;
            }
        }

        validateTransientStatus(message);
        MessageParams.reset();

        long timeout = message.expiresAtNanos() - message.createdAtNanos();
        command.setMonitoringTime(message.createdAtNanos(), message.isCrossNode(), timeout,
                DatabaseDescriptor.getSlowQueryTimeout(NANOSECONDS));

        if (message.trackWarnings()) {
            command.trackWarnings();
        }

        ReadResponse response;
        try (ReadExecutionController controller = command.executionController(message.trackRepairedData());
                UnfilteredPartitionIterator iterator = command.executeLocally(controller)) {
            if (iterator == null) {
                if (command.metadata().keyspace.equals("ycsb")) {
                    logger.error(
                            "[Tinoryj-ERROR] For token = {}, with {} query, ReadCommandVerbHandler Error to get data response from table {}",
                            tokenForRead,
                            command.isDigestQuery() == true ? "digest" : "data",
                            command.metadata().name, FBUtilities.getBroadcastAddressAndPort());
                }
                response = command.createEmptyResponse();
            } else {
                response = command.createResponse(iterator, controller.getRepairedDataInfo());
                if (command.metadata().keyspace.equals("ycsb")) {
                    ByteBuffer newDigest = response.digest(command);
                    String digestStr = "0x" + ByteBufferUtil.bytesToHex(newDigest);
                    if (digestStr.equals("0xd41d8cd98f00b204e9800998ecf8427e")) {
                        logger.error(
                                "[Tinoryj-ERROR] For token = {}, with {} query, ReadCommandVerbHandler Could not get non-empty response from table {}, address = {}, {}, response = {}, raw key = {}",
                                tokenForRead,
                                command.isDigestQuery() == true ? "digest" : "data",
                                command.metadata().name, FBUtilities.getBroadcastAddressAndPort(),
                                "Digest:0x" + ByteBufferUtil.bytesToHex(newDigest), response, rawKey);
                    }
                }
            }
        } catch (RejectException e) {
            if (command.metadata().keyspace.equals("ycsb")) {
                logger.error(
                        "[Tinoryj-ERROR] For token = {}, with {} query, ReadCommandVerbHandler from {}, Read Command target table is {}, target key is {}, meet errors",
                        tokenForRead,
                        command.isDigestQuery() == true ? "digest" : "data",
                        message.from(),
                        command.metadata().name);
            }
            if (!command.isTrackingWarnings())
                throw e;

            // make sure to log as the exception is swallowed
            logger.error(e.getMessage());

            response = command.createEmptyResponse();
            Message<ReadResponse> reply = message.responseWith(response);
            reply = MessageParams.addToMessage(reply);

            MessagingService.instance().send(reply, message.from());
            return;
        }

        if (!command.complete()) {
            Tracing.trace("Discarding partial response to {} (timed out)", message.from());
            MessagingService.instance().metrics.recordDroppedMessage(message,
                    message.elapsedSinceCreated(NANOSECONDS),
                    NANOSECONDS);
            return;
        }

        Tracing.trace("Enqueuing response to {}", message.from());
        Message<ReadResponse> reply = message.responseWith(response);
        reply = MessageParams.addToMessage(reply);
        MessagingService.instance().send(reply, message.from());
    }

    private void validateTransientStatus(Message<ReadCommand> message) {
        ReadCommand command = message.payload;
        if (command.metadata().isVirtual())
            return;
        Token token;

        if (command instanceof SinglePartitionReadCommand) {
            token = ((SinglePartitionReadCommand) command).partitionKey().getToken();
            // logger.debug("[Tinoryj] touch SinglePartitionReadCommand, token is {}, the
            // partition key is {}",
            // token, ((SinglePartitionReadCommand)
            // command).partitionKey().getRawKey(command.metadata()));
        } else {
            token = ((PartitionRangeReadCommand) command).dataRange().keyRange().right.getToken();
            // logger.debug("[Tinoryj] touch PartitionRangeReadCommand, token is {}",
            // token);
        }

        Replica replica = Keyspace.open(command.metadata().keyspace)
                .getReplicationStrategy()
                .getLocalReplicaFor(token);

        if (replica == null) {
            logger.warn("Received a read request from {} for a range that is not owned by the current replica {}.",
                    message.from(),
                    command);
            return;
        }

        if (!command.acceptsTransient() && replica.isTransient()) {
            MessagingService.instance().metrics.recordDroppedMessage(message,
                    message.elapsedSinceCreated(NANOSECONDS),
                    NANOSECONDS);
            throw new InvalidRequestException(
                    String.format("Attempted to serve %s data request from %s node in %s",
                            command.acceptsTransient() ? "transient" : "full",
                            replica.isTransient() ? "transient" : "full",
                            this));
        }
    }
}
