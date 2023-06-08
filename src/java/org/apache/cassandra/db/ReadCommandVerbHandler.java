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
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
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

    public void doVerb(Message<ReadCommand> message) {
        if (StorageService.instance.isBootstrapMode()) {
            throw new RuntimeException("Cannot service reads while bootstrapping!");
        }

        ReadCommand command = message.payload;

        if (command.metadata().keyspace.equals("ycsb")) {
            // logger.debug(
            // "[Tinoryj] Received read command from {}, target table is {}, target key is
            // {}, key token is {}",
            // message.from(),
            // command.metadata().name,
            // command instanceof SinglePartitionReadCommand
            // ? ((SinglePartitionReadCommand) command).partitionKey()
            // .getRawKey(command.metadata())
            // : null,
            // command instanceof SinglePartitionReadCommand
            // ? ((SinglePartitionReadCommand) command).partitionKey()
            // .getToken()
            // : null);
            // Update read command to the correct table
            List<InetAddress> sendRequestAddresses;
            if (command instanceof SinglePartitionReadCommand) {
                ByteBuffer targetReadKey = (command instanceof SinglePartitionReadCommand
                        ? ((SinglePartitionReadCommand) command).partitionKey().getKey()
                        : null);
                sendRequestAddresses = StorageService.instance.getNaturalEndpoints(command
                        .metadata().keyspace,
                        targetReadKey);
            } else {
                Token token = ((PartitionRangeReadCommand) command).dataRange().keyRange().right.getToken();
                sendRequestAddresses = StorageService.instance.getNaturalEndpointsForToken(command
                        .metadata().keyspace,
                        token);
            }

            switch (sendRequestAddresses.indexOf(FBUtilities.getJustBroadcastAddress())) {
                case 0:
                    command.updateTableMetadata(
                            Keyspace.open("ycsb").getColumnFamilyStore("usertable")
                                    .metadata());
                    ColumnFilter newColumnFilter = ColumnFilter
                            .allRegularColumnsBuilder(command.metadata(), false)
                            .build();
                    command.updateColumnFilter(newColumnFilter);
                    break;
                case 1:
                    command.updateTableMetadata(
                            Keyspace.open("ycsb").getColumnFamilyStore("usertable1")
                                    .metadata());
                    ColumnFilter newColumnFilter1 = ColumnFilter
                            .allRegularColumnsBuilder(command.metadata(), false)
                            .build();
                    command.updateColumnFilter(newColumnFilter1);
                    break;
                case 2:
                    command.updateTableMetadata(
                            Keyspace.open("ycsb").getColumnFamilyStore("usertable2")
                                    .metadata());
                    ColumnFilter newColumnFilter2 = ColumnFilter
                            .allRegularColumnsBuilder(command.metadata(), false)
                            .build();
                    command.updateColumnFilter(newColumnFilter2);
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
                logger.error(
                        "[Tinoryj-ERROR] ReadCommandVerbHandler Could not get {} response from table {}",
                        command.isDigestQuery() ? "digest" : "data",
                        command.metadata().name, FBUtilities.getBroadcastAddressAndPort());
                response = command.createEmptyResponse();
            } else {
                response = command.createResponse(iterator, controller.getRepairedDataInfo());
                ByteBuffer newDigest = response.digest(command);
                String digestStr = "0x" + ByteBufferUtil.bytesToHex(newDigest);
                if (digestStr.equals("0xd41d8cd98f00b204e9800998ecf8427e")) {
                    logger.error(
                            "[Tinoryj-ERROR] ReadCommandVerbHandler Could not get non-empty {} response from table {}, {}",
                            command.isDigestQuery() ? "digest" : "data",
                            command.metadata().name, FBUtilities.getBroadcastAddressAndPort(),
                            "Digest:0x" + ByteBufferUtil.bytesToHex(newDigest));
                }
            }
            // if (command.metadata().keyspace.equals("ycsb")) {
            // logger.debug(
            // "[Tinoryj] ReadCommandVerbHandler from {}, Read Command target table is {},
            // target key is {}, key token is {}, response is {}",
            // message.from(),
            // command.metadata().name,
            // command instanceof SinglePartitionReadCommand
            // ? ((SinglePartitionReadCommand) command).partitionKey()
            // .getRawKey(command.metadata())
            // : null,
            // command instanceof SinglePartitionReadCommand
            // ? ((SinglePartitionReadCommand) command).partitionKey()
            // .getToken()
            // : null,
            // response.toDebugString(command,
            // command instanceof SinglePartitionReadCommand
            // ? ((SinglePartitionReadCommand) command)
            // .partitionKey()
            // : null));
            // }
        } catch (RejectException e) {
            if (command.metadata().keyspace.equals("ycsb")) {
                logger.error(
                        "[Tinoryj-ERROR] ReadCommandVerbHandler from {}, Read Command target table is {}, target key is {}, key token is {}, meet errors",
                        message.from(),
                        command.metadata().name,
                        command instanceof SinglePartitionReadCommand
                                ? ((SinglePartitionReadCommand) command).partitionKey()
                                        .getRawKey(command.metadata())
                                : null,
                        command instanceof SinglePartitionReadCommand
                                ? ((SinglePartitionReadCommand) command).partitionKey()
                                        .getToken()
                                : null);
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
