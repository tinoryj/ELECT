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
package org.apache.cassandra.service.reads;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.reads.repair.NoopReadRepair;
import org.apache.cassandra.utils.ByteBufferUtil;

import static com.google.common.collect.Iterables.any;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class DigestResolver<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>>
        extends ResponseResolver<E, P> {
    private volatile Message<ReadResponse> dataResponse;

    public DigestResolver(ReadCommand command, ReplicaPlan.Shared<E, P> replicaPlan, long queryStartNanoTime) {
        super(command, replicaPlan, queryStartNanoTime);
        Preconditions.checkArgument(command instanceof SinglePartitionReadCommand,
                "DigestResolver can only be used with SinglePartitionReadCommand commands");
    }

    @Override
    public void preprocess(Message<ReadResponse> message) {
        super.preprocess(message);
        Replica replica = replicaPlan().lookup(message.from());
        if (dataResponse == null && !message.payload.isDigestResponse() && replica.isFull())
            dataResponse = message;
    }

    @VisibleForTesting
    public boolean hasTransientResponse() {
        return hasTransientResponse(responses.snapshot());
    }

    private boolean hasTransientResponse(Collection<Message<ReadResponse>> responses) {
        return any(responses,
                msg -> !msg.payload.isDigestResponse()
                        && replicaPlan().lookup(msg.from()).isTransient());
    }

    public PartitionIterator getData() {
        Collection<Message<ReadResponse>> responses = this.responses.snapshot();

        if (!hasTransientResponse(responses)) {
            return UnfilteredPartitionIterators.filter(dataResponse.payload.makeIterator(command), command.nowInSec());
        } else {
            // This path can be triggered only if we've got responses from full replicas and
            // they match, but
            // transient replica response still contains data, which needs to be reconciled.
            DataResolver<E, P> dataResolver = new DataResolver<>(command, replicaPlan, NoopReadRepair.instance,
                    queryStartNanoTime);

            dataResolver.preprocess(dataResponse);
            // Reconcile with transient replicas
            for (Message<ReadResponse> response : responses) {
                Replica replica = replicaPlan().lookup(response.from());
                if (replica.isTransient())
                    dataResolver.preprocess(response);
            }

            return dataResolver.resolve();
        }
    }

    public boolean responsesMatch() {
        long start = nanoTime();

        // validate digests against each other; return false immediately on mismatch.
        ByteBuffer digest = null;
        Collection<Message<ReadResponse>> snapshot = responses.snapshot();
        assert snapshot.size() > 0 : "Attempted response match comparison while no responses have been received.";
        if (snapshot.size() == 1) {
            return true;
        }
        // TODO: should also not calculate if only one full node
        Boolean isDigestMatchFlag = true;
        ByteBuffer digestSet[] = new ByteBuffer[snapshot.size()];
        ArrayList<InetAddressAndPort> endpoints = new ArrayList<>();
        int digestIndex = 0;
        InetAddressAndPort dataResponseAddress = null;
        int dataResponseIndex = 0;
        for (Message<ReadResponse> message : snapshot) {
            if (replicaPlan().lookup(message.from()).isTransient())
                continue;

            ByteBuffer newDigest = message.payload.digest(command);
            digestSet[digestIndex] = newDigest;
            if (digest == null) {
                digest = newDigest;
            }
            if (!digest.equals(newDigest)) {
                // rely on the fact that only single partition queries use digests
                isDigestMatchFlag = false;
            }
            endpoints.add(message.from());
            digestIndex++;
            if (message.payload.isDigestResponse() == false) {
                dataResponseAddress = message.from();
                dataResponseIndex = digestIndex - 1;
            }
        }
        int noDataCount = 0;
        boolean noDataResponseFlag = false;
        for (int i = 0; i < digestIndex; i++) {
            // logger.debug(
            // "[Tinoryj] Read operation get digest from {}, digest = {}",
            // endpoints.get(i), "0x" + ByteBufferUtil.bytesToHex(digestSet[i]));
            String digestStr = "0x" + ByteBufferUtil.bytesToHex(digestSet[i]);
            if (digestStr.equals("0xd41d8cd98f00b204e9800998ecf8427e")) {
                noDataCount++;
                if (i == dataResponseIndex) {
                    noDataResponseFlag = true;
                }
            }
        }
        if (noDataCount != 0) {
            if (command.metadata().keyspace.equals("ycsb")) {
                // Skip digest failure with up to two empty data (since redundancy transition
                // may remove the data in secondary nodes).
                if (noDataCount != snapshot.size()) {
                    if (noDataResponseFlag) {
                        logger.error("[Tinoryj-ERROR] Read get empty data response from {}.",
                                dataResponseAddress);
                        return false;
                    } else {
                        // Since no need to get data from all nodes, we can return true.
                        return true;
                    }
                } else {
                    logger.error(
                            "[Tinoryj-ERROR] Read operation get no success response.");
                    return false;
                }
            } else {
                // Perform read repair when hash not match and data is invalid.
                return false;
            }
        }

        if (isDigestMatchFlag == false) {
            // Perform read repair when hash not match but they all valid (inconsistency).
            return false;
        }

        if (logger.isTraceEnabled())
            logger.trace("responsesMatch: {} ms.", TimeUnit.NANOSECONDS.toMillis(nanoTime() - start));

        return true;
    }

    public boolean isDataPresent() {
        return dataResponse != null;
    }

    public DigestResolverDebugResult[] getDigestsByEndpoint() {
        DigestResolverDebugResult[] ret = new DigestResolverDebugResult[responses.size()];
        for (int i = 0; i < responses.size(); i++) {
            Message<ReadResponse> message = responses.get(i);
            ReadResponse response = message.payload;
            String digestHex = ByteBufferUtil.bytesToHex(response.digest(command));
            ret[i] = new DigestResolverDebugResult(message.from(), digestHex, message.payload.isDigestResponse());
        }
        return ret;
    }

    public static class DigestResolverDebugResult {
        public InetAddressAndPort from;
        public String digestHex;
        public boolean isDigestResponse;

        private DigestResolverDebugResult(InetAddressAndPort from, String digestHex, boolean isDigestResponse) {
            this.from = from;
            this.digestHex = digestHex;
            this.isDigestResponse = isDigestResponse;
        }
    }
}
