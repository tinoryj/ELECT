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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.transform.DuplicateRowChecker;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy.LocalReadRunnable;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.service.StorageService;
import static com.google.common.collect.Iterables.all;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import java.lang.reflect.Field;

/**
 * Sends a read request to the replicas needed to satisfy a given
 * ConsistencyLevel.
 *
 * Optionally, may perform additional requests to provide redundancy against
 * replica failure:
 * AlwaysSpeculatingReadExecutor will always send a request to one extra
 * replica, while
 * SpeculatingReadExecutor will wait until it looks like the original request is
 * in danger
 * of timing out before performing extra reads.
 */
public abstract class AbstractReadExecutor {
    private static final Logger logger = LoggerFactory.getLogger(AbstractReadExecutor.class);

    protected ReadCommand command;
    private final ReplicaPlan.SharedForTokenRead replicaPlan;
    protected final ReadRepair<EndpointsForToken, ReplicaPlan.ForTokenRead> readRepair;
    protected final DigestResolver<EndpointsForToken, ReplicaPlan.ForTokenRead> digestResolver;
    protected final ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> handler;
    protected final TraceState traceState;
    protected ColumnFamilyStore cfs;
    protected final long queryStartNanoTime;
    private final int initialDataRequestCount;
    protected volatile PartitionIterator result = null;
    // public static List<InetAddress> sendRequestAddresses;
    // public static Token targetReadToken;

    public final String primaryLSMTreeName = "usertable";
    public final String secondaryLSMTreeName1 = "usertable1";
    public final String secondaryLSMTreeName2 = "usertable2";

    AbstractReadExecutor(ColumnFamilyStore cfs, ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan,
            int initialDataRequestCount, long queryStartNanoTime) {
        this.command = command;
        this.replicaPlan = ReplicaPlan.shared(replicaPlan);
        this.initialDataRequestCount = initialDataRequestCount;
        // the ReadRepair and DigestResolver both need to see our updated
        this.readRepair = ReadRepair.create(command, this.replicaPlan, queryStartNanoTime);
        this.digestResolver = new DigestResolver<>(command, this.replicaPlan, queryStartNanoTime);
        this.handler = new ReadCallback<>(digestResolver, command, this.replicaPlan, queryStartNanoTime);
        this.cfs = cfs;
        this.traceState = Tracing.instance.get();
        this.queryStartNanoTime = queryStartNanoTime;

        // Set the digest version (if we request some digests). This is the smallest
        // version amongst all our target replicas since new nodes
        // knows how to produce older digest but the reverse is not true.
        // TODO: we need this when talking with pre-3.0 nodes. So if we preserve the
        // digest format moving forward, we can get rid of this once
        // we stop being compatible with pre-3.0 nodes.
        int digestVersion = MessagingService.current_version;
        for (Replica replica : replicaPlan.contacts())
            digestVersion = Math.min(digestVersion, MessagingService.instance().versions.get(replica.endpoint()));
        command.setDigestVersion(digestVersion);
    }

    public DecoratedKey getKey() {
        Preconditions.checkState(command instanceof SinglePartitionReadCommand,
                "Can only get keys for SinglePartitionReadCommand");
        return ((SinglePartitionReadCommand) command).partitionKey();
    }

    public ReadRepair<EndpointsForToken, ReplicaPlan.ForTokenRead> getReadRepair() {
        return readRepair;
    }

    protected void makeFullDataRequests(ReplicaCollection<?> replicas) {
        assert all(replicas, Replica::isFull);
        makeRequests(command, replicas);
    }

    protected void makeTransientDataRequests(Iterable<Replica> replicas) {
        makeRequests(command.copyAsTransientQuery(replicas), replicas);
    }

    protected void makeDigestRequests(Iterable<Replica> replicas) {
        assert all(replicas, Replica::isFull);
        // only send digest requests to full replicas, send data requests instead to the
        // transient replicas
        makeRequests(command.copyAsDigestQuery(replicas), replicas);
    }

    private void makeRequests(ReadCommand readCommand, Iterable<Replica> replicas) {
        boolean hasLocalEndpoint = false;
        Message<ReadCommand> message = null;
        for (Replica replica : replicas) {
            assert replica.isFull() || readCommand.acceptsTransient();
            InetAddressAndPort endpoint = replica.endpoint();

            if (traceState != null)
                traceState.trace("reading {} from {}", readCommand.isDigestQuery() ? "digest" : "data", endpoint);

            if (replica.isSelf()) {
                hasLocalEndpoint = true;
                continue;
            }
            if (null == message)
                message = readCommand.createMessage(false);

            MessagingService.instance().sendWithCallback(message, endpoint, handler);
        }

        // We delay the local (potentially blocking) read till the end to avoid stalling
        // remote requests.
        if (hasLocalEndpoint) {
            if (readCommand.metadata().keyspace.equals("ycsb")) {
                List<InetAddress> sendRequestAddresses;
                Token token = (command instanceof SinglePartitionReadCommand
                        ? ((SinglePartitionReadCommand) command).partitionKey().getToken()
                        : ((PartitionRangeReadCommand) command).dataRange().keyRange().right.getToken());
                sendRequestAddresses = StorageService.instance.getNaturalEndpointsForToken(command
                        .metadata().keyspace,
                        token);
                switch (sendRequestAddresses.indexOf(FBUtilities.getJustBroadcastAddress())) {
                    case 0:
                        // In case received request is not for primary LSM tree
                        readCommand.updateTableMetadata(
                                Keyspace.open("ycsb").getColumnFamilyStore("usertable")
                                        .metadata());
                        ColumnFilter newColumnFilter = ColumnFilter
                                .allRegularColumnsBuilder(readCommand.metadata(), false)
                                .build();
                        readCommand.updateColumnFilter(newColumnFilter);
                        if (readCommand.isDigestQuery() == true) {
                            logger.error("[Tinoryj-ERROR] Should not perform digest query on the primary lsm-tree");
                        }
                        break;
                    case 1:
                        readCommand.updateTableMetadata(
                                Keyspace.open("ycsb").getColumnFamilyStore("usertable1")
                                        .metadata());
                        ColumnFilter newColumnFilter1 = ColumnFilter
                                .allRegularColumnsBuilder(readCommand.metadata(), false)
                                .build();
                        readCommand.updateColumnFilter(newColumnFilter1);
                        this.command = readCommand;
                        this.cfs = Keyspace.open("ycsb").getColumnFamilyStore("usertable1");
                        if (readCommand.isDigestQuery() == false) {
                            logger.debug(
                                    "[Tinoryj] Local Should perform online recovery on the secondary lsm-tree usertable 1");
                            readCommand.setShouldPerformOnlineRecoveryDuringRead(true);
                        }
                        break;
                    case 2:
                        readCommand.updateTableMetadata(
                                Keyspace.open("ycsb").getColumnFamilyStore("usertable2")
                                        .metadata());
                        ColumnFilter newColumnFilter2 = ColumnFilter
                                .allRegularColumnsBuilder(readCommand.metadata(), false)
                                .build();
                        readCommand.updateColumnFilter(newColumnFilter2);
                        this.command = readCommand;
                        this.cfs = Keyspace.open("ycsb").getColumnFamilyStore("usertable2");
                        if (readCommand.isDigestQuery() == false) {
                            logger.debug(
                                    "[Tinoryj] Local Should perform online recovery on the secondary lsm-tree usertable 2");
                            readCommand.setShouldPerformOnlineRecoveryDuringRead(true);
                        }
                        break;
                    default:
                        logger.debug("[Tinoryj] Not support replication factor larger than 3");
                        break;
                }
            }
            Stage.READ.maybeExecuteImmediately(new LocalReadRunnable(readCommand, handler));
        }
    }

    private void makeRequestsForELECT(ReadCommand readCommand, Iterable<Replica> replicas) {
        List<InetAddress> sendRequestAddresses;
        Token token = (readCommand instanceof SinglePartitionReadCommand
                ? ((SinglePartitionReadCommand) readCommand).partitionKey().getToken()
                : ((PartitionRangeReadCommand) readCommand).dataRange().keyRange().right.getToken());
        sendRequestAddresses = StorageService.instance.getNaturalEndpointsForToken(readCommand
                .metadata().keyspace,
                token);

        boolean hasLocalEndpoint = false;
        Message<ReadCommand> message = null;
        for (int i = 0; i < sendRequestAddresses.size(); i++) {
            InetAddressAndPort endpoint = sendRequestAddresses[i];

            if (traceState != null)
                traceState.trace("reading {} from {}", readCommand.isDigestQuery() ? "digest" : "data", endpoint);

            if (replica.isSelf()) {
                hasLocalEndpoint = true;
                continue;
            }

            switch (sendRequestAddresses.indexOf(FBUtilities.getJustBroadcastAddress())) {
                case 0:
                    // In case received request is not for primary LSM tree
                    readCommand.updateTableMetadata(
                            Keyspace.open("ycsb").getColumnFamilyStore("usertable")
                                    .metadata());
                    ColumnFilter newColumnFilter = ColumnFilter
                            .allRegularColumnsBuilder(readCommand.metadata(), false)
                            .build();
                    readCommand.updateColumnFilter(newColumnFilter);
                    if (readCommand.isDigestQuery() == true) {
                        logger.error("[Tinoryj-ERROR] Should not perform digest query on the primary lsm-tree");
                    }
                    break;
                case 1:
                    readCommand.updateTableMetadata(
                            Keyspace.open("ycsb").getColumnFamilyStore("usertable1")
                                    .metadata());
                    ColumnFilter newColumnFilter1 = ColumnFilter
                            .allRegularColumnsBuilder(readCommand.metadata(), false)
                            .build();
                    readCommand.updateColumnFilter(newColumnFilter1);
                    this.command = readCommand;
                    this.cfs = Keyspace.open("ycsb").getColumnFamilyStore("usertable1");
                    if (readCommand.isDigestQuery() == false) {
                        logger.debug(
                                "[Tinoryj] Local Should perform online recovery on the secondary lsm-tree usertable 1");
                        readCommand.setShouldPerformOnlineRecoveryDuringRead(true);
                    }
                    break;
                case 2:
                    readCommand.updateTableMetadata(
                            Keyspace.open("ycsb").getColumnFamilyStore("usertable2")
                                    .metadata());
                    ColumnFilter newColumnFilter2 = ColumnFilter
                            .allRegularColumnsBuilder(readCommand.metadata(), false)
                            .build();
                    readCommand.updateColumnFilter(newColumnFilter2);
                    this.command = readCommand;
                    this.cfs = Keyspace.open("ycsb").getColumnFamilyStore("usertable2");
                    if (readCommand.isDigestQuery() == false) {
                        logger.debug(
                                "[Tinoryj] Local Should perform online recovery on the secondary lsm-tree usertable 2");
                        readCommand.setShouldPerformOnlineRecoveryDuringRead(true);
                    }
                    break;
                default:
                    logger.debug("[Tinoryj] Not support replication factor larger than 3");
                    break;
            }
            if (null == message)
                message = readCommand.createMessage(false);

            MessagingService.instance().sendWithCallback(message, endpoint, handler);
        }

        // We delay the local (potentially blocking) read till the end to avoid stalling
        // remote requests.
        if (hasLocalEndpoint) {
            switch (sendRequestAddresses.indexOf(FBUtilities.getJustBroadcastAddress())) {
                case 0:
                    // In case received request is not for primary LSM tree
                    readCommand.updateTableMetadata(
                            Keyspace.open("ycsb").getColumnFamilyStore("usertable")
                                    .metadata());
                    ColumnFilter newColumnFilter = ColumnFilter
                            .allRegularColumnsBuilder(readCommand.metadata(), false)
                            .build();
                    readCommand.updateColumnFilter(newColumnFilter);
                    if (readCommand.isDigestQuery() == true) {
                        logger.error("[Tinoryj-ERROR] Should not perform digest query on the primary lsm-tree");
                    }
                    break;
                case 1:
                    readCommand.updateTableMetadata(
                            Keyspace.open("ycsb").getColumnFamilyStore("usertable1")
                                    .metadata());
                    ColumnFilter newColumnFilter1 = ColumnFilter
                            .allRegularColumnsBuilder(readCommand.metadata(), false)
                            .build();
                    readCommand.updateColumnFilter(newColumnFilter1);
                    this.command = readCommand;
                    this.cfs = Keyspace.open("ycsb").getColumnFamilyStore("usertable1");
                    if (readCommand.isDigestQuery() == false) {
                        logger.debug(
                                "[Tinoryj] Local Should perform online recovery on the secondary lsm-tree usertable 1");
                        readCommand.setShouldPerformOnlineRecoveryDuringRead(true);
                    }
                    break;
                case 2:
                    readCommand.updateTableMetadata(
                            Keyspace.open("ycsb").getColumnFamilyStore("usertable2")
                                    .metadata());
                    ColumnFilter newColumnFilter2 = ColumnFilter
                            .allRegularColumnsBuilder(readCommand.metadata(), false)
                            .build();
                    readCommand.updateColumnFilter(newColumnFilter2);
                    this.command = readCommand;
                    this.cfs = Keyspace.open("ycsb").getColumnFamilyStore("usertable2");
                    if (readCommand.isDigestQuery() == false) {
                        logger.debug(
                                "[Tinoryj] Local Should perform online recovery on the secondary lsm-tree usertable 2");
                        readCommand.setShouldPerformOnlineRecoveryDuringRead(true);
                    }
                    break;
                default:
                    logger.debug("[Tinoryj] Not support replication factor larger than 3");
                    break;
            }
            Stage.READ.maybeExecuteImmediately(new LocalReadRunnable(readCommand, handler));
        }
    }

    public static void printStackTace(String msg) {
        logger.debug("stack trace {}", new Exception(msg));
    }

    /**
     * Perform additional requests if it looks like the original will time out. May
     * block while it waits
     * to see if the original requests are answered first.
     */
    public abstract void maybeTryAdditionalReplicas();

    /**
     * send the initial set of requests
     */
    public void executeAsync() {
        EndpointsForToken selected = replicaPlan().contacts();
        // Normal read path for Cassandra system tables.
        EndpointsForToken fullDataRequests = selected.filter(Replica::isFull, initialDataRequestCount);
        makeFullDataRequests(fullDataRequests); // Tinoryj-> to read the primary replica.
        // logger.debug("[Tinoryj] Try to read {} data on the primary replica, {}",
        // fullDataRequests.size(), fullDataRequests);
        makeTransientDataRequests(selected.filterLazily(Replica::isTransient));
        // Tinoryj-> to read the possible secondary replica.
        makeDigestRequests(selected.filterLazily(r -> r.isFull() && !fullDataRequests.contains(r)));
        // logger.debug("[Tinoryj] Try to read digest on the secondary replica, {}",
        // selected.filterLazily(r -> r.isFull() && !fullDataRequests.contains(r)));
    }

    /**
     * @return an executor appropriate for the configured speculative read policy
     */
    public static AbstractReadExecutor getReadExecutor(SinglePartitionReadCommand command,
            ConsistencyLevel consistencyLevel, long queryStartNanoTime) throws UnavailableException {
        Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().id);
        SpeculativeRetryPolicy retry = cfs.metadata().params.speculativeRetry;

        ReplicaPlan.ForTokenRead replicaPlan = ReplicaPlans.forRead(keyspace, command.partitionKey().getToken(),
                consistencyLevel, retry);

        // if (keyspace.getName().equals("ycsb")) {
        // targetReadToken = command.partitionKey().getToken();
        // sendRequestAddresses =
        // StorageService.instance.getNaturalEndpoints(command.metadata().keyspace,
        // command.partitionKey().getKey());
        // if (sendRequestAddresses.size() != 3) {
        // logger.debug("[Tinoryj-ERROR] sendRequestAddressesAndPorts.size() != 3");
        // }
        // boolean isReplicaPlanMatchToNaturalEndpointFlag = true;
        // for (int i = 0; i < replicaPlan.contacts().endpointList().size(); i++) {
        // if
        // (!replicaPlan.contacts().endpointList().get(i).getAddress().equals(sendRequestAddresses.get(i)))
        // {
        // isReplicaPlanMatchToNaturalEndpointFlag = false;
        // }
        // }
        // if (isReplicaPlanMatchToNaturalEndpointFlag == false) {
        // logger.debug(
        // "[Tinoryj-ERROR] for key token = {}, the primary node is not the first node
        // in the natural storage node list. The replication plan for read is {},
        // natural storage node list = {}",
        // command.partitionKey().getToken(),
        // replicaPlan.contacts().endpointList(),
        // sendRequestAddresses);
        // }
        // }

        // Speculative retry is disabled *OR*
        // 11980: Disable speculative retry if using EACH_QUORUM in order to prevent
        // miscounting DC responses
        if (retry.equals(NeverSpeculativeRetryPolicy.INSTANCE) || consistencyLevel == ConsistencyLevel.EACH_QUORUM) {
            // logger.debug("[Tinoryj] NeverSpeculatingReadExecutor is in use");
            return new NeverSpeculatingReadExecutor(cfs, command, replicaPlan, queryStartNanoTime, false);
        }

        // There are simply no extra replicas to speculate.
        // Handle this separately so it can record failed attempts to speculate due to
        // lack of replicas
        if (replicaPlan.contacts().size() == replicaPlan.readCandidates().size())

        {
            boolean recordFailedSpeculation = consistencyLevel != ConsistencyLevel.ALL;
            // logger.debug("[Tinoryj] NeverSpeculatingReadExecutor is in use");
            return new NeverSpeculatingReadExecutor(cfs, command, replicaPlan, queryStartNanoTime,
                    recordFailedSpeculation);
        }

        if (retry.equals(AlwaysSpeculativeRetryPolicy.INSTANCE)) {
            // logger.debug("[Tinoryj] AlwaysSpeculatingReadExecutor is in use");
            return new AlwaysSpeculatingReadExecutor(cfs, command, replicaPlan, queryStartNanoTime);
        } else {
            // PERCENTILE
            // or
            // logger.debug("[Tinoryj] SpeculatingReadExecutor is in use");
            // CUSTOM.
            return new SpeculatingReadExecutor(cfs, command, replicaPlan, queryStartNanoTime);
        }

    }

    public boolean hasLocalRead() {
        return replicaPlan().lookup(FBUtilities.getBroadcastAddressAndPort()) != null;
    }

    /**
     * Returns true if speculation should occur and if it should then block until it
     * is time to
     * send the speculative reads
     */
    boolean shouldSpeculateAndMaybeWait() {
        // no latency information, or we're overloaded
        if (cfs.sampleReadLatencyMicros > command.getTimeout(MICROSECONDS)) {
            if (logger.isTraceEnabled())
                logger.trace("Decided not to speculate as {} > {}", cfs.sampleReadLatencyMicros,
                        command.getTimeout(MICROSECONDS));
            return false;
        }

        if (logger.isTraceEnabled())
            logger.trace("Awaiting {} microseconds before speculating", cfs.sampleReadLatencyMicros);
        return !handler.await(cfs.sampleReadLatencyMicros, MICROSECONDS);
    }

    ReplicaPlan.ForTokenRead replicaPlan() {
        return replicaPlan.get();
    }

    void onReadTimeout() {
    }

    public static class NeverSpeculatingReadExecutor extends AbstractReadExecutor {
        /**
         * If never speculating due to lack of replicas
         * log it is as a failure if it should have happened
         * but couldn't due to lack of replicas
         */
        private final boolean logFailedSpeculation;

        public NeverSpeculatingReadExecutor(ColumnFamilyStore cfs, ReadCommand command,
                ReplicaPlan.ForTokenRead replicaPlan, long queryStartNanoTime, boolean logFailedSpeculation) {
            super(cfs, command, replicaPlan, 1, queryStartNanoTime);
            this.logFailedSpeculation = logFailedSpeculation;
        }

        public void maybeTryAdditionalReplicas() {
            if (shouldSpeculateAndMaybeWait() && logFailedSpeculation) {
                cfs.metric.speculativeInsufficientReplicas.inc();
            }
        }
    }

    static class SpeculatingReadExecutor extends AbstractReadExecutor {
        private volatile boolean speculated = false;

        public SpeculatingReadExecutor(ColumnFamilyStore cfs,
                ReadCommand command,
                ReplicaPlan.ForTokenRead replicaPlan,
                long queryStartNanoTime) {
            // We're hitting additional targets for read repair (??). Since our "extra"
            // replica is the least-
            // preferred by the snitch, we do an extra data read to start with against a
            // replica more
            // likely to respond; better to let RR fail than the entire query.
            super(cfs, command, replicaPlan, replicaPlan.readQuorum() < replicaPlan.contacts().size() ? 2 : 1,
                    queryStartNanoTime);
        }

        public void maybeTryAdditionalReplicas() {
            if (shouldSpeculateAndMaybeWait()) {
                // Handle speculation stats first in case the callback fires immediately
                cfs.metric.speculativeRetries.inc();
                speculated = true;

                ReplicaPlan.ForTokenRead replicaPlan = replicaPlan();
                ReadCommand retryCommand;
                Replica extraReplica;
                if (handler.resolver.isDataPresent()) {
                    extraReplica = replicaPlan.firstUncontactedCandidate(replica -> true);

                    // we should only use a SpeculatingReadExecutor if we have an extra replica to
                    // speculate against
                    assert extraReplica != null;

                    retryCommand = extraReplica.isTransient()
                            ? command.copyAsTransientQuery(extraReplica)
                            : command.copyAsDigestQuery(extraReplica);
                } else {
                    extraReplica = replicaPlan.firstUncontactedCandidate(Replica::isFull);
                    retryCommand = command;
                    if (extraReplica == null) {
                        cfs.metric.speculativeInsufficientReplicas.inc();
                        // cannot safely speculate a new data request, without more work - requests
                        // assumed to be
                        // unique per endpoint, and we have no full nodes left to speculate against
                        return;
                    }
                }

                // we must update the plan to include this new node, else when we come to
                // read-repair, we may not include this
                // speculated response in the data requests we make again, and we will not be
                // able to 'speculate' an extra repair read,
                // nor would we be able to speculate a new 'write' if the repair writes are
                // insufficient
                super.replicaPlan.addToContacts(extraReplica);

                if (traceState != null) {
                    traceState.trace("speculating read retry on {}", extraReplica);
                }
                logger.trace("speculating read retry on {}", extraReplica);
                MessagingService.instance().sendWithCallback(retryCommand.createMessage(false), extraReplica.endpoint(),
                        handler);
            }
        }

        @Override
        void onReadTimeout() {
            // Shouldn't be possible to get here without first attempting to speculate even
            // if the
            // timing is bad
            assert speculated;
            cfs.metric.speculativeFailedRetries.inc();
        }
    }

    private static class AlwaysSpeculatingReadExecutor extends AbstractReadExecutor {
        public AlwaysSpeculatingReadExecutor(ColumnFamilyStore cfs,
                ReadCommand command,
                ReplicaPlan.ForTokenRead replicaPlan,
                long queryStartNanoTime) {
            // presumably, we speculate an extra data request here in case it is our data
            // request that fails to respond,
            // and there are no more nodes to consult
            super(cfs, command, replicaPlan, replicaPlan.contacts().size() > 1 ? 2 : 1, queryStartNanoTime);
        }

        public void maybeTryAdditionalReplicas() {
            // no-op
        }

        @Override
        public void executeAsync() {
            super.executeAsync();
            cfs.metric.speculativeRetries.inc();
        }

        @Override
        void onReadTimeout() {
            cfs.metric.speculativeFailedRetries.inc();
        }
    }

    public void setResult(PartitionIterator result) {
        Preconditions.checkState(this.result == null, "Result can only be set once");
        if (command.metadata().keyspace.equals("ycsb")) {
            this.result = result;
        } else {
            this.result = DuplicateRowChecker.duringRead(result,
                    this.replicaPlan.get().readCandidates().endpointList());
        }
    }

    public void awaitResponses() throws ReadTimeoutException {
        awaitResponses(false);
    }

    /**
     * Wait for the CL to be satisfied by responses
     */
    public void awaitResponses(boolean logBlockingReadRepairAttempt) throws ReadTimeoutException {
        try {
            handler.awaitResults();
            assert digestResolver.isDataPresent() : "awaitResults returned with no data present.";
        } catch (ReadTimeoutException e) {
            try {
                onReadTimeout();
            } finally {
                throw e;
            }
        }
        // return immediately, or begin a read repair
        if (digestResolver.responsesMatch()) {
            setResult(digestResolver.getData());
        } else {
            logger.debug(
                    "[Tinoryj-ERROR] ReadExecutor awaitResponses() digest mismatch, starting read repair for key {}",
                    getKey());
            readRepair.startRepair(digestResolver, this::setResult);
            if (logBlockingReadRepairAttempt) {
                logger.info("Blocking Read Repair triggered for query [{}] at CL.{} with endpoints {}",
                        command.toCQLString(),
                        replicaPlan().consistencyLevel(),
                        replicaPlan().contacts());
            }
        }
    }

    public void awaitReadRepair() throws ReadTimeoutException {
        try {
            readRepair.awaitReads();
        } catch (ReadTimeoutException e) {
            if (Tracing.isTracing())
                Tracing.trace("Timed out waiting on digest mismatch repair requests");
            else
                logger.trace("Timed out waiting on digest mismatch repair requests");
            // the caught exception here will have CL.ALL from the repair command,
            // not whatever CL the initial command was at (CASSANDRA-7947)
            throw new ReadTimeoutException(replicaPlan().consistencyLevel(), handler.blockFor - 1, handler.blockFor,
                    true);
        }
    }

    boolean isDone() {
        return result != null;
    }

    public void maybeSendAdditionalDataRequests() {
        if (isDone())
            return;

        readRepair.maybeSendAdditionalReads();
    }

    public PartitionIterator getResult() throws ReadFailureException, ReadTimeoutException {
        Preconditions.checkState(result != null, "Result must be set first");
        return result;
    }
}
