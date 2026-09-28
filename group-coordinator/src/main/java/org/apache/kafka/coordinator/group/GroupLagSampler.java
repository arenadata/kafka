/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.coordinator.group.metrics.GroupLagInputs;
import org.apache.kafka.coordinator.group.metrics.GroupLagValue;
import org.apache.kafka.server.util.FutureUtils;
import org.apache.kafka.server.util.PartitionMetadataClient;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Periodically samples the lag of the groups hosted by the group coordinator
 * and publishes it to {@link GroupCoordinatorMetrics}.
 *
 * A sampling cycle:
 * <ol>
 *     <li>reads the committed offsets of every loaded shard through a read
 *     operation scheduled on the coordinator runtime (one per shard);</li>
 *     <li>resolves the log end offsets of all partitions with a single call
 *     to the {@link PartitionMetadataClient};</li>
 *     <li>computes {@code lag = max(0, logEndOffset - committedOffset)} per
 *     partition and aggregates it per group.</li>
 * </ol>
 *
 * The next cycle is scheduled only once the previous one completed, so cycles
 * never overlap. Nothing blocks the timer thread: the whole chain is
 * {@link CompletableFuture} based.
 */
public class GroupLagSampler implements AutoCloseable {

    /**
     * The name of the read operation scheduled on the runtime.
     */
    public static final String SAMPLE_OPERATION_NAME = "group-lag-sample";

    private final Logger log;
    private final CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime;
    private final PartitionMetadataClient partitionMetadataClient;
    private final GroupCoordinatorMetrics metrics;
    private final Time time;
    private final Timer timer;
    private final long intervalMs;

    /**
     * Whether the sampler is started and not closed.
     */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Whether a sampling cycle is in progress.
     */
    private final AtomicBoolean inFlight = new AtomicBoolean(false);

    /**
     * The currently scheduled timer task, if any.
     */
    private volatile TimerTask scheduledTask;

    public GroupLagSampler(
        LogContext logContext,
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime,
        PartitionMetadataClient partitionMetadataClient,
        GroupCoordinatorMetrics metrics,
        Time time,
        Timer timer,
        long intervalMs
    ) {
        this.log = logContext.logger(GroupLagSampler.class);
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.partitionMetadataClient = Objects.requireNonNull(partitionMetadataClient, "partitionMetadataClient");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.time = Objects.requireNonNull(time, "time");
        this.timer = Objects.requireNonNull(timer, "timer");
        if (intervalMs <= 0) {
            throw new IllegalArgumentException("intervalMs must be positive.");
        }
        this.intervalMs = intervalMs;
    }

    /**
     * Schedules the first sampling cycle. Calling it more than once has no effect.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Starting group lag sampling with an interval of {} ms.", intervalMs);
            schedule();
        }
    }

    /**
     * Cancels the scheduled sampling cycle. An in-flight cycle completes but
     * does not reschedule.
     */
    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            TimerTask task = scheduledTask;
            if (task != null) {
                task.cancel();
            }
            log.info("Stopped group lag sampling.");
        }
    }

    /**
     * @return Whether a sampling cycle is in progress.
     */
    public boolean inFlight() {
        return inFlight.get();
    }

    private void schedule() {
        if (!running.get()) {
            return;
        }
        TimerTask task = new TimerTask(intervalMs) {
            @Override
            public void run() {
                if (!running.get()) {
                    return;
                }
                sample().whenComplete((__, ___) -> schedule());
            }
        };
        scheduledTask = task;
        timer.add(task);
    }

    /**
     * Runs one sampling cycle. If a cycle is already in progress, this one is
     * skipped and the returned future completes immediately.
     *
     * @return A future completed once the cycle is done. It never completes
     *         exceptionally: failures are logged and leave the metrics untouched.
     */
    public CompletableFuture<Void> sample() {
        if (!inFlight.compareAndSet(false, true)) {
            log.debug("Skipping group lag sampling because the previous cycle is still in flight.");
            return CompletableFuture.completedFuture(null);
        }

        final long startTimeMs = time.milliseconds();
        final CompletableFuture<Void> result = new CompletableFuture<>();

        try {
            collectInputs()
                .thenCompose(this::computeLag)
                .whenComplete((lagByGroup, exception) -> {
                    try {
                        if (exception != null) {
                            log.warn("Group lag sampling failed.", exception);
                        } else {
                            metrics.updateGroupLag(lagByGroup, time.milliseconds());
                            log.debug("Sampled lag of {} groups in {} ms.",
                                lagByGroup.size(), time.milliseconds() - startTimeMs);
                        }
                    } finally {
                        inFlight.set(false);
                        result.complete(null);
                    }
                });
        } catch (Throwable t) {
            log.warn("Group lag sampling failed.", t);
            inFlight.set(false);
            result.complete(null);
        }

        return result;
    }

    /**
     * Reads the lag inputs of every loaded shard. Shards that are being loaded or
     * are not owned by this coordinator anymore are skipped for this cycle.
     */
    private CompletableFuture<List<GroupLagInputs>> collectInputs() {
        final List<CompletableFuture<GroupLagInputs>> futures = FutureUtils.mapExceptionally(
            runtime.scheduleReadAllOperation(
                SAMPLE_OPERATION_NAME,
                (shard, lastCommittedOffset) -> shard.collectGroupLagInputs(lastCommittedOffset)
            ),
            exception -> {
                exception = Errors.maybeUnwrapException(exception);
                if (exception instanceof NotCoordinatorException ||
                    exception instanceof CoordinatorLoadInProgressException) {
                    return GroupLagInputs.EMPTY;
                } else {
                    throw new CompletionException(exception);
                }
            }
        );

        return CompletableFuture
            .allOf(futures.toArray(new CompletableFuture<?>[0]))
            .thenApply(__ -> {
                final List<GroupLagInputs> inputs = new ArrayList<>(futures.size());
                futures.forEach(future -> inputs.add(future.join()));
                return inputs;
            });
    }

    /**
     * Resolves the log end offsets of every partition present in the inputs and
     * computes the lag of every group.
     */
    private CompletableFuture<Map<String, GroupLagValue>> computeLag(List<GroupLagInputs> inputs) {
        final Set<TopicPartition> partitions = new HashSet<>();
        inputs.forEach(shardInputs -> shardInputs.groups().values().forEach(input ->
            partitions.addAll(input.committedOffsets().keySet())
        ));

        if (partitions.isEmpty()) {
            return CompletableFuture.completedFuture(computeLag(inputs, Map.of()));
        }

        final Map<TopicPartition, CompletableFuture<PartitionMetadataClient.OffsetResponse>> logEndOffsets =
            partitionMetadataClient.listLatestOffsets(partitions);

        return CompletableFuture
            .allOf(logEndOffsets.values().toArray(new CompletableFuture<?>[0]))
            // Failures of individual partitions are reported in the OffsetResponse. An exceptionally
            // completed future is not expected but is handled per partition in computeLag.
            .handle((__, ___) -> computeLag(inputs, logEndOffsets));
    }

    /**
     * Computes the lag of every group from its committed offsets and the resolved
     * log end offsets. Partitions whose log end offset could not be resolved are
     * skipped; the number of skipped partitions is reflected in
     * {@link GroupLagValue#sampledPartitions()}.
     *
     * @param inputs        The inputs of all shards.
     * @param logEndOffsets The resolved log end offsets. All futures must be done.
     *
     * @return The lag of every group keyed by group id.
     */
    // Visible for testing.
    static Map<String, GroupLagValue> computeLag(
        List<GroupLagInputs> inputs,
        Map<TopicPartition, CompletableFuture<PartitionMetadataClient.OffsetResponse>> logEndOffsets
    ) {
        final Map<String, GroupLagValue> lagByGroup = new HashMap<>();

        inputs.forEach(shardInputs -> shardInputs.groups().forEach((groupId, input) -> {
            long sumLag = 0L;
            long maxLag = 0L;
            int sampledPartitions = 0;

            for (Map.Entry<TopicPartition, Long> entry : input.committedOffsets().entrySet()) {
                final CompletableFuture<PartitionMetadataClient.OffsetResponse> future = logEndOffsets.get(entry.getKey());
                if (future == null || !future.isDone() || future.isCompletedExceptionally()) {
                    continue;
                }
                final PartitionMetadataClient.OffsetResponse response = future.join();
                if (response == null || response.error() != Errors.NONE) {
                    continue;
                }
                final long lag = Math.max(0L, response.offset() - entry.getValue());
                sumLag += lag;
                maxLag = Math.max(maxLag, lag);
                sampledPartitions++;
            }

            lagByGroup.put(groupId, new GroupLagValue(
                shardInputs.shard(),
                input.type(),
                sumLag,
                maxLag,
                sampledPartitions,
                input.committedOffsets().size()
            ));
        }));

        return lagByGroup;
    }
}
