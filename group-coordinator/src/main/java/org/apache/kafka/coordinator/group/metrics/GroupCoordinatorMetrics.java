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
package org.apache.kafka.coordinator.group.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.Group;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.classic.ClassicGroupState;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup.ConsumerGroupState;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup;
import org.apache.kafka.coordinator.group.streams.StreamsGroup.StreamsGroupState;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.timeline.SnapshotRegistry;

import com.yammer.metrics.core.MetricsRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * These are the metrics which are managed by the {@link org.apache.kafka.coordinator.group.GroupMetadataManager} class.
 * They generally pertain to aspects of group management, such as the number of groups in different states.
 */
public class GroupCoordinatorMetrics extends CoordinatorMetrics implements AutoCloseable {

    public static final String METRICS_GROUP = "group-coordinator-metrics";

    /**
     * Old classic group count metric. To be deprecated.
     */
    public static final com.yammer.metrics.core.MetricName NUM_CLASSIC_GROUPS = getMetricName(
        "GroupMetadataManager", "NumGroups");
    public static final com.yammer.metrics.core.MetricName NUM_OFFSETS = getMetricName(
        "GroupMetadataManager", "NumOffsets");
    public static final com.yammer.metrics.core.MetricName NUM_CLASSIC_GROUPS_PREPARING_REBALANCE = getMetricName(
        "GroupMetadataManager", "NumGroupsPreparingRebalance");
    public static final com.yammer.metrics.core.MetricName NUM_CLASSIC_GROUPS_COMPLETING_REBALANCE = getMetricName(
        "GroupMetadataManager", "NumGroupsCompletingRebalance");
    public static final com.yammer.metrics.core.MetricName NUM_CLASSIC_GROUPS_STABLE = getMetricName(
        "GroupMetadataManager", "NumGroupsStable");
    public static final com.yammer.metrics.core.MetricName NUM_CLASSIC_GROUPS_DEAD = getMetricName(
        "GroupMetadataManager", "NumGroupsDead");
    public static final com.yammer.metrics.core.MetricName NUM_CLASSIC_GROUPS_EMPTY = getMetricName(
        "GroupMetadataManager", "NumGroupsEmpty");

    public static final String GROUP_COUNT_METRIC_NAME = "group-count";
    public static final String GROUP_COUNT_PROTOCOL_TAG = "protocol";
    public static final String SHARE_GROUP_PROTOCOL_TAG = GROUP_COUNT_PROTOCOL_TAG;
    public static final String CONSUMER_GROUP_COUNT_METRIC_NAME = "consumer-group-count";
    public static final String SHARE_GROUP_COUNT_METRIC_NAME = "share-group-count";
    public static final String CONSUMER_GROUP_COUNT_STATE_TAG = "state";
    public static final String SHARE_GROUP_COUNT_STATE_TAG = CONSUMER_GROUP_COUNT_STATE_TAG;
    public static final String STREAMS_GROUP_COUNT_METRIC_NAME = "streams-group-count";
    public static final String STREAMS_GROUP_COUNT_STATE_TAG = "state";

    public static final String GROUP_LAG_MAX_METRIC_NAME = "group-lag-max";
    public static final String GROUP_LAG_SUM_METRIC_NAME = "group-lag-sum";
    public static final String GROUP_LAG_SAMPLE_AGE_MS_METRIC_NAME = "group-lag-sample-age-ms";
    public static final String GROUP_LAG_GROUP_TAG = "group";
    public static final String GROUP_LAG_PROTOCOL_TAG = GROUP_COUNT_PROTOCOL_TAG;

    /**
     * The value of the {@link #GROUP_LAG_SAMPLE_AGE_MS_METRIC_NAME} metric before
     * the first successful sampling cycle.
     */
    public static final long GROUP_LAG_NOT_SAMPLED_YET = -1L;

    private static final Logger LOG = LoggerFactory.getLogger(GroupCoordinatorMetrics.class);

    public static final String OFFSET_COMMITS_SENSOR_NAME = "OffsetCommits";
    public static final String OFFSET_EXPIRED_SENSOR_NAME = "OffsetExpired";
    public static final String OFFSET_DELETIONS_SENSOR_NAME = "OffsetDeletions";
    public static final String CLASSIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME = "CompletedRebalances";
    public static final String CONSUMER_GROUP_REBALANCES_SENSOR_NAME = "ConsumerGroupRebalances";
    public static final String SHARE_GROUP_REBALANCES_SENSOR_NAME = "ShareGroupRebalances";
    public static final String STREAMS_GROUP_REBALANCES_SENSOR_NAME = "StreamsGroupRebalances";

    private final MetricName classicGroupCountMetricName;
    private final MetricName consumerGroupCountMetricName;
    private final MetricName consumerGroupCountEmptyMetricName;
    private final MetricName consumerGroupCountAssigningMetricName;
    private final MetricName consumerGroupCountReconcilingMetricName;
    private final MetricName consumerGroupCountStableMetricName;
    private final MetricName consumerGroupCountDeadMetricName;
    private final MetricName shareGroupCountMetricName;
    private final MetricName shareGroupCountEmptyMetricName;
    private final MetricName shareGroupCountStableMetricName;
    private final MetricName shareGroupCountDeadMetricName;
    private final MetricName streamsGroupCountMetricName;
    private final MetricName streamsGroupCountEmptyMetricName;
    private final MetricName streamsGroupCountAssigningMetricName;
    private final MetricName streamsGroupCountReconcilingMetricName;
    private final MetricName streamsGroupCountStableMetricName;
    private final MetricName streamsGroupCountDeadMetricName;
    private final MetricName streamsGroupCountNotReadyMetricName;
    private final MetricName groupLagMaxMetricName;
    private final MetricName groupLagSampleAgeMsMetricName;

    private final MetricsRegistry registry;
    private final Metrics metrics;
    private final Map<TopicPartition, GroupCoordinatorMetricsShard> shards = new ConcurrentHashMap<>();

    /**
     * The maximum number of groups with per-group lag gauges.
     */
    private final int maxLagGroups;

    /**
     * The lag of every sampled group. Mutated only while holding {@link #lagLock};
     * gauges read the immutable {@link GroupLagValue} held by each entry.
     */
    private final Map<String, GroupLagEntry> lagByGroup = new ConcurrentHashMap<>();
    private final Object lagLock = new Object();
    // Guarded by lagLock.
    private int registeredLagGroups = 0;
    private volatile long groupLagMax = 0L;
    private volatile long lastLagSampleTimeMs = GROUP_LAG_NOT_SAMPLED_YET;
    private volatile boolean maxLagGroupsWarned = false;

    /**
     * The per-group lag state. The metric names are null while the group has no
     * per-group gauges because of {@link #maxLagGroups}.
     */
    private static final class GroupLagEntry {
        volatile GroupLagValue value;
        MetricName sumMetricName;
        MetricName maxMetricName;

        GroupLagEntry(GroupLagValue value) {
            this.value = value;
        }

        boolean registered() {
            return sumMetricName != null;
        }
    }

    /**
     * Global sensors. These are shared across all metrics shards.
     */
    public final Map<String, Sensor> globalSensors;

    public GroupCoordinatorMetrics() {
        this(KafkaYammerMetrics.defaultRegistry(), new Metrics());
    }

    public GroupCoordinatorMetrics(MetricsRegistry registry, Metrics metrics) {
        this(registry, metrics, GroupCoordinatorConfig.GROUP_COORDINATOR_LAG_METRICS_MAX_GROUPS_DEFAULT);
    }

    /**
     * @param registry      The yammer metrics registry.
     * @param metrics       The Kafka metrics.
     * @param maxLagGroups  The maximum number of groups with per-group lag gauges.
     */
    @SuppressWarnings("MethodLength")
    public GroupCoordinatorMetrics(MetricsRegistry registry, Metrics metrics, int maxLagGroups) {
        this.registry = Objects.requireNonNull(registry);
        this.metrics = Objects.requireNonNull(metrics);
        if (maxLagGroups < 0) {
            throw new IllegalArgumentException("maxLagGroups must not be negative.");
        }
        this.maxLagGroups = maxLagGroups;

        groupLagMaxMetricName = metrics.metricName(
            GROUP_LAG_MAX_METRIC_NAME,
            METRICS_GROUP,
            "The maximum partition lag across all classic, consumer and streams groups hosted by this coordinator."
        );

        groupLagSampleAgeMsMetricName = metrics.metricName(
            GROUP_LAG_SAMPLE_AGE_MS_METRIC_NAME,
            METRICS_GROUP,
            "The number of milliseconds since the last successful group lag sampling cycle, or -1 if none completed yet."
        );

        classicGroupCountMetricName = metrics.metricName(
            GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The total number of groups using the classic rebalance protocol.",
            Map.of(GROUP_COUNT_PROTOCOL_TAG, Group.GroupType.CLASSIC.toString())
        );

        consumerGroupCountMetricName = metrics.metricName(
            GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The total number of groups using the consumer rebalance protocol.",
            Map.of(GROUP_COUNT_PROTOCOL_TAG, Group.GroupType.CONSUMER.toString())
        );

        consumerGroupCountEmptyMetricName = metrics.metricName(
            CONSUMER_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of consumer groups in empty state.",
            Map.of(CONSUMER_GROUP_COUNT_STATE_TAG, ConsumerGroupState.EMPTY.toString())
        );

        consumerGroupCountAssigningMetricName = metrics.metricName(
            CONSUMER_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of consumer groups in assigning state.",
            Map.of(CONSUMER_GROUP_COUNT_STATE_TAG, ConsumerGroupState.ASSIGNING.toString())
        );

        consumerGroupCountReconcilingMetricName = metrics.metricName(
            CONSUMER_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of consumer groups in reconciling state.",
            Map.of(CONSUMER_GROUP_COUNT_STATE_TAG, ConsumerGroupState.RECONCILING.toString())
        );

        consumerGroupCountStableMetricName = metrics.metricName(
            CONSUMER_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of consumer groups in stable state.",
            Map.of(CONSUMER_GROUP_COUNT_STATE_TAG, ConsumerGroupState.STABLE.toString())
        );

        consumerGroupCountDeadMetricName = metrics.metricName(
            CONSUMER_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of consumer groups in dead state.",
            Map.of(CONSUMER_GROUP_COUNT_STATE_TAG, ConsumerGroupState.DEAD.toString())
        );

        shareGroupCountMetricName = metrics.metricName(
            GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The total number of share groups.",
            Map.of(SHARE_GROUP_PROTOCOL_TAG, Group.GroupType.SHARE.toString())
        );

        shareGroupCountEmptyMetricName = metrics.metricName(
            SHARE_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of share groups in empty state.",
            Map.of(SHARE_GROUP_COUNT_STATE_TAG, ShareGroup.ShareGroupState.EMPTY.toString())
        );

        shareGroupCountStableMetricName = metrics.metricName(
            SHARE_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of share groups in stable state.",
            Map.of(SHARE_GROUP_COUNT_STATE_TAG, ShareGroup.ShareGroupState.STABLE.toString())
        );

        shareGroupCountDeadMetricName = metrics.metricName(
            SHARE_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of share groups in dead state.",
            Map.of(SHARE_GROUP_COUNT_STATE_TAG, ShareGroup.ShareGroupState.DEAD.toString())
        );

        streamsGroupCountMetricName = metrics.metricName(
            GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The total number of groups using the streams rebalance protocol.",
            Map.of(GROUP_COUNT_PROTOCOL_TAG, Group.GroupType.STREAMS.toString())
        );

        streamsGroupCountEmptyMetricName = metrics.metricName(
            STREAMS_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of streams groups in empty state.",
            Map.of(STREAMS_GROUP_COUNT_STATE_TAG, StreamsGroupState.EMPTY.toString())
        );

        streamsGroupCountAssigningMetricName = metrics.metricName(
            STREAMS_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of streams groups in assigning state.",
            Map.of(STREAMS_GROUP_COUNT_STATE_TAG, StreamsGroupState.ASSIGNING.toString())
        );

        streamsGroupCountReconcilingMetricName = metrics.metricName(
            STREAMS_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of streams groups in reconciling state.",
            Map.of(STREAMS_GROUP_COUNT_STATE_TAG, StreamsGroupState.RECONCILING.toString())
        );

        streamsGroupCountStableMetricName = metrics.metricName(
            STREAMS_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of streams groups in stable state.",
            Map.of(STREAMS_GROUP_COUNT_STATE_TAG, StreamsGroupState.STABLE.toString())
        );

        streamsGroupCountDeadMetricName = metrics.metricName(
            STREAMS_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of streams groups in dead state.",
            Map.of(STREAMS_GROUP_COUNT_STATE_TAG, StreamsGroupState.DEAD.toString())
        );

        streamsGroupCountNotReadyMetricName = metrics.metricName(
            STREAMS_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of streams groups in not ready state.",
            Map.of(STREAMS_GROUP_COUNT_STATE_TAG, StreamsGroupState.NOT_READY.toString())
        );

        registerGauges();

        Sensor offsetCommitsSensor = metrics.sensor(OFFSET_COMMITS_SENSOR_NAME);
        offsetCommitsSensor.add(new Meter(
            metrics.metricName("offset-commit-rate",
                METRICS_GROUP,
                "The rate of committed offsets"),
            metrics.metricName("offset-commit-count",
                METRICS_GROUP,
                "The total number of committed offsets")));

        Sensor offsetExpiredSensor = metrics.sensor(OFFSET_EXPIRED_SENSOR_NAME);
        offsetExpiredSensor.add(new Meter(
            metrics.metricName("offset-expiration-rate",
                METRICS_GROUP,
                "The rate of expired offsets"),
            metrics.metricName("offset-expiration-count",
                METRICS_GROUP,
                "The total number of expired offsets")));

        Sensor offsetDeletionsSensor = metrics.sensor(OFFSET_DELETIONS_SENSOR_NAME);
        offsetDeletionsSensor.add(new Meter(
            metrics.metricName("offset-deletion-rate",
                METRICS_GROUP,
                "The rate of administrative deleted offsets"),
            metrics.metricName("offset-deletion-count",
                METRICS_GROUP,
                "The total number of administrative deleted offsets")));

        Sensor classicGroupCompletedRebalancesSensor = metrics.sensor(CLASSIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME);
        classicGroupCompletedRebalancesSensor.add(new Meter(
            metrics.metricName("group-completed-rebalance-rate",
                METRICS_GROUP,
                "The rate of classic group completed rebalances"),
            metrics.metricName("group-completed-rebalance-count",
                METRICS_GROUP,
                "The total number of classic group completed rebalances")));

        Sensor consumerGroupRebalanceSensor = metrics.sensor(CONSUMER_GROUP_REBALANCES_SENSOR_NAME);
        consumerGroupRebalanceSensor.add(new Meter(
            metrics.metricName("consumer-group-rebalance-rate",
                METRICS_GROUP,
                "The rate of consumer group rebalances"),
            metrics.metricName("consumer-group-rebalance-count",
                METRICS_GROUP,
                "The total number of consumer group rebalances")));

        Sensor shareGroupRebalanceSensor = metrics.sensor(SHARE_GROUP_REBALANCES_SENSOR_NAME);
        shareGroupRebalanceSensor.add(new Meter(
            metrics.metricName("share-group-rebalance-rate",
                METRICS_GROUP,
                "The rate of share group rebalances"),
            metrics.metricName("share-group-rebalance-count",
                METRICS_GROUP,
                "The total number of share group rebalances")));
        
        Sensor streamsGroupRebalanceSensor = metrics.sensor(STREAMS_GROUP_REBALANCES_SENSOR_NAME);
        streamsGroupRebalanceSensor.add(new Meter(
            metrics.metricName("streams-group-rebalance-rate",
                METRICS_GROUP,
                "The rate of streams group rebalances"),
            metrics.metricName("streams-group-rebalance-count",
                METRICS_GROUP,
                "The total number of streams group rebalances")));

        globalSensors = Collections.unmodifiableMap(Utils.mkMap(
            Utils.mkEntry(OFFSET_COMMITS_SENSOR_NAME, offsetCommitsSensor),
            Utils.mkEntry(OFFSET_EXPIRED_SENSOR_NAME, offsetExpiredSensor),
            Utils.mkEntry(OFFSET_DELETIONS_SENSOR_NAME, offsetDeletionsSensor),
            Utils.mkEntry(CLASSIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME, classicGroupCompletedRebalancesSensor),
            Utils.mkEntry(CONSUMER_GROUP_REBALANCES_SENSOR_NAME, consumerGroupRebalanceSensor),
            Utils.mkEntry(SHARE_GROUP_REBALANCES_SENSOR_NAME, shareGroupRebalanceSensor),
            Utils.mkEntry(STREAMS_GROUP_REBALANCES_SENSOR_NAME, streamsGroupRebalanceSensor)
        ));
    }

    private Long numOffsets() {
        return shards.values().stream().mapToLong(GroupCoordinatorMetricsShard::numOffsets).sum();
    }

    private Long numClassicGroups() {
        return shards.values().stream().mapToLong(GroupCoordinatorMetricsShard::numClassicGroups).sum();
    }

    private Long numClassicGroups(ClassicGroupState state) {
        return shards.values().stream().mapToLong(shard -> shard.numClassicGroups(state)).sum();
    }

    private long numConsumerGroups() {
        return shards.values().stream().mapToLong(GroupCoordinatorMetricsShard::numConsumerGroups).sum();
    }

    private long numConsumerGroups(ConsumerGroupState state) {
        return shards.values().stream().mapToLong(shard -> shard.numConsumerGroups(state)).sum();
    }

    private long numStreamsGroups() {
        return shards.values().stream().mapToLong(GroupCoordinatorMetricsShard::numStreamsGroups).sum();
    }

    private long numStreamsGroups(StreamsGroupState state) {
        return shards.values().stream().mapToLong(shard -> shard.numStreamsGroups(state)).sum();
    }
    
    private long numShareGroups() {
        return shards.values().stream().mapToLong(GroupCoordinatorMetricsShard::numShareGroups).sum();
    }

    private long numShareGroups(ShareGroup.ShareGroupState state) {
        return shards.values().stream().mapToLong(shard -> shard.numShareGroups(state)).sum();
    }

    @Override
    public void close() {
        Arrays.asList(
            NUM_OFFSETS,
            NUM_CLASSIC_GROUPS,
            NUM_CLASSIC_GROUPS_PREPARING_REBALANCE,
            NUM_CLASSIC_GROUPS_COMPLETING_REBALANCE,
            NUM_CLASSIC_GROUPS_STABLE,
            NUM_CLASSIC_GROUPS_DEAD,
            NUM_CLASSIC_GROUPS_EMPTY
        ).forEach(registry::removeMetric);

        Arrays.asList(
            classicGroupCountMetricName,
            consumerGroupCountMetricName,
            consumerGroupCountEmptyMetricName,
            consumerGroupCountAssigningMetricName,
            consumerGroupCountReconcilingMetricName,
            consumerGroupCountStableMetricName,
            consumerGroupCountDeadMetricName,
            shareGroupCountMetricName,
            shareGroupCountEmptyMetricName,
            shareGroupCountStableMetricName,
            shareGroupCountDeadMetricName,
            streamsGroupCountMetricName,
            streamsGroupCountEmptyMetricName,
            streamsGroupCountAssigningMetricName,
            streamsGroupCountReconcilingMetricName,
            streamsGroupCountStableMetricName,
            streamsGroupCountDeadMetricName,
            streamsGroupCountNotReadyMetricName,
            groupLagMaxMetricName,
            groupLagSampleAgeMsMetricName
        ).forEach(metrics::removeMetric);

        synchronized (lagLock) {
            lagByGroup.values().forEach(this::unregisterGroupLagGauges);
            lagByGroup.clear();
            registeredLagGroups = 0;
            groupLagMax = 0L;
        }

        Arrays.asList(
            OFFSET_COMMITS_SENSOR_NAME,
            OFFSET_EXPIRED_SENSOR_NAME,
            OFFSET_DELETIONS_SENSOR_NAME,
            CLASSIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME,
            CONSUMER_GROUP_REBALANCES_SENSOR_NAME,
            SHARE_GROUP_REBALANCES_SENSOR_NAME,
            STREAMS_GROUP_REBALANCES_SENSOR_NAME
        ).forEach(metrics::removeSensor);
    }

    @Override
    public GroupCoordinatorMetricsShard newMetricsShard(SnapshotRegistry snapshotRegistry, TopicPartition tp) {
        return new GroupCoordinatorMetricsShard(snapshotRegistry, globalSensors, tp);
    }

    @Override
    public void activateMetricsShard(CoordinatorMetricsShard shard) {
        if (!(shard instanceof GroupCoordinatorMetricsShard)) {
            throw new IllegalArgumentException("GroupCoordinatorMetrics can only activate GroupCoordinatorMetricShard");
        }
        shards.put(shard.topicPartition(), (GroupCoordinatorMetricsShard) shard);
    }

    @Override
    public void deactivateMetricsShard(CoordinatorMetricsShard shard) {
        shards.remove(shard.topicPartition());
        removeGroupLag(shard.topicPartition());
    }

    /**
     * Updates the group lag gauges with the result of a sampling cycle.
     *
     * Groups absent from {@code sampledLag} are removed. Groups present but with no
     * sampled partition keep their previous value, if any, so that a transient
     * ListOffsets failure does not zero the gauges.
     *
     * The sample time only advances when the cycle resolved at least one log end
     * offset, or had none to resolve. When every lookup failed, the gauges still hold
     * the previous cycle's values, so the sample age keeps growing to show that they
     * are stale.
     *
     * Values computed from a coordinator shard that was unloaded, or unloaded and loaded
     * again, since the cycle read it are discarded, so that a cycle still waiting for
     * ListOffsets does not bring back the gauges that {@link #deactivateMetricsShard}
     * removed.
     *
     * @param sampledLag    The lag of every sampled group keyed by group id.
     * @param sampleTimeMs  The time at which the cycle completed.
     */
    public void updateGroupLag(Map<String, GroupLagValue> sampledLag, long sampleTimeMs) {
        synchronized (lagLock) {
            final Map<String, GroupLagValue> activeLag = discardInactiveShards(sampledLag);

            // Drop the groups that are gone.
            Iterator<Map.Entry<String, GroupLagEntry>> iterator = lagByGroup.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, GroupLagEntry> entry = iterator.next();
                if (!activeLag.containsKey(entry.getKey())) {
                    unregisterGroupLagGauges(entry.getValue());
                    iterator.remove();
                }
            }

            // Upsert the sampled groups.
            activeLag.forEach((groupId, value) -> {
                GroupLagEntry entry = lagByGroup.get(groupId);
                if (value.sampledPartitions() == 0) {
                    // Keep the previous value, if any.
                    if (entry != null) {
                        entry.value = new GroupLagValue(
                            value.shard(),
                            value.type(),
                            entry.value.sumLag(),
                            entry.value.maxLag(),
                            0,
                            value.totalPartitions()
                        );
                    }
                } else if (entry == null) {
                    lagByGroup.put(groupId, new GroupLagEntry(value));
                } else {
                    entry.value = value;
                }
            });

            registerGroupLagGaugesUpToCap();
            groupLagMax = computeGroupLagMax();

            if (isSuccessfulSample(activeLag)) {
                lastLagSampleTimeMs = sampleTimeMs;
            }
        }
    }

    /**
     * Keeps only the values computed from a metrics shard that is still the active one of
     * its partition. Each load of a partition creates a new metrics shard, so a value from
     * an unloaded or reloaded shard carries an instance that is no longer in {@link #shards}.
     *
     * Must be called while holding {@link #lagLock}. {@link #deactivateMetricsShard} removes
     * the shard from {@link #shards} before it takes the lock to remove the shard's gauges,
     * so either this check sees the shard gone, or the removal runs after this update.
     */
    private Map<String, GroupLagValue> discardInactiveShards(Map<String, GroupLagValue> sampledLag) {
        Map<String, GroupLagValue> active = new HashMap<>(sampledLag.size());
        sampledLag.forEach((groupId, value) -> {
            if (shards.get(value.shard().topicPartition()) == value.shard()) {
                active.put(groupId, value);
            }
        });
        if (active.size() < sampledLag.size()) {
            LOG.debug("Discarded the lag of {} groups hosted by coordinator shards that were unloaded " +
                "or reloaded during the sampling cycle.", sampledLag.size() - active.size());
        }
        return active;
    }

    /**
     * @return True if the cycle resolved at least one log end offset, or had none to
     *         resolve. False when every lookup failed.
     */
    private static boolean isSuccessfulSample(Map<String, GroupLagValue> sampledLag) {
        boolean hadLookups = false;
        for (GroupLagValue value : sampledLag.values()) {
            if (value.sampledPartitions() > 0) {
                return true;
            }
            hadLookups |= value.totalPartitions() > 0;
        }
        return !hadLookups;
    }

    /**
     * Registers gauges for the groups without any, in group id order, up to
     * {@link #maxLagGroups}. Logs a warning the first time a group is left out.
     * Must be called while holding {@link #lagLock}.
     */
    private void registerGroupLagGaugesUpToCap() {
        if (registeredLagGroups < maxLagGroups) {
            List<String> unregistered = new ArrayList<>();
            lagByGroup.forEach((groupId, entry) -> {
                if (!entry.registered()) unregistered.add(groupId);
            });
            unregistered.sort(null);
            for (String groupId : unregistered) {
                if (registeredLagGroups >= maxLagGroups) break;
                registerGroupLagGauges(groupId, lagByGroup.get(groupId));
            }
        }

        if (lagByGroup.size() > registeredLagGroups && !maxLagGroupsWarned) {
            LOG.warn("The number of groups with per-group lag metrics reached {} ({}). Per-group lag metrics " +
                "are not exposed for the remaining groups; they only contribute to the {} metric.",
                maxLagGroups,
                GroupCoordinatorConfig.GROUP_COORDINATOR_LAG_METRICS_MAX_GROUPS_CONFIG,
                GROUP_LAG_MAX_METRIC_NAME);
            maxLagGroupsWarned = true;
        }
    }

    /**
     * Removes the lag of the groups hosted by the given shard. Called when the
     * shard is unloaded so that the gauges do not survive until the next cycle.
     */
    private void removeGroupLag(TopicPartition shard) {
        synchronized (lagLock) {
            boolean removed = false;
            Iterator<Map.Entry<String, GroupLagEntry>> iterator = lagByGroup.entrySet().iterator();
            while (iterator.hasNext()) {
                GroupLagEntry entry = iterator.next().getValue();
                if (shard.equals(entry.value.shard().topicPartition())) {
                    unregisterGroupLagGauges(entry);
                    iterator.remove();
                    removed = true;
                }
            }
            if (removed) {
                groupLagMax = computeGroupLagMax();
            }
        }
    }

    private long computeGroupLagMax() {
        return lagByGroup.values().stream().mapToLong(entry -> entry.value.maxLag()).max().orElse(0L);
    }

    private void registerGroupLagGauges(String groupId, GroupLagEntry entry) {
        Map<String, String> tags = Map.of(
            GROUP_LAG_GROUP_TAG, groupId,
            GROUP_LAG_PROTOCOL_TAG, entry.value.type().toString()
        );
        entry.sumMetricName = metrics.metricName(
            GROUP_LAG_SUM_METRIC_NAME,
            METRICS_GROUP,
            "The sum of the partition lags of the group.",
            tags
        );
        entry.maxMetricName = metrics.metricName(
            GROUP_LAG_MAX_METRIC_NAME,
            METRICS_GROUP,
            "The maximum partition lag of the group.",
            tags
        );
        metrics.addMetric(entry.sumMetricName, (Gauge<Long>) (config, now) -> entry.value.sumLag());
        metrics.addMetric(entry.maxMetricName, (Gauge<Long>) (config, now) -> entry.value.maxLag());
        registeredLagGroups++;
    }

    private void unregisterGroupLagGauges(GroupLagEntry entry) {
        if (entry.registered()) {
            metrics.removeMetric(entry.sumMetricName);
            metrics.removeMetric(entry.maxMetricName);
            entry.sumMetricName = null;
            entry.maxMetricName = null;
            registeredLagGroups--;
        }
    }

    /**
     * @return The number of groups with per-group lag gauges. Visible for testing.
     */
    public int registeredLagGroups() {
        synchronized (lagLock) {
            return registeredLagGroups;
        }
    }

    @Override
    public void onUpdateLastCommittedOffset(TopicPartition tp, long offset) {
        CoordinatorMetricsShard shard = shards.get(tp);
        if (shard != null) {
            shard.commitUpTo(offset);
        }
    }

    public static com.yammer.metrics.core.MetricName getMetricName(String type, String name) {
        return getMetricName("kafka.coordinator.group", type, name);
    }

    private void registerGauges() {
        registry.newGauge(NUM_OFFSETS, new com.yammer.metrics.core.Gauge<Long>() {
            @Override
            public Long value() {
                return numOffsets();
            }
        });

        registry.newGauge(NUM_CLASSIC_GROUPS, new com.yammer.metrics.core.Gauge<Long>() {
            @Override
            public Long value() {
                return numClassicGroups();
            }
        });

        registry.newGauge(NUM_CLASSIC_GROUPS_PREPARING_REBALANCE, new com.yammer.metrics.core.Gauge<Long>() {
            @Override
            public Long value() {
                return numClassicGroups(ClassicGroupState.PREPARING_REBALANCE);
            }
        });

        registry.newGauge(NUM_CLASSIC_GROUPS_COMPLETING_REBALANCE, new com.yammer.metrics.core.Gauge<Long>() {
            @Override
            public Long value() {
                return numClassicGroups(ClassicGroupState.COMPLETING_REBALANCE);
            }
        });

        registry.newGauge(NUM_CLASSIC_GROUPS_STABLE, new com.yammer.metrics.core.Gauge<Long>() {
            @Override
            public Long value() {
                return numClassicGroups(ClassicGroupState.STABLE);
            }
        });

        registry.newGauge(NUM_CLASSIC_GROUPS_DEAD, new com.yammer.metrics.core.Gauge<Long>() {
            @Override
            public Long value() {
                return numClassicGroups(ClassicGroupState.DEAD);
            }
        });

        registry.newGauge(NUM_CLASSIC_GROUPS_EMPTY, new com.yammer.metrics.core.Gauge<Long>() {
            @Override
            public Long value() {
                return numClassicGroups(ClassicGroupState.EMPTY);
            }
        });

        metrics.addMetric(
            classicGroupCountMetricName,
            (Gauge<Long>) (config, now) -> numClassicGroups()
        );

        metrics.addMetric(
            consumerGroupCountMetricName,
            (Gauge<Long>) (config, now) -> numConsumerGroups()
        );

        metrics.addMetric(
            consumerGroupCountEmptyMetricName,
            (Gauge<Long>) (config, now) -> numConsumerGroups(ConsumerGroupState.EMPTY)
        );

        metrics.addMetric(
            consumerGroupCountAssigningMetricName,
            (Gauge<Long>) (config, now) -> numConsumerGroups(ConsumerGroupState.ASSIGNING)
        );

        metrics.addMetric(
            consumerGroupCountReconcilingMetricName,
            (Gauge<Long>) (config, now) -> numConsumerGroups(ConsumerGroupState.RECONCILING)
        );

        metrics.addMetric(
            consumerGroupCountStableMetricName,
            (Gauge<Long>) (config, now) -> numConsumerGroups(ConsumerGroupState.STABLE)
        );

        metrics.addMetric(
            consumerGroupCountDeadMetricName,
            (Gauge<Long>) (config, now) -> numConsumerGroups(ConsumerGroupState.DEAD)
        );

        metrics.addMetric(
            shareGroupCountMetricName,
            (Gauge<Long>) (config, now) -> numShareGroups()
        );

        metrics.addMetric(
            shareGroupCountEmptyMetricName,
            (Gauge<Long>) (config, now) -> numShareGroups(ShareGroup.ShareGroupState.EMPTY)
        );

        metrics.addMetric(
            shareGroupCountStableMetricName,
            (Gauge<Long>) (config, now) -> numShareGroups(ShareGroup.ShareGroupState.STABLE)
        );

        metrics.addMetric(
            shareGroupCountDeadMetricName,
            (Gauge<Long>) (config, now) -> numShareGroups(ShareGroup.ShareGroupState.DEAD)
        );

        metrics.addMetric(
            streamsGroupCountMetricName,
            (Gauge<Long>) (config, now) -> numStreamsGroups()
        );

        metrics.addMetric(
            streamsGroupCountEmptyMetricName,
            (Gauge<Long>) (config, now) -> numStreamsGroups(StreamsGroupState.EMPTY)
        );

        metrics.addMetric(
            streamsGroupCountAssigningMetricName,
            (Gauge<Long>) (config, now) -> numStreamsGroups(StreamsGroupState.ASSIGNING)
        );

        metrics.addMetric(
            streamsGroupCountReconcilingMetricName,
            (Gauge<Long>) (config, now) -> numStreamsGroups(StreamsGroupState.RECONCILING)
        );

        metrics.addMetric(
            streamsGroupCountStableMetricName,
            (Gauge<Long>) (config, now) -> numStreamsGroups(StreamsGroupState.STABLE)
        );

        metrics.addMetric(
            streamsGroupCountDeadMetricName,
            (Gauge<Long>) (config, now) -> numStreamsGroups(StreamsGroupState.DEAD)
        );

        metrics.addMetric(
            streamsGroupCountNotReadyMetricName,
            (Gauge<Long>) (config, now) -> numStreamsGroups(StreamsGroupState.NOT_READY)
        );

        metrics.addMetric(
            groupLagMaxMetricName,
            (Gauge<Long>) (config, now) -> groupLagMax
        );

        metrics.addMetric(
            groupLagSampleAgeMsMetricName,
            (Gauge<Long>) (config, now) -> {
                long sampleTimeMs = lastLagSampleTimeMs;
                return sampleTimeMs == GROUP_LAG_NOT_SAMPLED_YET ? GROUP_LAG_NOT_SAMPLED_YET : Math.max(0L, now - sampleTimeMs);
            }
        );
    }
}
