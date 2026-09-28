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
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.coordinator.group.metrics.GroupLagInputs;
import org.apache.kafka.coordinator.group.metrics.GroupLagValue;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.server.util.PartitionMetadataClient;
import org.apache.kafka.server.util.timer.MockTimer;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GroupLagSamplerTest {

    private static final TopicPartition SHARD_0 = new TopicPartition("__consumer_offsets", 0);
    private static final TopicPartition SHARD_1 = new TopicPartition("__consumer_offsets", 1);
    private static final TopicPartition FOO_0 = new TopicPartition("foo", 0);
    private static final TopicPartition FOO_1 = new TopicPartition("foo", 1);
    private static final TopicPartition BAR_0 = new TopicPartition("bar", 0);

    @SuppressWarnings("unchecked")
    private static CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> mockRuntime() {
        return (CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord>) mock(CoordinatorRuntime.class);
    }

    private static CompletableFuture<PartitionMetadataClient.OffsetResponse> offset(long offset) {
        return CompletableFuture.completedFuture(new PartitionMetadataClient.OffsetResponse(offset, Errors.NONE));
    }

    private static CompletableFuture<PartitionMetadataClient.OffsetResponse> error(Errors error) {
        return CompletableFuture.completedFuture(new PartitionMetadataClient.OffsetResponse(-1L, error));
    }

    private static GroupLagSampler sampler(
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime,
        PartitionMetadataClient client,
        GroupCoordinatorMetrics metrics,
        MockTime time,
        MockTimer timer
    ) {
        return new GroupLagSampler(new LogContext(), runtime, client, metrics, time, timer, 1000L);
    }

    @Test
    public void testSampleComputesLagAcrossShards() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        PartitionMetadataClient client = mock(PartitionMetadataClient.class);
        GroupCoordinatorMetrics metrics = mock(GroupCoordinatorMetrics.class);
        MockTime time = new MockTime();
        GroupLagSampler sampler = sampler(runtime, client, metrics, time, new MockTimer(time));

        when(runtime.scheduleReadAllOperation(eq(GroupLagSampler.SAMPLE_OPERATION_NAME), any())).thenReturn(List.of(
            CompletableFuture.completedFuture(new GroupLagInputs(SHARD_0, Map.of(
                "grp-0", new GroupLagInputs.GroupLagInput(Group.GroupType.CONSUMER, Map.of(FOO_0, 10L, FOO_1, 5L))
            ))),
            CompletableFuture.completedFuture(new GroupLagInputs(SHARD_1, Map.of(
                "grp-1", new GroupLagInputs.GroupLagInput(Group.GroupType.CLASSIC, Map.of(FOO_0, 90L, BAR_0, 200L))
            )))
        ));

        when(client.listLatestOffsets(Set.of(FOO_0, FOO_1, BAR_0))).thenReturn(Map.of(
            FOO_0, offset(100L),
            FOO_1, offset(5L),
            BAR_0, offset(150L) // Committed offset ahead of the log end offset: lag is clamped to 0.
        ));

        time.sleep(10);
        sampler.sample().join();

        assertFalse(sampler.inFlight());
        verify(client, times(1)).listLatestOffsets(Set.of(FOO_0, FOO_1, BAR_0));

        ArgumentCaptor<Map<String, GroupLagValue>> captor = ArgumentCaptor.forClass(Map.class);
        verify(metrics, times(1)).updateGroupLag(captor.capture(), eq(time.milliseconds()));
        assertEquals(
            Map.of(
                "grp-0", new GroupLagValue(SHARD_0, Group.GroupType.CONSUMER, 90L, 90L, 2, 2),
                "grp-1", new GroupLagValue(SHARD_1, Group.GroupType.CLASSIC, 10L, 10L, 2, 2)
            ),
            captor.getValue()
        );
    }

    @Test
    public void testSampleSkipsPartitionsWithErrors() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        PartitionMetadataClient client = mock(PartitionMetadataClient.class);
        GroupCoordinatorMetrics metrics = mock(GroupCoordinatorMetrics.class);
        MockTime time = new MockTime();
        GroupLagSampler sampler = sampler(runtime, client, metrics, time, new MockTimer(time));

        when(runtime.scheduleReadAllOperation(eq(GroupLagSampler.SAMPLE_OPERATION_NAME), any())).thenReturn(List.of(
            CompletableFuture.completedFuture(new GroupLagInputs(SHARD_0, Map.of(
                "grp-0", new GroupLagInputs.GroupLagInput(Group.GroupType.CONSUMER, Map.of(FOO_0, 10L, FOO_1, 5L)),
                "grp-1", new GroupLagInputs.GroupLagInput(Group.GroupType.STREAMS, Map.of(BAR_0, 1L))
            )))
        ));

        when(client.listLatestOffsets(Set.of(FOO_0, FOO_1, BAR_0))).thenReturn(Map.of(
            FOO_0, offset(100L),
            FOO_1, error(Errors.LEADER_NOT_AVAILABLE),
            BAR_0, CompletableFuture.failedFuture(new RuntimeException("boom"))
        ));

        sampler.sample().join();

        ArgumentCaptor<Map<String, GroupLagValue>> captor = ArgumentCaptor.forClass(Map.class);
        verify(metrics, times(1)).updateGroupLag(captor.capture(), anyLong());
        assertEquals(
            Map.of(
                "grp-0", new GroupLagValue(SHARD_0, Group.GroupType.CONSUMER, 90L, 90L, 1, 2),
                "grp-1", new GroupLagValue(SHARD_0, Group.GroupType.STREAMS, 0L, 0L, 0, 1)
            ),
            captor.getValue()
        );
    }

    @Test
    public void testSampleWithoutPartitionsDoesNotCallClient() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        PartitionMetadataClient client = mock(PartitionMetadataClient.class);
        GroupCoordinatorMetrics metrics = mock(GroupCoordinatorMetrics.class);
        MockTime time = new MockTime();
        GroupLagSampler sampler = sampler(runtime, client, metrics, time, new MockTimer(time));

        when(runtime.scheduleReadAllOperation(eq(GroupLagSampler.SAMPLE_OPERATION_NAME), any())).thenReturn(List.of(
            CompletableFuture.completedFuture(new GroupLagInputs(SHARD_0, Map.of()))
        ));

        sampler.sample().join();

        verify(client, never()).listLatestOffsets(any());
        verify(metrics, times(1)).updateGroupLag(eq(Map.of()), anyLong());
    }

    @Test
    public void testSampleSkipsUnavailableShards() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        PartitionMetadataClient client = mock(PartitionMetadataClient.class);
        GroupCoordinatorMetrics metrics = mock(GroupCoordinatorMetrics.class);
        MockTime time = new MockTime();
        GroupLagSampler sampler = sampler(runtime, client, metrics, time, new MockTimer(time));

        when(runtime.scheduleReadAllOperation(eq(GroupLagSampler.SAMPLE_OPERATION_NAME), any())).thenReturn(List.of(
            CompletableFuture.completedFuture(new GroupLagInputs(SHARD_0, Map.of(
                "grp-0", new GroupLagInputs.GroupLagInput(Group.GroupType.CONSUMER, Map.of(FOO_0, 10L))
            ))),
            CompletableFuture.failedFuture(new NotCoordinatorException("")),
            CompletableFuture.failedFuture(new CoordinatorLoadInProgressException(""))
        ));
        when(client.listLatestOffsets(Set.of(FOO_0))).thenReturn(Map.of(FOO_0, offset(15L)));

        sampler.sample().join();

        verify(metrics, times(1)).updateGroupLag(
            eq(Map.of("grp-0", new GroupLagValue(SHARD_0, Group.GroupType.CONSUMER, 5L, 5L, 1, 1))),
            anyLong()
        );
    }

    @Test
    public void testSampleFailureLeavesMetricsUntouched() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        PartitionMetadataClient client = mock(PartitionMetadataClient.class);
        GroupCoordinatorMetrics metrics = mock(GroupCoordinatorMetrics.class);
        MockTime time = new MockTime();
        GroupLagSampler sampler = sampler(runtime, client, metrics, time, new MockTimer(time));

        when(runtime.scheduleReadAllOperation(eq(GroupLagSampler.SAMPLE_OPERATION_NAME), any())).thenReturn(List.of(
            CompletableFuture.failedFuture(new RuntimeException("boom"))
        ));

        sampler.sample().join();

        assertFalse(sampler.inFlight());
        verify(client, never()).listLatestOffsets(any());
        verify(metrics, never()).updateGroupLag(any(), anyLong());

        // The sampler recovers on the next cycle.
        when(runtime.scheduleReadAllOperation(eq(GroupLagSampler.SAMPLE_OPERATION_NAME), any())).thenReturn(List.of(
            CompletableFuture.completedFuture(new GroupLagInputs(SHARD_0, Map.of()))
        ));
        sampler.sample().join();
        verify(metrics, times(1)).updateGroupLag(eq(Map.of()), anyLong());
    }

    @Test
    public void testSampleDoesNotOverlap() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        PartitionMetadataClient client = mock(PartitionMetadataClient.class);
        GroupCoordinatorMetrics metrics = mock(GroupCoordinatorMetrics.class);
        MockTime time = new MockTime();
        GroupLagSampler sampler = sampler(runtime, client, metrics, time, new MockTimer(time));

        when(runtime.scheduleReadAllOperation(eq(GroupLagSampler.SAMPLE_OPERATION_NAME), any())).thenReturn(List.of(
            CompletableFuture.completedFuture(new GroupLagInputs(SHARD_0, Map.of(
                "grp-0", new GroupLagInputs.GroupLagInput(Group.GroupType.CONSUMER, Map.of(FOO_0, 10L))
            )))
        ));
        CompletableFuture<PartitionMetadataClient.OffsetResponse> pending = new CompletableFuture<>();
        when(client.listLatestOffsets(Set.of(FOO_0))).thenReturn(Map.of(FOO_0, pending));

        CompletableFuture<Void> first = sampler.sample();
        assertTrue(sampler.inFlight());
        assertFalse(first.isDone());

        // A second cycle is skipped while the first one is in flight.
        assertTrue(sampler.sample().isDone());
        verify(runtime, times(1)).scheduleReadAllOperation(eq(GroupLagSampler.SAMPLE_OPERATION_NAME), any());
        verify(metrics, never()).updateGroupLag(any(), anyLong());

        pending.complete(new PartitionMetadataClient.OffsetResponse(12L, Errors.NONE));
        first.join();

        assertFalse(sampler.inFlight());
        verify(metrics, times(1)).updateGroupLag(
            eq(Map.of("grp-0", new GroupLagValue(SHARD_0, Group.GroupType.CONSUMER, 2L, 2L, 1, 1))),
            anyLong()
        );
    }

    @Test
    public void testStartSchedulesPeriodicSampling() throws InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        PartitionMetadataClient client = mock(PartitionMetadataClient.class);
        GroupCoordinatorMetrics metrics = mock(GroupCoordinatorMetrics.class);
        MockTime time = new MockTime();
        MockTimer timer = new MockTimer(time);
        GroupLagSampler sampler = sampler(runtime, client, metrics, time, timer);

        when(runtime.scheduleReadAllOperation(eq(GroupLagSampler.SAMPLE_OPERATION_NAME), any())).thenReturn(List.of(
            CompletableFuture.completedFuture(new GroupLagInputs(SHARD_0, Map.of()))
        ));

        sampler.start();
        verify(runtime, never()).scheduleReadAllOperation(any(), any());

        // First cycle after one interval.
        timer.advanceClock(1001L);
        verify(runtime, times(1)).scheduleReadAllOperation(eq(GroupLagSampler.SAMPLE_OPERATION_NAME), any());
        verify(metrics, times(1)).updateGroupLag(eq(Map.of()), anyLong());

        // Second cycle after another interval.
        timer.advanceClock(1001L);
        verify(runtime, times(2)).scheduleReadAllOperation(eq(GroupLagSampler.SAMPLE_OPERATION_NAME), any());

        // Nothing after close.
        sampler.close();
        timer.advanceClock(1001L);
        timer.advanceClock(1001L);
        verify(runtime, times(2)).scheduleReadAllOperation(eq(GroupLagSampler.SAMPLE_OPERATION_NAME), any());
    }

    @Test
    public void testComputeLag() {
        Map<TopicPartition, CompletableFuture<PartitionMetadataClient.OffsetResponse>> logEndOffsets = Map.of(
            FOO_0, offset(100L),
            FOO_1, offset(50L),
            BAR_0, error(Errors.UNKNOWN_TOPIC_OR_PARTITION)
        );

        Map<String, GroupLagValue> lag = GroupLagSampler.computeLag(
            List.of(new GroupLagInputs(SHARD_0, Map.of(
                "all-ok", new GroupLagInputs.GroupLagInput(Group.GroupType.CONSUMER, Map.of(FOO_0, 40L, FOO_1, 45L)),
                "partial", new GroupLagInputs.GroupLagInput(Group.GroupType.CLASSIC, Map.of(FOO_0, 99L, BAR_0, 0L)),
                "none", new GroupLagInputs.GroupLagInput(Group.GroupType.STREAMS, Map.of(BAR_0, 0L))
            ))),
            logEndOffsets
        );

        assertEquals(
            Map.of(
                "all-ok", new GroupLagValue(SHARD_0, Group.GroupType.CONSUMER, 65L, 60L, 2, 2),
                "partial", new GroupLagValue(SHARD_0, Group.GroupType.CLASSIC, 1L, 1L, 1, 2),
                "none", new GroupLagValue(SHARD_0, Group.GroupType.STREAMS, 0L, 0L, 0, 1)
            ),
            lag
        );
    }
}
