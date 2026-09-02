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
package org.apache.kafka.connect.openmetadata;

import org.apache.kafka.connect.metadata.EntityReference;
import org.apache.kafka.connect.metadata.LineageEdge;
import org.apache.kafka.connect.metadata.MetadataEvent;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchSenderTest {

    private ScheduledExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void offerStoresEventAndDrainNowFlushes() {
        List<MetadataEvent> received = new ArrayList<>();
        BatchSender sender = new BatchSender(config(10, 50, 0), executor, received::add);

        MetadataEvent event = sampleEvent();
        sender.offer(event);
        assertEquals(1, sender.bufferDepth());

        sender.drainNow();

        assertEquals(1, received.size());
        assertEquals(0, sender.bufferDepth());
        assertEquals(0L, sender.droppedEventsCount());
    }

    @Test
    void offerDropsWhenBufferFull() {
        List<MetadataEvent> received = new ArrayList<>();
        BatchSender sender = new BatchSender(config(2, 50, 0), executor, received::add);

        sender.offer(sampleEvent());
        sender.offer(sampleEvent());
        sender.offer(sampleEvent());
        sender.offer(sampleEvent());
        sender.offer(sampleEvent());

        assertEquals(3L, sender.droppedEventsCount());
        assertEquals(2, sender.bufferDepth());

        sender.drainNow();
        assertEquals(2, received.size());
    }

    @Test
    void drainNowFlushesAllBatches() {
        List<MetadataEvent> received = new ArrayList<>();
        BatchSender sender = new BatchSender(config(10, 3, 0), executor, received::add);

        for (int i = 0; i < 10; i++) {
            sender.offer(sampleEvent());
        }

        sender.drainNow();

        assertEquals(10, received.size());
        assertEquals(0, sender.bufferDepth());
    }

    @Test
    void successfulSendIsNotRetried() {
        AtomicInteger calls = new AtomicInteger();
        Consumer<MetadataEvent> sink = e -> calls.incrementAndGet();
        BatchSender sender = new BatchSender(config(10, 50, 3), executor, sink);

        sender.offer(sampleEvent());
        sender.drainNow();

        assertEquals(1, calls.get());
    }

    @Test
    void transientFailureRetriesUpToMaxThenDrops() {
        AtomicInteger calls = new AtomicInteger();
        Consumer<MetadataEvent> sink = e -> {
            calls.incrementAndGet();
            throw new RuntimeException("transient");
        };
        BatchSender sender = new BatchSender(config(10, 50, 3), executor, sink);

        sender.offer(sampleEvent());
        sender.drainNow();

        // Initial attempt + 3 retries = 4 calls.
        assertEquals(4, calls.get());
    }

    @Test
    void transientFailureSucceedsBeforeMaxRetries() {
        AtomicInteger calls = new AtomicInteger();
        Consumer<MetadataEvent> sink = e -> {
            int n = calls.incrementAndGet();
            if (n == 1) {
                throw new RuntimeException("first try fails");
            }
        };
        BatchSender sender = new BatchSender(config(10, 50, 3), executor, sink);

        sender.offer(sampleEvent());
        sender.drainNow();

        assertEquals(2, calls.get());
    }

    @Test
    void permanentFailureIsNotRetried() {
        // Guard against the historical infinite-loop bug in sendWithRetry.
        assertTimeoutPreemptively(Duration.ofSeconds(3), () -> {
            AtomicInteger calls = new AtomicInteger();
            Consumer<MetadataEvent> sink = e -> {
                calls.incrementAndGet();
                throw new BatchSender.PermanentSendException("4xx");
            };
            BatchSender sender = new BatchSender(config(10, 50, 5), executor, sink);

            sender.offer(sampleEvent());
            sender.drainNow();

            assertEquals(1, calls.get());
        });
    }

    @Test
    void scheduledTaskFiresAndStopCancels() throws Exception {
        CountDownLatch firstDrain = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        Consumer<MetadataEvent> sink = e -> {
            calls.incrementAndGet();
            firstDrain.countDown();
        };
        Map<String, Object> overrides = new HashMap<>();
        overrides.put(OpenMetadataReporterConfig.FLUSH_INTERVAL_MS_CONFIG, 50L);
        BatchSender sender = new BatchSender(config(10, 50, 0, overrides), executor, sink);

        sender.start();
        sender.offer(sampleEvent());
        assertTrue(firstDrain.await(3, TimeUnit.SECONDS), "scheduled drain never fired");

        sender.stop();

        int callsAfterStop = calls.get();
        Thread.sleep(200);
        assertEquals(callsAfterStop, calls.get(), "scheduled task fired after stop()");
    }

    @Test
    void wrap400StatusIsPermanent() {
        RuntimeException wrapped = BatchSender.wrap(
                new IOException("PUT /lineage returned 400: bad request"));
        assertInstanceOf(BatchSender.PermanentSendException.class, wrapped);
    }

    @Test
    void wrap408StatusIsTransient() {
        RuntimeException wrapped = BatchSender.wrap(
                new IOException("PUT /lineage returned 408: timeout"));
        assertEquals(RuntimeException.class, wrapped.getClass());
    }

    @Test
    void wrap429StatusIsTransient() {
        RuntimeException wrapped = BatchSender.wrap(
                new IOException("PUT /lineage returned 429: too many"));
        assertEquals(RuntimeException.class, wrapped.getClass());
    }

    @Test
    void wrap500StatusIsTransient() {
        RuntimeException wrapped = BatchSender.wrap(
                new IOException("PUT /lineage returned 500: server"));
        assertEquals(RuntimeException.class, wrapped.getClass());
    }

    @Test
    void wrap404StatusIsPermanent() {
        RuntimeException wrapped = BatchSender.wrap(
                new IOException("Lookup of table x returned 404: not found"));
        assertInstanceOf(BatchSender.PermanentSendException.class, wrapped);
    }

    @Test
    void wrapNullMessageStaysTransient() {
        RuntimeException wrapped = BatchSender.wrap(new IOException((String) null));
        assertEquals(RuntimeException.class, wrapped.getClass());
    }

    private static MetadataEvent sampleEvent() {
        return new LineageEdge(
                new EntityReference(EntityReference.TYPE_KAFKA_TOPIC, "topic-a"),
                new EntityReference(EntityReference.TYPE_ICEBERG_TABLE, "db.tbl"),
                "pipeline-a");
    }

    private static OpenMetadataReporterConfig config(int bufferSize, int batchSize, int maxRetries) {
        return config(bufferSize, batchSize, maxRetries, new HashMap<>());
    }

    private static OpenMetadataReporterConfig config(int bufferSize,
                                                     int batchSize,
                                                     int maxRetries,
                                                     Map<String, Object> overrides) {
        Map<String, Object> m = new HashMap<>();
        m.put(OpenMetadataReporterConfig.URL_CONFIG, "http://localhost:1");
        m.put(OpenMetadataReporterConfig.BUFFER_SIZE_CONFIG, bufferSize);
        m.put(OpenMetadataReporterConfig.BATCH_SIZE_CONFIG, batchSize);
        m.put(OpenMetadataReporterConfig.MAX_RETRIES_CONFIG, maxRetries);
        m.put(OpenMetadataReporterConfig.RETRY_BACKOFF_MS_CONFIG, 1L);
        m.put(OpenMetadataReporterConfig.FLUSH_INTERVAL_MS_CONFIG, 60_000L);
        m.putAll(overrides);
        return new OpenMetadataReporterConfig(m);
    }
}
