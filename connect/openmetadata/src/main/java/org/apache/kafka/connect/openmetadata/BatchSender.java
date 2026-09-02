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

import org.apache.kafka.connect.metadata.MetadataEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Owns the in-memory event buffer and the background draining thread. the
 * actual serialization-and-HTTP per event is delegated to a callback so this
 * class can be unit-tested in isolation.
 */
public class BatchSender {

    private static final Logger log = LoggerFactory.getLogger(BatchSender.class);

    private final BlockingQueue<MetadataEvent> buffer;
    private final ScheduledExecutorService executor;
    private final OpenMetadataReporterConfig config;
    private final Consumer<MetadataEvent> sink;
    private final AtomicLong droppedEvents = new AtomicLong();

    private ScheduledFuture<?> task;

    public BatchSender(OpenMetadataReporterConfig config, ScheduledExecutorService executor, Consumer<MetadataEvent> sink) {
        this.config = config;
        this.executor = executor;
        this.sink = sink;
        this.buffer = new LinkedBlockingQueue<>(config.bufferSize());
    }

    public void start() {
        long intervalMs = config.flushInterval().toMillis();
        task = executor.scheduleWithFixedDelay(this::drainSafely, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void offer(MetadataEvent event) {
        if (!buffer.offer(event)) {
            long dropped = droppedEvents.incrementAndGet();
            if (Long.bitCount(dropped) == 1) {
                log.warn("Metadata reporter buffer full; dropped {} events so far. "
                        + "Consider increasing {}.", dropped, OpenMetadataReporterConfig.BUFFER_SIZE_CONFIG);
            }
        }
    }

    public void drainNow() {
        drainAllSafely();
    }

    public void stop() {
        if (task != null) {
            task.cancel(false);
        }
        drainAllSafely();
    }

    public long droppedEventsCount() {
        return droppedEvents.get();
    }

    public int bufferDepth() {
        return buffer.size();
    }

    private void drainSafely() {
        try {
            drainOnce();
        } catch (Throwable t) {
            log.warn("Metadata batch drain failed", t);
        }
    }

    private void drainAllSafely() {
        try {
            drainAvailable();
        } catch (Throwable t) {
            log.warn("Metadata buffer drain failed", t);
        }
    }

    private synchronized void drainAvailable() {
        int remaining = buffer.size();
        while (remaining > 0) {
            int drained = drainOnce(Math.min(config.batchSize(), remaining));
            if (drained == 0) {
                return;
            }
            remaining -= drained;
        }
    }

    private synchronized void drainOnce() {
        drainOnce(config.batchSize());
    }

    private int drainOnce(int maxEvents) {
        List<MetadataEvent> batch = new ArrayList<>(maxEvents);
        buffer.drainTo(batch, maxEvents);
        for (MetadataEvent event : batch) {
            sendWithRetry(event);
        }
        return batch.size();
    }

    private void sendWithRetry(MetadataEvent event) {
        int attempt = 0;
        long backoffMs = config.retryBackoff().toMillis();
        int maxRetries = config.maxRetries();
        while (true) {
            try {
                sink.accept(event);
                return;
            } catch (DeferredSendException e) {
                long timeoutMs = config.entityNotAvailableRetryTimeout().toMillis();
                if (timeoutMs > 0 && event.timestamp() + timeoutMs < System.currentTimeMillis()) {
                    log.warn("Dropping metadata event {} because referenced entities were not available "
                            + "within {} ms", event, timeoutMs);
                    return;
                }
                log.debug("Deferring metadata event until a later flush: {}", event, e);
                offer(event);
                return;
            } catch (PermanentSendException e) {
                log.warn("Dropping metadata event {}: {}", event, e.getMessage());
                return;
            } catch (RuntimeException e) {
                if (attempt >= maxRetries) {
                    log.warn("Giving up in metadata event {} after {} retries", event, attempt, e);
                    return;
                }
                attempt++;
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
                backoffMs = Math.min(backoffMs * 2, 30_000L);
            }
        }
    }

    static class PermanentSendException extends RuntimeException {
        PermanentSendException(String msg) {
            super(msg);
        }
        PermanentSendException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    static RuntimeException wrap(IOException e) {
        if (e.getMessage() != null) {
            for (int code = 400; code < 500; code++) {
                if (code == 408 || code == 429) continue;
                if (e.getMessage().contains("returned " + code)) {
                    return new PermanentSendException(e.getMessage(), e);
                }
            }
        }
        return new RuntimeException(e);
    }
}
