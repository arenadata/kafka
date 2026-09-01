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
package org.apache.kafka.connect.metadata;

import org.apache.kafka.common.Configurable;

import java.io.Closeable;

/**
 * SPI that Kafka Connect connectors use to report metadata events (lineage,
 * table creation, schema evolution) to an external catalog or discovery system.
 * <p>
 * Implementations are loaded by the Connect runtime via the
 * {@code metadata.reporter.class} connector configuration. A single instance
 * is constructed per task and injected into the task's context. Connectors
 * obtain it via {@link org.apache.kafka.connect.sink.SinkTaskContext#metadataReporter()}
 * or {@link org.apache.kafka.connect.source.SourceTaskContext#metadataReporter()}.
 * <p>
 * Reporting is best-effort: {@link #report(MetadataEvent)} must not block the
 * data path and must not throw. Implementations are expected to buffer events
 * internally and deliver them asynchronously.
 * <p>
 * Reporter JARs installed through the worker's {@code plugin.path} must include
 * a {@link java.util.ServiceLoader} provider configuration for this interface.
 *
 * @since 4.2
 */
public interface MetadataReporter extends Configurable, Closeable {

    /**
     * Submit a metadata event for asynchrounous delivery. Must not block or throw.
     * If the implementation's buffer is full, the event should be dropped (with
     * a log warning) rather than back-pressuring the caller.
     *
     * @param event the event to report; must not be null
     */
    void report(MetadataEvent event);

    /**
     * Block until all events submitted before this call have been delivered
     * (or permanently failed). Called by the runtime on task stop or rebalance.
     * <p>
     * The default implementation is a no-op; synchronous reporters may leave it
     * as-is.
     */
    default void flush() {}
}
