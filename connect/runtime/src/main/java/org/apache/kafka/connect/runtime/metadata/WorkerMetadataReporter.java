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
package org.apache.kafka.connect.runtime.metadata;

import org.apache.kafka.connect.metadata.MetadataEvent;
import org.apache.kafka.connect.metadata.MetadataReporter;
import org.apache.kafka.connect.runtime.isolation.LoaderSwap;
import org.apache.kafka.connect.util.ConnectorTaskId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Runtime wrapper around a user-configured {@link MetadataReporter}. Owned by
 * the worker task; exposed to connector code via
 * {@link org.apache.kafka.connect.sink.SinkTaskContext#metadataReporter()} or
 * {@link org.apache.kafka.connect.source.SourceTaskContext#metadataReporter()}.
 * <p>
 * Responsibilities:
 * <ul>
 *     <li>Swap the thread context classloader to the plugin's classloader on
 *     every call, so the user reporter can resolve its own dependencies.</li>
 *     <li>Swallow exceptions thrown by {@link #report(MetadataEvent)} so that
 *     metadata reporting failures never affect the data path.</li>
 *     <li>Propagate {@link #flush()} and {@link #close()} to the underlying
 *     plugin during task shutdown</li>
 * </ul>
 */
public class WorkerMetadataReporter implements MetadataReporter {

    private static final Logger log = LoggerFactory.getLogger(WorkerMetadataReporter.class);

    private final ConnectorTaskId taskId;
    private final MetadataReporter delegate;
    private final ClassLoader pluginLoader;
    private final Function<ClassLoader, LoaderSwap> loaderSwapper;

    public WorkerMetadataReporter(ConnectorTaskId taskId,
                                  MetadataReporter delegate,
                                  ClassLoader pluginLoader,
                                  Function<ClassLoader, LoaderSwap> loaderSwapper) {
        this.taskId = Objects.requireNonNull(taskId, "taskId");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.pluginLoader = Objects.requireNonNull(pluginLoader, "pluginLoader");
        this.loaderSwapper = Objects.requireNonNull(loaderSwapper, "loaderSwapper");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No-op: the worker called configure() on the delegate before constructing
        // this wrapper. Kept for interface compliance. Calling it a second time
        // would reset reporter state unexpectedly.
    }

    @Override
    public void report(MetadataEvent event) {
        if (event == null) {
            return;
        }
        try (LoaderSwap  ignored = loaderSwapper.apply(pluginLoader)) {
            delegate.report(event);
        } catch (Throwable t) {
            log.warn("{} Metadata reporter threw while reporting event {}; dropping", taskId, event, t);
        }
    }

    @Override
    public void flush() {
        try (LoaderSwap  ignored = loaderSwapper.apply(pluginLoader)) {
            delegate.flush();
        } catch (Throwable t) {
            log.warn("{} Metadata reporter threw during flush", taskId, t);
        }
    }

    @Override
    public void close() throws IOException {
        try (LoaderSwap ignored = loaderSwapper.apply(pluginLoader)) {
            delegate.close();
        } catch (IOException e) {
            log.warn("{} Metadata reporter threw during close", taskId, e);
            throw e;
        } catch (Throwable t) {
            log.warn("{} Metadata reporter threw during close", taskId, t);
        }
    }
}
