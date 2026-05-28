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

import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.metadata.EntityReference;
import org.apache.kafka.connect.metadata.LineageEdge;
import org.apache.kafka.connect.metadata.MetadataEvent;
import org.apache.kafka.connect.metadata.MetadataReporter;
import org.apache.kafka.connect.metadata.SchemaEvolved;
import org.apache.kafka.connect.metadata.TableCreated;
import org.apache.kafka.connect.openmetadata.model.ColumnPayload;
import org.apache.kafka.connect.openmetadata.model.EntityRef;
import org.apache.kafka.connect.openmetadata.model.LineageEdgePayload;
import org.apache.kafka.connect.openmetadata.model.TablePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link MetadataReporter} that ships events to an OpenMetadata server over
 * its REST API. Events are buffered in a memory and drained by a background
 * thread; reporting is best-effort and never blocks the data path.
 *
 * @see <a href="https://docs.open-metadata.org/main/connectors/ingestion/lineage">OpenMetadata lineage</a>
 */
public class OpenMetadataReporter implements MetadataReporter {

    private static final Logger log = LoggerFactory.getLogger(OpenMetadataReporter.class);

    private static final String OM_TYPE_TOPIC = "topic";
    private static final String OM_TYPE_TABLE = "table";
    private static final String OM_TYPE_PIPELINE = "pipeline";

    private final AtomicReference<State> state = new AtomicReference<>();

    private static class State {
        final OpenMetadataReporterConfig config;
        final OpenMetadataClient client;
        final BatchSender sender;
        final ScheduledExecutorService executor;

        State(OpenMetadataReporterConfig config, OpenMetadataClient client, BatchSender sender, ScheduledExecutorService executor) {
            this.config = config;
            this.client = client;
            this.sender = sender;
            this.executor = executor;
        }
    }

    @Override
    public void configure(Map<String, ?> rawConfigs) {
        Map<String, Object> scoped = new java.util.HashMap<>();
        for (Map.Entry<String, ?> e : rawConfigs.entrySet()) {
            String key = e.getKey();
            if (key.startsWith("openmetadata.")) {
                scoped.put(key.substring("openmetadata.".length()), e.getValue());
            }
        }

        OpenMetadataReporterConfig config = new OpenMetadataReporterConfig(scoped);
        OpenMetadataClient client = new OpenMetadataClient(config);
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
                ThreadUtils.createThreadFactory("om-metadata-reporter-%d", true));
        BatchSender sender = new BatchSender(config, executor, this::dispatch);
        sender.start();

        if (!state.compareAndSet(null, new State(config, client, sender, executor))) {
            executor.shutdownNow();
            throw new IllegalStateException("OpenMetadataReporter already configured");
        }
    }

    @Override
    public void report(MetadataEvent event) {
        State s = state.get();
        if (s == null || event == null) {
            return;
        }
        s.sender.offer(event);
    }

    @Override
    public void flush() {
        State s = state.get();
        if (s != null) {
            s.sender.drainNow();
        }
    }

    @Override
    public void close() throws IOException {
        State s = state.getAndSet(null);
        if (s == null) {
            return;
        }
        try {
            s.sender.stop();
        } finally {
            s.executor.shutdown();
            try {
                if (!s.executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    s.executor.shutdownNow();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                s.executor.shutdownNow();
            }
        }
    }

    private void dispatch(MetadataEvent event) {
        State s = state.get();
        if (s == null) {
            return;
        }
        try {
            if (event instanceof LineageEdge) {
                handleLineage(s, (LineageEdge) event);
            } else if (event instanceof TableCreated) {
                handleTableCreated(s, (TableCreated) event);
            } else if (event instanceof SchemaEvolved) {
                handleSchemaEvolved(s, (SchemaEvolved) event);
            } else {
                log.debug("Ignoring unknown metadata event type: {}", event.getClass().getName());
            }
        } catch (IOException e) {
            throw BatchSender.wrap(e);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
        }
    }

    private void handleLineage(State s, LineageEdge edge) throws IOException, InterruptedException {
        EntityRef from = resolve(s, edge.source());
        EntityRef to = resolve(s, edge.target());
        if (from == null || to == null) {
            log.debug("Lineage edge skipped, endpoint missing in OpenMetadata: {} -> {}", edge.source(), edge.target());
            return;
        }
        EntityRef pipeline = null;
        if (edge.pipelineName() != null) {
            pipeline = s.client.lookupByFqn(OM_TYPE_PIPELINE, edge.pipelineName());
        }
        s.client.putLineage(new LineageEdgePayload(from, to, pipeline));
    }

    private void handleTableCreated(State s, TableCreated event) throws IOException, InterruptedException {
        TablePayload payload = new TablePayload();
        payload.name = event.tableName();
        payload.databaseSchema = joinNonNull(event.catalogName(), event.databaseName());
        payload.columns = toColumns(event.schema());
        s.client.putTable(payload);
    }

    private void handleSchemaEvolved(State s, SchemaEvolved event) throws IOException, InterruptedException {
        // OpenMetadata applies schema changes as a re-PUT of the full column list.
        TablePayload payload = new TablePayload();
        int lastDot = event.tableFqn().lastIndexOf('.');
        if (lastDot < 0) {
            log.warn("Cannot derive table/schema from FQN '{}'; skipping", event.tableFqn());
            return;
        }
        payload.databaseSchema = event.tableFqn().substring(0, lastDot);
        payload.name = event.tableFqn().substring(lastDot + 1);
        payload.columns = toColumns(event.newSchema());
        s.client.putTable(payload);
    }

    private EntityRef resolve(State s, EntityReference ref) throws IOException, InterruptedException {
        switch (ref.type()) {
            case EntityReference.TYPE_KAFKA_TOPIC:
                return s.client.lookupByFqn(OM_TYPE_TOPIC, ref.name());
            case EntityReference.TYPE_ICEBERG_TABLE:
            case EntityReference.TYPE_JDBC_TABLE:
                return s.client.lookupByFqn(OM_TYPE_TABLE, ref.name());
            default:
                log.debug("Unsupported entity type {}; skipping", ref.type());
                return null;
        }
    }
    private static List<ColumnPayload> toColumns(Schema schema) {
        if (schema == null || schema.type() != Schema.Type.STRUCT) {
            return new ArrayList<>();
        }
        List<ColumnPayload> cols = new ArrayList<>(schema.fields().size());
        for (Field f : schema.fields()) {
            cols.add(new ColumnPayload(f.name(), toOmType(f.schema())));
        }
        return cols;
    }

    private static String toOmType(Schema schema) {
        if (schema == null) return "UNKNOWN";
        switch (schema.type()) {
            case INT8: return "TINYINT";
            case INT16: return "SMALLINT";
            case INT32: return "INT";
            case INT64: return "BIGINT";
            case FLOAT32: return "FLOAT";
            case FLOAT64: return "DOUBLE";
            case BOOLEAN: return "BOOLEAN";
            case STRING: return "STRING";
            case BYTES: return "VARBINARY";
            case ARRAY: return "ARRAY";
            case MAP: return "MAP";
            case STRUCT: return "STRUCT";
            default: return "UNKNOWN";
        }
    }

    private static String joinNonNull(String a, String b) {
        if (a == null || a.isEmpty()) return b;
        if (b == null || b.isEmpty()) return a;
        return a + "." + b;
    }
}
