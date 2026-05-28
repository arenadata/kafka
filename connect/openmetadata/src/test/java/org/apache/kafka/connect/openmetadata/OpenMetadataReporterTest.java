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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.metadata.EntityReference;
import org.apache.kafka.connect.metadata.LineageEdge;
import org.apache.kafka.connect.metadata.SchemaEvolved;
import org.apache.kafka.connect.metadata.TableCreated;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenMetadataReporterTest {

    private StubOpenMetadataServer server;
    private OpenMetadataReporter reporter;

    @BeforeEach
    void setUp() throws IOException {
        server = new StubOpenMetadataServer();
        reporter = new OpenMetadataReporter();
    }

    @AfterEach
    void tearDown() throws IOException {
        try {
            reporter.close();
        } finally {
            server.close();
        }
    }

    @Test
    void lineageEdgeLooksUpEndpointsAndPostsLineage() {
        server.on("GET", "/api/v1/topic/name/", 200, "{\"id\":\"topic-id\"}");
        server.on("GET", "/api/v1/table/name/", 200, "{\"id\":\"table-id\"}");
        server.on("GET", "/api/v1/pipeline/name/", 200, "{\"id\":\"pipe-id\"}");
        server.on("PUT", "/api/v1/lineage", 200, "{}");

        configure();
        reporter.report(new LineageEdge(
                new EntityReference(EntityReference.TYPE_KAFKA_TOPIC, "orders"),
                new EntityReference(EntityReference.TYPE_ICEBERG_TABLE, "warehouse.orders"),
                "iceberg-sink"));
        reporter.flush();

        List<StubOpenMetadataServer.RecordedRequest> reqs = server.requests();
        assertEquals(4, reqs.size(), "expected source lookup, target lookup, pipeline lookup, PUT /lineage");
        assertEquals("GET", reqs.get(0).method);
        assertTrue(reqs.get(0).path.startsWith("/api/v1/topic/name/"), reqs.get(0).path);
        assertEquals("GET", reqs.get(1).method);
        assertTrue(reqs.get(1).path.startsWith("/api/v1/table/name/"), reqs.get(1).path);
        assertEquals("GET", reqs.get(2).method);
        assertTrue(reqs.get(2).path.startsWith("/api/v1/pipeline/name/"), reqs.get(2).path);
        assertEquals("PUT", reqs.get(3).method);
        assertEquals("/api/v1/lineage", reqs.get(3).path);
        assertTrue(reqs.get(3).body.contains("\"id\":\"topic-id\""), "from id missing: " + reqs.get(3).body);
        assertTrue(reqs.get(3).body.contains("\"id\":\"table-id\""), "to id missing: " + reqs.get(3).body);
        assertTrue(reqs.get(3).body.contains("\"id\":\"pipe-id\""), "pipeline id missing: " + reqs.get(3).body);
    }

    @Test
    void lineageEdgeSkipsPutWhenSourceLookupReturns404() {
        server.on("GET", "/api/v1/topic/name/", 404, "");
        server.on("GET", "/api/v1/table/name/", 200, "{\"id\":\"table-id\"}");

        configure();
        reporter.report(new LineageEdge(
                new EntityReference(EntityReference.TYPE_KAFKA_TOPIC, "missing"),
                new EntityReference(EntityReference.TYPE_ICEBERG_TABLE, "warehouse.orders"),
                "p"));
        reporter.flush();

        List<StubOpenMetadataServer.RecordedRequest> reqs = server.requests();
        assertTrue(reqs.size() <= 2, "no PUT /lineage when source is missing; got " + reqs.size() + " requests");
        for (StubOpenMetadataServer.RecordedRequest r : reqs) {
            assertEquals("GET", r.method, "unexpected non-GET request: " + r.method + " " + r.path);
        }
    }

    @Test
    void unsupportedEntityTypeSkipsLineageWithoutLookup() {
        server.on("GET", "/api/v1/table/name/", 200, "{\"id\":\"table-id\"}");

        configure();
        reporter.report(new LineageEdge(
                new EntityReference("clickhouse.table", "x"),
                new EntityReference(EntityReference.TYPE_ICEBERG_TABLE, "warehouse.orders"),
                "p"));
        reporter.flush();

        List<StubOpenMetadataServer.RecordedRequest> reqs = server.requests();
        for (StubOpenMetadataServer.RecordedRequest r : reqs) {
            assertEquals("GET", r.method, "no PUT /lineage when source type is unsupported");
            assertTrue(r.path.startsWith("/api/v1/table/name/"),
                    "no lookup should be issued for the unsupported source: " + r.path);
        }
    }

    @Test
    void tableCreatedPutsTableWithDottedDatabaseSchema() {
        server.on("PUT", "/api/v1/tables", 200, "{}");

        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        configure();
        reporter.report(new TableCreated("cat", "db", "events", schema));
        reporter.flush();

        List<StubOpenMetadataServer.RecordedRequest> reqs = server.requests();
        assertEquals(1, reqs.size());
        StubOpenMetadataServer.RecordedRequest req = reqs.get(0);
        assertEquals("PUT", req.method);
        assertEquals("/api/v1/tables", req.path);
        assertTrue(req.body.contains("\"name\":\"events\""), "body: " + req.body);
        assertTrue(req.body.contains("\"databaseSchema\":\"cat.db\""),
                "catalog and database must join with a dot; body: " + req.body);
        assertTrue(req.body.contains("\"dataType\":\"BIGINT\""), "id column must map INT64->BIGINT; body: " + req.body);
        assertTrue(req.body.contains("\"dataType\":\"STRING\""), "name column must map STRING; body: " + req.body);
    }

    @Test
    void tableCreatedWithNullCatalogUsesDatabaseAsSchema() {
        server.on("PUT", "/api/v1/tables", 200, "{}");

        configure();
        reporter.report(new TableCreated(null, "db", "events",
                SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build()));
        reporter.flush();

        StubOpenMetadataServer.RecordedRequest req = server.requests().get(0);
        assertTrue(req.body.contains("\"databaseSchema\":\"db\""), "body: " + req.body);
    }

    @Test
    void schemaEvolvedSplitsFqnIntoSchemaAndName() {
        server.on("PUT", "/api/v1/tables", 200, "{}");

        Schema newSchema = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .field("email", Schema.STRING_SCHEMA)
                .build();
        configure();
        reporter.report(new SchemaEvolved("cat.db.users", null, newSchema));
        reporter.flush();

        StubOpenMetadataServer.RecordedRequest req = server.requests().get(0);
        assertEquals("PUT", req.method);
        assertEquals("/api/v1/tables", req.path);
        assertTrue(req.body.contains("\"name\":\"users\""), "body: " + req.body);
        assertTrue(req.body.contains("\"databaseSchema\":\"cat.db\""), "body: " + req.body);
        assertTrue(req.body.contains("\"dataType\":\"STRING\""), "body: " + req.body);
    }

    @Test
    void reportBeforeConfigureIsSilentlyDropped() {
        OpenMetadataReporter unconfigured = new OpenMetadataReporter();
        assertDoesNotThrow(() -> unconfigured.report(new TableCreated("c", "d", "t",
                SchemaBuilder.struct().field("x", Schema.INT64_SCHEMA).build())));
        assertDoesNotThrow(unconfigured::flush);
        assertDoesNotThrow(unconfigured::close);
        assertEquals(0, server.requests().size(), "no HTTP traffic expected without configure()");
    }

    @Test
    void reportAfterCloseIsSilentlyDropped() throws IOException {
        configure();
        reporter.close();

        assertDoesNotThrow(() -> reporter.report(new TableCreated("c", "d", "t",
                SchemaBuilder.struct().field("x", Schema.INT64_SCHEMA).build())));
        assertDoesNotThrow(reporter::flush);

        assertEquals(0, server.requests().size(), "no HTTP traffic expected after close()");
    }

    @Test
    void doubleConfigureThrows() {
        configure();
        Map<String, Object> props = props();
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> reporter.configure(props));
    }

    @Test
    void authorizationHeaderIncludesToken() {
        server.on("PUT", "/api/v1/tables", 200, "{}");

        configure();
        reporter.report(new TableCreated("c", "d", "t",
                SchemaBuilder.struct().field("x", Schema.INT64_SCHEMA).build()));
        reporter.flush();

        StubOpenMetadataServer.RecordedRequest req = server.requests().get(0);
        assertEquals("Bearer test-token", req.headers.get("Authorization"));
    }

    private void configure() {
        reporter.configure(props());
    }

    private Map<String, Object> props() {
        Map<String, Object> p = new HashMap<>();
        p.put("openmetadata." + OpenMetadataReporterConfig.URL_CONFIG, server.url());
        p.put("openmetadata." + OpenMetadataReporterConfig.TOKEN_CONFIG, "test-token");
        // Long flush interval — tests drive draining via reporter.flush() instead.
        p.put("openmetadata." + OpenMetadataReporterConfig.FLUSH_INTERVAL_MS_CONFIG, 60_000L);
        // Zero retries for deterministic single-attempt assertions.
        p.put("openmetadata." + OpenMetadataReporterConfig.MAX_RETRIES_CONFIG, 0);
        p.put("openmetadata." + OpenMetadataReporterConfig.RETRY_BACKOFF_MS_CONFIG, 1L);
        p.put("openmetadata." + OpenMetadataReporterConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000L);
        return p;
    }
}
