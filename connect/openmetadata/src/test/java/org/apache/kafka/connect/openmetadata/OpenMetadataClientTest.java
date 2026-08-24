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

import org.apache.kafka.connect.openmetadata.model.ColumnPayload;
import org.apache.kafka.connect.openmetadata.model.EntityRef;
import org.apache.kafka.connect.openmetadata.model.LineageEdgePayload;
import org.apache.kafka.connect.openmetadata.model.TablePayload;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenMetadataClientTest {

    private StubOpenMetadataServer server;

    @BeforeEach
    void setUp() throws IOException {
        server = new StubOpenMetadataServer();
    }

    @AfterEach
    void tearDown() {
        server.close();
    }

    @Test
    void lookupReturnsParsedEntityOnOk() throws Exception {
        server.on("GET", "/api/v1/tables/name/", 200,
                "{\"id\":\"abc-123\",\"fullyQualifiedName\":\"db.schema.tbl\"}");

        OpenMetadataClient client = clientWithToken("secret");
        EntityRef ref = client.lookupByFqn("table", "db.schema.tbl");

        assertNotNull(ref);
        assertEquals("abc-123", ref.id);
        assertEquals("db.schema.tbl", ref.fullyQualifiedName);
        assertEquals("table", ref.type, "client must stamp the entity type onto the result");
    }

    @Test
    void lookupThrowsEntityNotAvailableOn404() {
        server.setDefault(404, "");

        OpenMetadataClient client = clientWithToken("secret");
        assertThrows(EntityNotAvailableException.class,
                () -> client.lookupByFqn("table", "missing.fqn"));
    }

    @Test
    void lookupThrowsOn500() {
        server.setDefault(500, "boom");

        OpenMetadataClient client = clientWithToken("secret");
        IOException ex = assertThrows(IOException.class,
                () -> client.lookupByFqn("table", "any.fqn"));
        assertTrue(ex.getMessage().contains("500"), "status code must appear in the message: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("any.fqn"), "fqn must appear in the message: " + ex.getMessage());
    }

    @Test
    void lookupSendsAuthHeaderWhenTokenSet() throws Exception {
        server.setDefault(200, "{\"id\":\"topic-id\"}");

        OpenMetadataClient client = clientWithToken("my-jwt");
        client.lookupByFqn("topic", "some.topic");

        StubOpenMetadataServer.RecordedRequest req = server.requests().get(0);
        assertEquals("Bearer my-jwt", req.headers.get("Authorization"));
    }

    @Test
    void lookupOmitsAuthHeaderWhenTokenNull() throws Exception {
        server.setDefault(200, "{\"id\":\"topic-id\"}");

        OpenMetadataClient client = clientWithoutToken();
        client.lookupByFqn("topic", "some.topic");

        StubOpenMetadataServer.RecordedRequest req = server.requests().get(0);
        assertFalse(req.headers.containsKey("Authorization"),
                "no Authorization header should be sent when token is null");
    }

    @Test
    void lookupUrlEncodesFqn() throws Exception {
        server.setDefault(200, "{\"id\":\"table-id\"}");

        OpenMetadataClient client = clientWithToken("t");
        client.lookupByFqn("table", "weird name/with chars");

        StubOpenMetadataServer.RecordedRequest req = server.requests().get(0);
        assertTrue(req.path.startsWith("/api/v1/tables/name/"), "path: " + req.path);
        assertFalse(req.path.contains(" "), "spaces must be percent-encoded: " + req.path);
        assertFalse(req.path.contains("/with"), "embedded slashes must be percent-encoded: " + req.path);
    }

    @Test
    void putTableSendsCorrectBodyAndContentType() throws Exception {
        server.on("PUT", "/api/v1/tables", 200, "{}");

        OpenMetadataClient client = clientWithToken("t");
        TablePayload payload = new TablePayload();
        payload.name = "events";
        payload.databaseSchema = "main.public";
        payload.columns = Arrays.asList(new ColumnPayload("id", "BIGINT"));
        client.putTable(payload);

        StubOpenMetadataServer.RecordedRequest req = server.requests().get(0);
        assertEquals("PUT", req.method);
        assertEquals("/api/v1/tables", req.path);
        assertEquals("application/json", req.headers.get("Content-Type"));
        assertTrue(req.body.contains("\"name\":\"events\""), "body: " + req.body);
        assertTrue(req.body.contains("\"databaseSchema\":\"main.public\""), "body: " + req.body);
        assertTrue(req.body.contains("\"dataType\":\"BIGINT\""), "body: " + req.body);
    }

    @Test
    void putTableThrowsOn500() {
        server.on("PUT", "/api/v1/tables", 500, "broken");

        OpenMetadataClient client = clientWithToken("t");
        TablePayload payload = new TablePayload();
        payload.name = "x";
        payload.databaseSchema = "y";

        IOException ex = assertThrows(IOException.class, () -> client.putTable(payload));
        assertTrue(ex.getMessage().contains("returned 500"), "msg: " + ex.getMessage());
    }

    @Test
    void putLineageSendsEdgeBody() throws Exception {
        server.on("PUT", "/api/v1/lineage", 200, "{}");

        OpenMetadataClient client = clientWithToken("t");
        EntityRef from = new EntityRef("topic", "events");
        from.id = "from-id";
        EntityRef to = new EntityRef("table", "main.public.events");
        to.id = "to-id";
        LineageEdgePayload payload = new LineageEdgePayload(from, to, null);

        client.putLineage(payload);

        StubOpenMetadataServer.RecordedRequest req = server.requests().get(0);
        assertEquals("PUT", req.method);
        assertEquals("/api/v1/lineage", req.path);
        assertTrue(req.body.contains("\"fromEntity\""), "body: " + req.body);
        assertTrue(req.body.contains("\"toEntity\""), "body: " + req.body);
        assertTrue(req.body.contains("\"id\":\"from-id\""), "body: " + req.body);
        assertTrue(req.body.contains("\"id\":\"to-id\""), "body: " + req.body);
    }

    @Test
    void putLineageThrowsOn400() {
        server.on("PUT", "/api/v1/lineage", 400, "bad");

        OpenMetadataClient client = clientWithToken("t");
        LineageEdgePayload payload = new LineageEdgePayload(
                new EntityRef("topic", "t"), new EntityRef("table", "x.y.z"), null);

        IOException ex = assertThrows(IOException.class, () -> client.putLineage(payload));
        assertTrue(ex.getMessage().contains("returned 400"), "msg: " + ex.getMessage());
    }

    @Test
    void urlTrailingSlashIsStrippedBeforeAppendingApiV1() throws Exception {
        server.setDefault(200, "{\"id\":\"topic-id\"}");

        Map<String, Object> props = baseProps();
        props.put(OpenMetadataReporterConfig.URL_CONFIG, server.url() + "/");
        OpenMetadataClient client = new OpenMetadataClient(new OpenMetadataReporterConfig(props));
        client.lookupByFqn("topic", "x");

        StubOpenMetadataServer.RecordedRequest req = server.requests().get(0);
        assertFalse(req.path.startsWith("//"), "path must not double-slash after base URL: " + req.path);
        assertTrue(req.path.startsWith("/api/v1/"), "path: " + req.path);
    }

    @Test
    void isPermanentClassification() {
        assertTrue(OpenMetadataClient.isPermanent(400));
        assertTrue(OpenMetadataClient.isPermanent(401));
        assertTrue(OpenMetadataClient.isPermanent(404));
        assertFalse(OpenMetadataClient.isPermanent(408), "408 must be transient");
        assertFalse(OpenMetadataClient.isPermanent(429), "429 must be transient");
        assertFalse(OpenMetadataClient.isPermanent(500));
        assertFalse(OpenMetadataClient.isPermanent(503));
        assertFalse(OpenMetadataClient.isPermanent(200));
    }

    private OpenMetadataClient clientWithToken(String token) {
        Map<String, Object> props = baseProps();
        props.put(OpenMetadataReporterConfig.TOKEN_CONFIG, token);
        return new OpenMetadataClient(new OpenMetadataReporterConfig(props));
    }

    private OpenMetadataClient clientWithoutToken() {
        return new OpenMetadataClient(new OpenMetadataReporterConfig(baseProps()));
    }

    private Map<String, Object> baseProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(OpenMetadataReporterConfig.URL_CONFIG, server.url());
        props.put(OpenMetadataReporterConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000L);
        return props;
    }
}
