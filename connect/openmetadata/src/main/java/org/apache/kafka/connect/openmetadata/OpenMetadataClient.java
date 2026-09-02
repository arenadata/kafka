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

import org.apache.kafka.connect.openmetadata.model.EntityRef;
import org.apache.kafka.connect.openmetadata.model.LineageEdgePayload;
import org.apache.kafka.connect.openmetadata.model.TablePayload;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * Wrapper over {@link HttpClient} that knows how to talk to a few
 * OpenMetadata REST endpoints. All retry/backoff lives in {@link BatchSender};
 * this class does one HTTP call per method and rethrows.
 */
public class OpenMetadataClient {

    private final HttpClient http;
    private final ObjectMapper mapper;
    private final String baseUrl;
    private final String authHeader;
    private final Duration requestTimeout;

    private static final String API_URI = "/api/v1";

    public OpenMetadataClient(OpenMetadataReporterConfig config) {
        this.baseUrl = config.url() + API_URI;
        this.authHeader = config.token() == null ? null : "Bearer " + config.token().value();
        this.requestTimeout = config.requestTimeout();
        this.mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        HttpClient.Builder builder = HttpClient.newBuilder().connectTimeout(config.requestTimeout());
        if (!config.verifySsl()) {
            builder.sslContext(insecureContext());
        }
        this.http = builder.build();
    }

    public EntityRef lookupByFqn(String entityType, String fqn) throws IOException, InterruptedException {
        URI uri = URI.create(baseUrl + "/" + entityType + "s/name/" + encode(fqn));
        HttpResponse<String> response = send(HttpRequest.newBuilder(uri).GET());
        if (response.statusCode() == 404) {
            throw new EntityNotAvailableException(
                    "Lookup of " + entityType + " " + fqn + " returned 404: " + response.body());
        }
        if (response.statusCode() / 100 != 2) {
            throw new IOException("Lookup of " + entityType + " " + fqn + " returned " + response.statusCode() + ": " + response.body());
        }
        EntityRef ref = mapper.readValue(response.body(), EntityRef.class);
        ref.type = entityType;
        return ref;
    }

    public void putTable(TablePayload payload) throws IOException, InterruptedException {
        URI uri = URI.create(baseUrl + "/tables");
        String body = mapper.writeValueAsString(payload);
        HttpResponse<String> response = send(HttpRequest.newBuilder(uri)
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)));
        if (response.statusCode() / 100 != 2) {
            throw new IOException("PUT /tables returned " + response.statusCode() + ": " + response.body());
        }
    }

    public void putLineage(LineageEdgePayload payload) throws IOException, InterruptedException {
        URI uri = URI.create(baseUrl + "/lineage");
        String body = mapper.writeValueAsString(payload);
        HttpResponse<String> response = send(HttpRequest.newBuilder(uri)
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)));
        if (response.statusCode() == 404) {
            throw new EntityNotAvailableException("PUT /lineage returned 404: " + response.body());
        }
        if (response.statusCode() / 100 != 2) {
            throw new IOException("PUT /lineage returned " + response.statusCode() + ": " + response.body());
        }
    }

    private HttpResponse<String> send(HttpRequest.Builder builder) throws IOException, InterruptedException {
        builder.timeout(requestTimeout);
        if (authHeader != null) {
            builder.header("Authorization", authHeader);
        }
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    static boolean isPermanent(int statusCode) {
        return statusCode >= 400 && statusCode < 500 && statusCode != 408 && statusCode != 429;
    }

    private static String encode(String fqn) {
        return  java.net.URLEncoder.encode(fqn, StandardCharsets.UTF_8);
    }

    private static SSLContext  insecureContext() {
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, new TrustManager[] {
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] chain, String authType) {}
                    @Override
                    public void checkServerTrusted(X509Certificate[] chain, String authType) {}
                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                }
            }, new SecureRandom());
            return ctx;
        } catch (Exception e) {
            throw new IllegalStateException("Cannot create insecure SSL context", e);
        }
    }
}
