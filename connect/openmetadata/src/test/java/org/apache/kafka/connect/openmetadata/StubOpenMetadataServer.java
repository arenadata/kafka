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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Tiny in-process HTTP server backed by the JDK's {@code com.sun.net.httpserver}.
 * Supports per-(method, path-prefix) canned responses and records every request
 * for later assertion.
 */
class StubOpenMetadataServer implements AutoCloseable {

    static final class RecordedRequest {
        final String method;
        final String path;
        final Map<String, String> headers;
        final String body;

        RecordedRequest(String method, String path, Map<String, String> headers, String body) {
            this.method = method;
            this.path = path;
            this.headers = headers;
            this.body = body;
        }
    }

    static final class Response {
        final int status;
        final String body;

        Response(int status, String body) {
            this.status = status;
            this.body = body;
        }
    }

    private final HttpServer server;
    private final List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
    private final Map<String, Response> routes = new ConcurrentHashMap<>();
    private volatile Response defaultResponse = new Response(200, "");

    StubOpenMetadataServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.setExecutor(null);
        server.start();
    }

    String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    /**
     * Register a canned response keyed by "{METHOD} {pathPrefix}" — first match
     * by registration order wins. Use a leading slash on the prefix.
     */
    void on(String method, String pathPrefix, int status, String body) {
        routes.put(method + " " + pathPrefix, new Response(status, body));
    }

    void setDefault(int status, String body) {
        defaultResponse = new Response(status, body);
    }

    List<RecordedRequest> requests() {
        return requests;
    }

    private void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().toString();
        Map<String, String> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        exchange.getRequestHeaders().forEach((k, v) -> {
            if (!v.isEmpty()) {
                headers.put(k, v.get(0));
            }
        });
        byte[] reqBytes = exchange.getRequestBody().readAllBytes();
        String reqBody = new String(reqBytes, StandardCharsets.UTF_8);
        requests.add(new RecordedRequest(method, path, headers, reqBody));

        Response response = routeFor(method, path);
        byte[] bytes = response.body.getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0) {
            exchange.sendResponseHeaders(response.status, -1);
        } else {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(response.status, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
    }

    private Response routeFor(String method, String path) {
        // Iterate in registration order so the most-specific route wins if registered first.
        Map<String, Response> snapshot = new LinkedHashMap<>(routes);
        for (Map.Entry<String, Response> e : snapshot.entrySet()) {
            String key = e.getKey();
            int sp = key.indexOf(' ');
            String routeMethod = key.substring(0, sp);
            String routePrefix = key.substring(sp + 1);
            if (routeMethod.equals(method) && path.startsWith(routePrefix)) {
                return e.getValue();
            }
        }
        return defaultResponse;
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
