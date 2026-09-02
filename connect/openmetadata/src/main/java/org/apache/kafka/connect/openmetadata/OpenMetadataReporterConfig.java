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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.types.Password;

import java.time.Duration;
import java.util.Map;

public class OpenMetadataReporterConfig extends AbstractConfig {

    public static final String URL_CONFIG = "url";
    private static final String URL_DOC =
            "Base URL of the OpenMetadata server, e.g. https://openmetadata.example.com. "
                    + "The /api/v1 path suffix is appended automatically.";

    public static final String TOKEN_CONFIG = "token";
    private static final String TOKEN_DOC =
            "Bearer token used for OpenMetadata API authentication. May be a JWT "
                    + "or any opaque string accepted by the sever. Read from a file with "
                    + "the standard Kafka Connect ${file: ...} indirection if desired.";

    public static final String BATCH_SIZE_CONFIG = "batch.size";
    public static final int BATCH_SIZE_DEFAULT = 50;
    private static final String BATCH_SIZE_DOC =
            "Maximum number of metadata events drained from the buffer in one batch.";

    public static final String FLUSH_INTERVAL_MS_CONFIG = "flush.interval.ms";
    public static final long FLUSH_INTERVAL_MS_DEFAULT = 10_000L;
    private static final String FLUSH_INTERVAL_MS_DOC =
            "How often the background sender wakes up to drain the buffer, in milliseconds.";

    public static final String BUFFER_SIZE_CONFIG = "buffer.size";
    public static final int BUFFER_SIZE_DEFAULT = 1_000;
    private static final String BUFFER_SIZE_DOC =
            "Maximum number of events to buffer in memory. New events are dropped "
                    + "(with a log warning) when the buffer is full.";

    public static final String REQUEST_TIMEOUT_MS_CONFIG = "request.timeout.ms";
    public static final long REQUEST_TIMEOUT_MS_DEFAULT = 30_000L;
    private static final String REQUEST_TIMEOUT_MS_DOC =
            "HTTP request timeout  for calls to the OpenMetadata API.";

    public static final String MAX_RETRIES_CONFIG = "max.retries";
    public static final int MAX_RETRIES_MS_DEFAULT = 3;
    private static final String MAX_RETRIES_DOC =
            "Maximum number of retry attempts for transient (5xx, network) failures. "
                    + "4xx responses are not retried.";

    public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
    public static final long RETRY_BACKOFF_MS_DEFAULT = 500L;
    private static final String RETRY_BACKOFF_MS_DOC =
            "Initial backoff between retry attempts, doubled on each subsequent retry.";

    public static final String VERIFY_SSL_CONFIG = "verify.ssl";
    public static final boolean VERIFY_SSL_DEFAULT = true;
    private static final String VERIFY_SSL_DOC =
            "Whether to verify the OpenMEtadata server's TLS certificate.";

    public static final String ENTITY_NOT_AVAILABLE_RETRY_TIMEOUT_MINUTES_CONFIG =
            "entity.not.available.retry.timeout.minutes";
    public static final long ENTITY_NOT_AVAILABLE_RETRY_TIMEOUT_MINUTES_DEFAULT = 1_440L;
    private static final String ENTITY_NOT_AVAILABLE_RETRY_TIMEOUT_MINUTES_DOC =
            "Maximum time, in minutes, to retry a metadata event when a referenced "
                    + "entity is not yet available in OpenMetadata. The timeout is measured "
                    + "from the event timestamp. Set to 0 to retry indefinitely.";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(URL_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    Importance.HIGH, URL_DOC, "Connection", 1, Width.LONG, "OpenMetadata URL")
            .define(TOKEN_CONFIG, Type.PASSWORD, null,
                    Importance.HIGH, TOKEN_DOC, "Connection", 2, Width.LONG, "Bearer token")
            .define(VERIFY_SSL_CONFIG, Type.BOOLEAN, VERIFY_SSL_DEFAULT,
                    Importance.LOW, VERIFY_SSL_DOC, "Connection", 3, Width.SHORT, "Verify TLS")
            .define(REQUEST_TIMEOUT_MS_CONFIG, Type.LONG, REQUEST_TIMEOUT_MS_DEFAULT,
                    Importance.LOW, REQUEST_TIMEOUT_MS_DOC, "Connection", 4, Width.SHORT, "Request timeout")
            .define(BUFFER_SIZE_CONFIG, Type.INT, BUFFER_SIZE_DEFAULT,
                    Importance.MEDIUM, BUFFER_SIZE_DOC, "Buffering", 1, Width.SHORT, "Buffer size")
            .define(BATCH_SIZE_CONFIG, Type.INT, BATCH_SIZE_DEFAULT,
                    Importance.MEDIUM, BATCH_SIZE_DOC, "Buffering", 2, Width.SHORT, "Batch size")
            .define(FLUSH_INTERVAL_MS_CONFIG, Type.LONG, FLUSH_INTERVAL_MS_DEFAULT,
                    Importance.MEDIUM, FLUSH_INTERVAL_MS_DOC, "Buffering", 3, Width.SHORT, "Flush interval (ms)")
            .define(MAX_RETRIES_CONFIG, Type.INT, MAX_RETRIES_MS_DEFAULT,
                    Importance.LOW, MAX_RETRIES_DOC, "Retry", 1, Width.SHORT, "Max retries")
            .define(RETRY_BACKOFF_MS_CONFIG, Type.LONG, RETRY_BACKOFF_MS_DEFAULT,
                    Importance.LOW, RETRY_BACKOFF_MS_DOC, "Retry", 2, Width.SHORT, "Retry backoff (ms)")
            .define(ENTITY_NOT_AVAILABLE_RETRY_TIMEOUT_MINUTES_CONFIG, Type.LONG,
                    ENTITY_NOT_AVAILABLE_RETRY_TIMEOUT_MINUTES_DEFAULT, ConfigDef.Range.atLeast(0L),
                    Importance.MEDIUM, ENTITY_NOT_AVAILABLE_RETRY_TIMEOUT_MINUTES_DOC,
                    "Retry", 3, Width.SHORT, "Entity availability retry timeout (minutes)");

    public OpenMetadataReporterConfig(Map<String, ?> originals) {
        super(CONFIG_DEF, originals);
    }

    public String url() {
        String raw = getString(URL_CONFIG);
        return raw.endsWith("/") ? raw.substring(0, raw.length() - 1) : raw;
    }

    public Password token() {
        return getPassword(TOKEN_CONFIG);
    }

    public boolean verifySsl() {
        return getBoolean(VERIFY_SSL_CONFIG);
    }

    public Duration requestTimeout() {
        return Duration.ofMillis(getLong(REQUEST_TIMEOUT_MS_CONFIG));
    }

    public int bufferSize() {
        return getInt(BUFFER_SIZE_CONFIG);
    }

    public int batchSize() {
        return getInt(BATCH_SIZE_CONFIG);
    }

    public Duration flushInterval() {
        return Duration.ofMillis(getLong(FLUSH_INTERVAL_MS_CONFIG));
    }

    public int maxRetries() {
        return getInt(MAX_RETRIES_CONFIG);
    }

    public Duration retryBackoff() {
        return Duration.ofMillis(getLong(RETRY_BACKOFF_MS_CONFIG));
    }

    public static ConfigDef configDef() {
        return CONFIG_DEF;
    }

    public Duration entityNotAvailableRetryTimeout() {
        return Duration.ofMinutes(getLong(ENTITY_NOT_AVAILABLE_RETRY_TIMEOUT_MINUTES_CONFIG));
    }
}
