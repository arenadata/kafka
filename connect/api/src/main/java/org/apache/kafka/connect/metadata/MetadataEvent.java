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

/**
 * Base class for metadata events reported via {@link MetadataReporter}.
 * <p>
 * Events are immutable value objects. Each concrete subtype carries the
 * fields relevant to one kind of metadata change; reporters dispatch on
 * the runtime type using {@code instanceof}.
 * <p>
 * This class is abstract and sealed in intent: the Connect project defines
 * the permitted subtypes ({@link LineageEdge}, {@link TableCreated},
 * {@link SchemaEvolved}). Downstream code must not extend it directly; new
 * event types will be added here as the metadata model grows.
 *
 * @since 4.2
 */
public abstract class MetadataEvent {

    private final long timestamp;

    /**
     * @param timestamp epoch milliseconds at which the underlying change occurred
     */
    protected MetadataEvent(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * @return epoch milliseconds at which the underlying change occurred
     */
    public final long timestamp() {
        return timestamp;
    }
}
