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

import org.apache.kafka.connect.data.Schema;

import java.util.Objects;

/**
 * Signals that the schema of an existing downstream table was evolved
 * (columns added, types widened, etc.). Reported by sink connectors that
 * perform DDL in response to upstream schema changes.
 *
 * @since 4.2
 */
public class SchemaEvolved extends MetadataEvent {

    private final String tableFqn;
    private final Schema oldSchema;
    private final Schema newSchema;

    public SchemaEvolved(String tableFqn, Schema oldSchema, Schema newSchema) {
        this(System.currentTimeMillis(), tableFqn, oldSchema, newSchema);
    }

    public SchemaEvolved(long timestamp, String tableFqn, Schema oldSchema, Schema newSchema) {
        super(timestamp);
        this.tableFqn = Objects.requireNonNull(tableFqn, "tableFqn");
        this.oldSchema = oldSchema;
        this.newSchema = Objects.requireNonNull(newSchema, "newSchema");
    }

    public String tableFqn() {
        return tableFqn;
    }

    /**
     * @return the schema prior to the change, or {@code null} if the connector
     * did not capture or track the precious schema.
     */
    public Schema oldSchema() {
        return oldSchema;
    }

    public Schema newSchema() {
        return newSchema;
    }
}
