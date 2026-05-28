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
 * Signals that a new table (or equivalent downstream entity) was created.
 * Reported by sink connectors that auto-create destinations.
 *
 * @since 4.2
 */
public class TableCreated extends MetadataEvent {

    private final String catalogName;
    private final String databaseName;
    private final String tableName;
    private final Schema schema;

    public TableCreated(String catalogName,
                        String databaseName,
                        String tableName,
                        Schema schema) {
        this(System.currentTimeMillis(), catalogName, databaseName, tableName, schema);
    }

    public TableCreated(long timestamp,
                        String catalogName,
                        String databaseName,
                        String tableName,
                        Schema schema) {
        super(timestamp);
        this.catalogName = catalogName;
        this.databaseName = Objects.requireNonNull(databaseName, "databaseName");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.schema = Objects.requireNonNull(schema, "schema");
    }

    /**
     * @return the catalog name, or {@code null} if the target system has no
     * catalog concept (e.g. a flat JDBC schema).
     */
    public String catalogName() {
        return catalogName;
    }

    public String databaseName() {
        return databaseName;
    }

    public String tableName() {
        return tableName;
    }

    public Schema schema() {
        return schema;
    }
}
