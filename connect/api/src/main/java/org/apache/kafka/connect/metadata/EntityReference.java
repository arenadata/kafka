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

import java.util.Objects;

/**
 * Identifies a data entity referenced by a metadata event, such as a Kafka
 * topic or a downstream table. The pair ({@link #type()}, {@link #name()})
 * is expected to be unique within the target catalog's namespace.
 * <p>
 * {@code type} is a free-form string (e.g. {@code "kafka.topic"},
 * {@code "iceberg.table"}, {@code "jdbc.table"}) so that new source/sink
 * systems can be added without changing this API.
 *
 * @since 4.2
 */
public final class EntityReference {

    public static final String TYPE_KAFKA_TOPIC = "kafka.topic";
    public static final String TYPE_ICEBERG_TABLE = "iceberg.table";
    public static final String TYPE_JDBC_TABLE = "jdbc.table";

    private final String type;
    private final String name;

    public EntityReference(String type, String name) {
        this.type = Objects.requireNonNull(type, "type");
        this.name = Objects.requireNonNull(name, "name");
    }

    public String type() {
        return type;
    }

    public String name() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EntityReference)) return false;
        EntityReference that = (EntityReference) o;
        return type.equals(that.type) && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name);
    }

    @Override
    public String toString() {
        return type + ":" + name;
    }
}
