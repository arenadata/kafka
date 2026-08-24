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

import java.util.List;
import java.util.Objects;

/**
 * Maps one or more source columns to a target column in a lineage edge.
 * Column names are fully-qualified names understood by the metadata system.
 *
 * @since 4.2
 */
public final class ColumnLineage {

    private final List<String> fromColumns;
    private final String toColumn;

    public ColumnLineage(List<String> fromColumns, String toColumn) {
        this.fromColumns = List.copyOf(Objects.requireNonNull(fromColumns, "fromColumns"));
        this.toColumn = Objects.requireNonNull(toColumn, "toColumn");
    }

    public List<String> fromColumns() {
        return fromColumns;
    }

    public String toColumn() {
        return toColumn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnLineage)) return false;
        ColumnLineage that = (ColumnLineage) o;
        return fromColumns.equals(that.fromColumns) && toColumn.equals(that.toColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fromColumns, toColumn);
    }

    @Override
    public String toString() {
        return fromColumns + " -> " + toColumn;
    }
}
