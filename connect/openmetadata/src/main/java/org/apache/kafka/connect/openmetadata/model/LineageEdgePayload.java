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
package org.apache.kafka.connect.openmetadata.model;

import org.apache.kafka.connect.metadata.ColumnLineage;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class LineageEdgePayload {

    public Edge edge;

    public LineageEdgePayload() {
    }

    public LineageEdgePayload(EntityRef from, EntityRef to, EntityRef pipeline) {
        this(from, to, pipeline, List.of());
    }

    public LineageEdgePayload(EntityRef from,
                              EntityRef to,
                              EntityRef pipeline,
                              List<ColumnLineage> columnsLineage) {
        this.edge = new Edge();
        this.edge.fromEntity = from;
        this.edge.toEntity = to;
        if (pipeline != null || !columnsLineage.isEmpty()) {
            this.edge.lineageDetails = new LineageDetails();
            this.edge.lineageDetails.pipeline = pipeline;
            if (!columnsLineage.isEmpty()) {
                this.edge.lineageDetails.columnsLineage = columnsLineage.stream()
                        .map(ColumnLineagePayload::new)
                        .collect(Collectors.toList());
            }
        }
    }

    public static final class Edge {
        public EntityRef fromEntity;
        public EntityRef toEntity;
        public LineageDetails lineageDetails;
    }

    public static final class LineageDetails {
        public EntityRef pipeline;
        public List<ColumnLineagePayload> columnsLineage;
    }
}
