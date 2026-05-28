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
 * Signal that data has flowed from {@link #source()} to {@link #target()} as
 * part of a named pipeline (typically the connector instance name)
 * <p>
 * Reported by sink connectors after a successful commit/write and by source
 * connectors after records have been produced to the target topic
 *
 * @since 4.2
 */
public final class LineageEdge extends MetadataEvent {

    private final EntityReference source;
    private final EntityReference target;
    private final String pipelineName;

    public LineageEdge(EntityReference source, EntityReference target, String pipelineName) {
        this(System.currentTimeMillis(), source, target, pipelineName);
    }

    public LineageEdge(long timestamp,
                       EntityReference source,
                       EntityReference target,
                       String pipelineName) {
        super(timestamp);
        this.source = Objects.requireNonNull(source, "source");
        this.target = Objects.requireNonNull(target, "target");
        this.pipelineName = Objects.requireNonNull(pipelineName, "pipelineName");
    }

    public EntityReference source() {
        return source;
    }

    public EntityReference target() {
        return target;
    }

    public String pipelineName() {
        return pipelineName;
    }
}
