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
package org.apache.kafka.coordinator.group.metrics;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.coordinator.group.Group;

import java.util.Map;
import java.util.Objects;

/**
 * The inputs required to compute the lag of the groups hosted by a single
 * group coordinator shard, as read from the coordinator state at a given
 * committed offset.
 *
 * @param shard  The __consumer_offsets partition the inputs were read from. May be null
 *               when the shard is not associated with a metrics shard.
 * @param groups The lag inputs keyed by group id.
 */
public record GroupLagInputs(
    TopicPartition shard,
    Map<String, GroupLagInput> groups
) {
    public static final GroupLagInputs EMPTY = new GroupLagInputs(null, Map.of());

    public GroupLagInputs {
        Objects.requireNonNull(groups, "groups");
    }

    /**
     * The lag inputs of a single group.
     *
     * @param type             The group type. Share groups are never included.
     * @param committedOffsets The committed offset of each topic partition.
     */
    public record GroupLagInput(
        Group.GroupType type,
        Map<TopicPartition, Long> committedOffsets
    ) {
        public GroupLagInput {
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(committedOffsets, "committedOffsets");
        }
    }
}
