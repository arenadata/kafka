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

import java.util.Objects;

/**
 * The lag of a single group as computed by one sampling cycle.
 *
 * @param shard             The __consumer_offsets partition hosting the group. May be null.
 * @param type              The group type.
 * @param sumLag            The sum of the lag of all sampled partitions.
 * @param maxLag            The maximum lag across all sampled partitions.
 * @param sampledPartitions The number of partitions for which the log end offset
 *                          could be resolved in this cycle.
 * @param totalPartitions   The number of partitions with a committed offset.
 */
public record GroupLagValue(
    TopicPartition shard,
    Group.GroupType type,
    long sumLag,
    long maxLag,
    int sampledPartitions,
    int totalPartitions
) {
    public GroupLagValue {
        Objects.requireNonNull(type, "type");
    }
}
