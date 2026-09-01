# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM eclipse-temurin:21-jre-jammy

USER root

COPY build/distributions/kafka-connect_*.tgz /tmp/

RUN set -eux; \
    archive="$(find /tmp -maxdepth 1 -name 'kafka-connect_*.tgz' -print -quit)"; \
    test -n "$archive"; \
    mkdir -p /opt/kafka /opt/kafka/plugins; \
    tar -xzf "$archive" -C /opt/kafka; \
    chmod 755 /opt/kafka/bin/*.sh; \
    rm "$archive"; \
    useradd --create-home --uid 10001 --shell /bin/bash appuser; \
    chown -R appuser:appuser /opt/kafka

COPY docker/connect/run /usr/local/bin/run-kafka-connect

RUN chmod 755 /usr/local/bin/run-kafka-connect

ENV KAFKA_HOME=/opt/kafka \
    KAFKA_HEAP_OPTS="-Xms256M -Xmx2G" \
    CONNECT_PLUGIN_PATH="/opt/kafka/plugins"

EXPOSE 8083

USER appuser

ENTRYPOINT ["/usr/local/bin/run-kafka-connect"]
