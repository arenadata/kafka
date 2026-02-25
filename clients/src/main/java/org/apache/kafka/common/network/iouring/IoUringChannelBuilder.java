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
package org.apache.kafka.common.network.iouring;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.network.Authenticator;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.ChannelMetadataRegistry;
import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.security.auth.PlaintextAuthenticationContext;
import org.apache.kafka.common.utils.Utils;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * {@link ChannelBuilder} for io_uring-backed plaintext channels.
 *
 * <p>This is analogous to {@code PlaintextChannelBuilder} but produces
 * {@link IoUringTransportLayer} instances instead of NIO-based transport layers.
 * It is used by {@link IoUringSelector} when building channels for accepted
 * or connected sockets.
 */
public class IoUringChannelBuilder implements ChannelBuilder {

    private final ListenerName listenerName;
    private IoUringSelector selector;
    private Map<String, ?> configs;

    /**
     * @param listenerName non-null when instantiated on the broker, null for client mode
     * @param selector     the IoUringSelector that manages the io_uring instance
     */
    public IoUringChannelBuilder(ListenerName listenerName, IoUringSelector selector) {
        this.listenerName = listenerName;
        this.selector = selector;
    }

    /**
     * Set the IoUringSelector after construction.
     * Required because the selector is created after the builder; call this before any channel is built.
     */
    public void setSelector(IoUringSelector selector) {
        this.selector = selector;
    }

    @Override
    public void configure(Map<String, ?> configs) throws KafkaException {
        this.configs = configs;
    }

    @Override
    public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize,
                                     MemoryPool memoryPool, ChannelMetadataRegistry metadataRegistry) throws KafkaException {
        try {
            IoUringSelectionKey ioKey = (IoUringSelectionKey) key;
            IoUringTransportLayer transportLayer = new IoUringTransportLayer(
                ioKey.fd(), ioKey, selector, true
            );
            ioKey.transportLayer = transportLayer;
            final IoUringTransportLayer finalTransportLayer = transportLayer;
            Supplier<Authenticator> authenticatorCreator = () ->
                new IoUringAuthenticator(configs, finalTransportLayer, listenerName);
            return new KafkaChannel(id, transportLayer, authenticatorCreator, maxReceiveSize,
                memoryPool != null ? memoryPool : MemoryPool.NONE, metadataRegistry);
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Build a KafkaChannel for an outbound connection (connect path).
     *
     * @param id             channel id
     * @param fd             file descriptor of the socket
     * @param key            the IoUringSelectionKey for this fd
     * @param maxReceiveSize max receive buffer size
     * @param memoryPool     memory pool
     * @param metadataRegistry metadata registry
     * @param connected      true if already connected (immediate connect)
     * @return the KafkaChannel
     */
    public KafkaChannel buildChannel(String id, int fd, IoUringSelectionKey key,
                                     int maxReceiveSize, MemoryPool memoryPool,
                                     ChannelMetadataRegistry metadataRegistry, boolean connected) {
        try {
            IoUringTransportLayer transportLayer = new IoUringTransportLayer(
                fd, key, selector, connected
            );
            key.transportLayer = transportLayer;
            final IoUringTransportLayer finalTransportLayer = transportLayer;
            Supplier<Authenticator> authenticatorCreator = () ->
                new IoUringAuthenticator(configs, finalTransportLayer, listenerName);
            return new KafkaChannel(id, transportLayer, authenticatorCreator, maxReceiveSize,
                memoryPool != null ? memoryPool : MemoryPool.NONE, metadataRegistry);
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public void close() {}

    /**
     * Authenticator for io_uring plaintext channels.
     */
    private static class IoUringAuthenticator implements Authenticator {
        private final IoUringTransportLayer transportLayer;
        private final KafkaPrincipalBuilder principalBuilder;
        private final ListenerName listenerName;

        private IoUringAuthenticator(Map<String, ?> configs, IoUringTransportLayer transportLayer,
                                     ListenerName listenerName) {
            this.transportLayer = transportLayer;
            this.principalBuilder = ChannelBuilders.createPrincipalBuilder(configs, null, null);
            this.listenerName = listenerName;
        }

        @Override
        public void authenticate() {}

        @Override
        public KafkaPrincipal principal() {
            if (listenerName == null)
                throw new IllegalStateException("Unexpected call to principal() when listenerName is null");
            try {
                InetSocketAddress remoteAddr = IoUring.getRemoteAddress(transportLayer.fd());
                if (remoteAddr == null) {
                    throw new KafkaException("Unable to determine remote address for fd=" + transportLayer.fd());
                }
                return principalBuilder.build(
                    new PlaintextAuthenticationContext(remoteAddr.getAddress(), listenerName.value()));
            } catch (KafkaException e) {
                throw e;
            } catch (Exception e) {
                throw new KafkaException("Failed to determine client address", e);
            }
        }

        @Override
        public Optional<KafkaPrincipalSerde> principalSerde() {
            return principalBuilder instanceof KafkaPrincipalSerde
                ? Optional.of((KafkaPrincipalSerde) principalBuilder)
                : Optional.empty();
        }

        @Override
        public boolean complete() {
            return true;
        }

        @Override
        public void close() {
            if (principalBuilder instanceof Closeable)
                Utils.closeQuietly((Closeable) principalBuilder, "principal builder");
        }
    }
}
