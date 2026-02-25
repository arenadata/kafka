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

import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelectionKey;

/**
 * A synthetic {@link SelectionKey} implementation for io_uring-backed channels.
 *
 * <p>This does not correspond to an actual NIO registration. It provides
 * the interest/ready ops tracking needed by {@code TransportLayer} and
 * {@code KafkaChannel} without requiring a real NIO Selector.
 */
public class IoUringSelectionKey extends AbstractSelectionKey {

    private final int fd;
    private volatile int interestOps;
    private volatile int readyOps;
    private final IoUringSelector ioUringSelector;

    // Channel index used to encode user_data for io_uring SQEs. Set by IoUringSelector
    // after construction so that onInterestOpsChanged can cancel/re-register polls
    // by the correct user_data key (channelIndex-based, not fd-based).
    long channelIndex;

    // Reference to the IoUringTransportLayer, used by IoUringSelector to access
    // the transport layer without needing package-private access to KafkaChannel.transportLayer
    IoUringTransportLayer transportLayer;

    // For server-side inbound connections (accepted by Acceptor): the NIO SocketChannel
    // whose fd we extracted. Kept alive so SslTransportLayer can read/write through it.
    // Null for outbound connections created via connect().
    private SocketChannel nioChannel;

    public IoUringSelectionKey(int fd, int initialOps, IoUringSelector ioUringSelector) {
        this.fd = fd;
        this.interestOps = initialOps;
        this.readyOps = 0;
        this.ioUringSelector = ioUringSelector;
    }

    /**
     * Returns the file descriptor associated with this key.
     */
    public int fd() {
        return fd;
    }

    @Override
    public SelectableChannel channel() {
        // For server-side inbound connections, returns the NIO SocketChannel
        // so that SslTransportLayer can perform reads/writes through it.
        // For outbound io_uring connections (connect path), returns null.
        return nioChannel;
    }

    /**
     * Set the NIO SocketChannel for server-side inbound connections.
     * Must be called before building a channel that uses SslTransportLayer.
     */
    void setNioChannel(SocketChannel channel) {
        this.nioChannel = channel;
    }

    @Override
    public Selector selector() {
        // Not backed by a real NIO Selector
        return null;
    }

    @Override
    public int interestOps() {
        return interestOps;
    }

    @Override
    public SelectionKey interestOps(int ops) {
        int oldOps = this.interestOps;
        this.interestOps = ops;
        if (oldOps != ops && ioUringSelector != null) {
            ioUringSelector.onInterestOpsChanged(this, oldOps, ops);
        }
        return this;
    }

    @Override
    public int readyOps() {
        return readyOps;
    }

    /**
     * Set the ready ops. Called by the IoUringSelector when CQEs are processed.
     */
    public void readyOps(int ops) {
        this.readyOps = ops;
    }

    /**
     * Add to the ready ops.
     */
    public void addReadyOps(int ops) {
        this.readyOps |= ops;
    }

    // attach() and attachment() are final in SelectionKey and inherited from there.
}
