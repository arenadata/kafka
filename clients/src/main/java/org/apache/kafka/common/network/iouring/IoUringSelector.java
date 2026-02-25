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

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelMetadataRegistry;
import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.CipherInformation;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link Selectable} implementation backed by Linux io_uring.
 *
 * <p>This replaces the NIO-based {@code Selector} for Kafka's network I/O layer.
 * Instead of using {@code java.nio.channels.Selector} (which uses epoll on Linux),
 * this class uses io_uring's submission/completion queue model for reduced syscall
 * overhead and improved throughput.
 *
 * <p>Architecture:
 * <ul>
 *   <li>One io_uring instance per IoUringSelector (per Processor thread)</li>
 *   <li>File descriptors are registered with multishot poll for event notification</li>
 *   <li>Read/write operations go through the io_uring submission queue</li>
 *   <li>Completions are drained in the {@link #poll(long)} method</li>
 * </ul>
 *
 * <p>This class is not thread safe!
 */
public class IoUringSelector implements Selectable, AutoCloseable {

    public static final long NO_IDLE_TIMEOUT_MS = -1;
    public static final int NO_FAILED_AUTHENTICATION_DELAY = 0;

    private static final int DEFAULT_RING_SIZE = 4096;

    private final Logger log;
    private final long ring;
    private final int eventFd;
    // For outbound connect() path: io_uring plaintext channel builder
    private final IoUringChannelBuilder clientChannelBuilder;
    // For inbound register(String, SocketChannel) path: SSL-capable standard channel builder
    private final ChannelBuilder serverChannelBuilder;

    private final Map<String, KafkaChannel> channels;
    private final Map<Integer, KafkaChannel> fdToChannel;
    private final Map<Integer, IoUringSelectionKey> fdToKey;
    private final Map<Long, Integer> channelIndexToFd;
    private final Set<KafkaChannel> explicitlyMutedChannels;
    private boolean outOfMemory;
    private final List<NetworkSend> completedSends;
    private final LinkedHashMap<String, NetworkReceive> completedReceives;
    private final Map<String, ChannelState> disconnected;
    private final List<String> connected;
    private final List<String> failedSends;
    private final Map<String, KafkaChannel> closingChannels;

    private final Time time;
    private final int maxReceiveSize;
    private final MemoryPool memoryPool;
    private final long lowMemThreshold;
    private final IdleExpiryManager idleExpiryManager;

    private boolean madeReadProgressLastPoll = true;

    // Pending wakeup flag
    private final AtomicBoolean wakeupTriggered = new AtomicBoolean(false);

    // Keys whose io_uring poll registration needs updating due to interestOps changes.
    // Populated during processReadyChannels; flushed once per poll() iteration to avoid
    // racing cancel+re-register cycles when doHandshake() changes interestOps multiple times.
    private final Set<IoUringSelectionKey> dirtyKeys = new HashSet<>();

    // 8-byte direct buffer used to drain the eventfd counter on each wakeup read CQE.
    // Must stay alive for the duration of the selector.
    private final java.nio.ByteBuffer eventFdReadBuf;

    // Index counter for user_data encoding
    private long nextChannelIndex = 1; // 0 reserved for wakeup eventfd

    /**
     * Create a new IoUringSelector.
     *
     * @param maxReceiveSize       max size of a single receive
     * @param connectionMaxIdleMs  idle timeout in ms
     * @param metrics              metrics registry
     * @param time                 time implementation
     * @param metricGrpPrefix      metric group prefix
     * @param metricTags           metric tags
     * @param metricsPerConnection per-connection metrics enabled
     * @param channelBuilder       channel builder
     * @param memoryPool           memory pool
     * @param logContext           log context
     * @throws IoUringException if io_uring setup fails
     */
    public IoUringSelector(int maxReceiveSize,
                           long connectionMaxIdleMs,
                           Metrics metrics,
                           Time time,
                           String metricGrpPrefix,
                           Map<String, String> metricTags,
                           boolean metricsPerConnection,
                           boolean recordTimePerConnection,
                           IoUringChannelBuilder clientChannelBuilder,
                           ChannelBuilder serverChannelBuilder,
                           MemoryPool memoryPool,
                           LogContext logContext) throws IoUringException {
        this.log = logContext.logger(IoUringSelector.class);
        this.clientChannelBuilder = clientChannelBuilder;
        this.serverChannelBuilder = serverChannelBuilder;
        this.maxReceiveSize = maxReceiveSize;
        this.time = time;
        this.channels = new HashMap<>();
        this.fdToChannel = new HashMap<>();
        this.fdToKey = new HashMap<>();
        this.channelIndexToFd = new HashMap<>();
        this.explicitlyMutedChannels = new HashSet<>();
        this.outOfMemory = false;
        this.completedSends = new ArrayList<>();
        this.completedReceives = new LinkedHashMap<>();
        this.closingChannels = new HashMap<>();
        this.connected = new ArrayList<>();
        this.disconnected = new HashMap<>();
        this.failedSends = new ArrayList<>();
        this.memoryPool = memoryPool;
        this.lowMemThreshold = (long) (0.1 * this.memoryPool.size());
        this.idleExpiryManager = connectionMaxIdleMs < 0 ? null : new IdleExpiryManager(time, connectionMaxIdleMs);

        // Initialize io_uring
        this.ring = IoUring.setup(DEFAULT_RING_SIZE);
        log.info("io_uring initialized with ring size {}", DEFAULT_RING_SIZE);

        // Create eventfd for wakeup support.
        // We use an eventfd read SQE (not multishot poll) so the kernel atomically drains
        // the counter when the CQE fires. After each wakeup CQE we re-arm another read.
        // This avoids the spin loop that a multishot poll causes when the counter isn't drained.
        this.eventFd = IoUring.createEventFd();
        this.eventFdReadBuf = io.netty.channel.uring.KafkaIoUringBridge.allocateEventFdBuffer();
        long wakeupUserData = IoUring.encodeUserData(0, IoUring.OP_WAKEUP);
        IoUring.prepEventFdRead(ring, eventFd, IoUring.bufferAddress(eventFdReadBuf), wakeupUserData);
        IoUring.submit(ring);
    }

    /**
     * Begin connecting to the given address and add the connection to this selector.
     */
    @Override
    public void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
        ensureNotRegistered(id);

        boolean ipv6 = address.getAddress().getAddress().length == 16;
        int fd = IoUring.openSocket(ipv6);
        if (fd < 0) {
            throw new IoUringException("Failed to create socket: errno=" + (-fd));
        }

        // Configure socket
        int sendBuf = sendBufferSize == Selectable.USE_DEFAULT_BUFFER_SIZE ? 0 : sendBufferSize;
        int recvBuf = receiveBufferSize == Selectable.USE_DEFAULT_BUFFER_SIZE ? 0 : receiveBufferSize;
        int rc = IoUring.configureSocket(fd, true, true, sendBuf, recvBuf);
        if (rc < 0) {
            IoUring.closeFd(fd);
            throw new IoUringException("Failed to configure socket: errno=" + (-rc));
        }

        long channelIndex = nextChannelIndex++;
        IoUringSelectionKey key = new IoUringSelectionKey(fd, SelectionKey.OP_CONNECT, this);
        key.channelIndex = channelIndex;

        try {
            ChannelMetadataRegistry metadataRegistry = new IoUringChannelMetadataRegistry();
            KafkaChannel channel = clientChannelBuilder.buildChannel(
                id, fd, key, maxReceiveSize, memoryPool, metadataRegistry, false
            );
            key.attach(channel);

            this.channels.put(id, channel);
            this.fdToChannel.put(fd, channel);
            this.fdToKey.put(fd, key);
            this.channelIndexToFd.put(channelIndex, fd);

            if (idleExpiryManager != null)
                idleExpiryManager.update(id, time.nanoseconds());

            // Initiate non-blocking connect
            boolean immediatelyConnected = IoUring.startConnect(fd, address.getAddress(), address.getPort());
            IoUringTransportLayer transport = getTransportLayer(channel);

            if (immediatelyConnected && transport != null) {
                transport.setConnected(true);
                this.connected.add(id);
                // Register for read events
                long pollUserData = IoUring.encodeUserData(channelIndex, IoUring.OP_POLL);
                IoUring.prepPollAddMultishot(ring, fd, IoUring.POLLIN | IoUring.POLLRDHUP, pollUserData);
            } else {
                // Register poll for POLLOUT to detect connection completion
                long pollUserData = IoUring.encodeUserData(channelIndex, IoUring.OP_POLL);
                IoUring.prepPollAdd(ring, fd, IoUring.POLLOUT, pollUserData);
            }
            IoUring.submit(ring);

            log.debug("Initiated io_uring connect to {} for channel {}", address, id);
        } catch (Exception e) {
            channels.remove(id);
            fdToChannel.remove(fd);
            fdToKey.remove(fd);
            channelIndexToFd.remove(channelIndex);
            IoUring.closeFd(fd);
            throw new IOException("Failed to create io_uring channel for " + id, e);
        }
    }

    /**
     * Register an existing SocketChannel with this io_uring selector.
     * Used on the server-side when a connection is accepted by the Acceptor thread.
     *
     * <p>The NIO SocketChannel is kept alive and stored in the SelectionKey so that
     * SSL-capable transport layers (e.g. SslTransportLayer) can perform reads/writes
     * through it. io_uring is used only for readiness polling (POLLIN/POLLOUT).
     */
    public void register(String id, SocketChannel socketChannel) throws IOException {
        ensureNotRegistered(id);

        // Extract fd from the NIO SocketChannel for io_uring polling
        int fd = extractFd(socketChannel);

        long channelIndex = nextChannelIndex++;
        IoUringSelectionKey key = new IoUringSelectionKey(fd, SelectionKey.OP_READ, this);
        key.channelIndex = channelIndex;
        // Store the NIO SocketChannel in the key so SslTransportLayer can use it for I/O
        key.setNioChannel(socketChannel);

        try {
            ChannelMetadataRegistry metadataRegistry = new IoUringChannelMetadataRegistry();
            // Use the SSL-capable serverChannelBuilder (e.g. SslChannelBuilder) for inbound connections.
            // It reads the SelectionKey's channel() — which now returns the NIO SocketChannel above.
            KafkaChannel channel = serverChannelBuilder.buildChannel(
                id, key, maxReceiveSize, memoryPool, metadataRegistry
            );
            key.attach(channel);

            this.channels.put(id, channel);
            this.fdToChannel.put(fd, channel);
            this.fdToKey.put(fd, key);
            this.channelIndexToFd.put(channelIndex, fd);

            if (idleExpiryManager != null)
                idleExpiryManager.update(id, time.nanoseconds());

            // Register for read events via multishot poll
            long userData = IoUring.encodeUserData(channelIndex, IoUring.OP_POLL);
            IoUring.prepPollAddMultishot(ring, fd, IoUring.POLLIN | IoUring.POLLRDHUP, userData);
            IoUring.submit(ring);

            // Default to empty client information
            if (metadataRegistry.clientInformation() == null)
                metadataRegistry.registerClientInformation(ClientInformation.EMPTY);

            log.debug("Registered io_uring channel {} with fd={}", id, fd);
        } catch (Exception e) {
            channels.remove(id);
            fdToChannel.remove(fd);
            fdToKey.remove(fd);
            channelIndexToFd.remove(channelIndex);
            throw new IOException("Failed to register io_uring channel " + id, e);
        }
    }

    /**
     * Register a raw file descriptor (from io_uring accept) with this selector.
     * Uses the plaintext io_uring channel builder (no SSL).
     */
    public void register(String id, int fd) throws IOException {
        ensureNotRegistered(id);

        long channelIndex = nextChannelIndex++;
        IoUringSelectionKey key = new IoUringSelectionKey(fd, SelectionKey.OP_READ, this);
        key.channelIndex = channelIndex;

        try {
            ChannelMetadataRegistry metadataRegistry = new IoUringChannelMetadataRegistry();
            KafkaChannel channel = clientChannelBuilder.buildChannel(
                id, fd, key, maxReceiveSize, memoryPool, metadataRegistry, true
            );
            key.attach(channel);

            this.channels.put(id, channel);
            this.fdToChannel.put(fd, channel);
            this.fdToKey.put(fd, key);
            this.channelIndexToFd.put(channelIndex, fd);

            if (idleExpiryManager != null)
                idleExpiryManager.update(id, time.nanoseconds());

            // Register for read events
            long userData = IoUring.encodeUserData(channelIndex, IoUring.OP_POLL);
            IoUring.prepPollAddMultishot(ring, fd, IoUring.POLLIN | IoUring.POLLRDHUP, userData);
            IoUring.submit(ring);

            if (metadataRegistry.clientInformation() == null)
                metadataRegistry.registerClientInformation(ClientInformation.EMPTY);

            log.debug("Registered io_uring channel {} with fd={}", id, fd);
        } catch (Exception e) {
            channels.remove(id);
            fdToChannel.remove(fd);
            fdToKey.remove(fd);
            channelIndexToFd.remove(channelIndex);
            throw new IOException("Failed to register io_uring channel " + id, e);
        }
    }

    private void ensureNotRegistered(String id) {
        if (this.channels.containsKey(id))
            throw new IllegalStateException("There is already a connection for id " + id);
        if (this.closingChannels.containsKey(id))
            throw new IllegalStateException("There is already a connection for id " + id + " that is still being closed");
    }

    @Override
    public void wakeup() {
        if (wakeupTriggered.compareAndSet(false, true)) {
            IoUring.signalEventFd(eventFd);
        }
    }

    /**
     * Do I/O using io_uring. Submits pending operations, waits for completions,
     * and processes the results.
     *
     * @param timeout The amount of time to wait, in milliseconds
     */
    @Override
    public void poll(long timeout) throws IOException {
        if (timeout < 0)
            throw new IllegalArgumentException("timeout should be >= 0");

        clear();

        if (!memoryPool.isOutOfMemory() && outOfMemory) {
            log.trace("Broker no longer low on memory - unmuting incoming sockets");
            for (KafkaChannel channel : channels.values()) {
                if (channel.isInMutableState() && !explicitlyMutedChannels.contains(channel)) {
                    channel.maybeUnmute();
                }
            }
            outOfMemory = false;
        }

        // Flush any interest-ops changes (e.g. OP_WRITE added by processNewResponses) BEFORE
        // blocking in submitAndWait. Without this, the POLLOUT poll registration never reaches
        // the kernel until after submitAndWait returns — but submitAndWait would block forever
        // waiting for a CQE that can only arrive once POLLOUT is registered. This is equivalent
        // to what NIO's Selector does: registering OP_WRITE on the selection key before select().
        flushDirtyKeys();

        // Submit any pending SQEs and wait for completions
        long timeoutNanos = timeout * 1_000_000L;
        IoUring.submitAndWait(ring, timeout > 0 ? 1 : 0, timeoutNanos);
        long endSelect = time.nanoseconds();

        // Reset wakeup flag
        wakeupTriggered.set(false);

        // Drain completion queue via callback
        IoUring.processCompletions(ring, (res, flags, udata) ->
            handleCompletion(res, flags, udata, endSelect));

        // Process channels that need read/write
        processReadyChannels(endSelect);

        // Apply any interest ops changes accumulated during processReadyChannels.
        // Done here (not inline) so that rapid changes within one handshake step
        // are coalesced into a single cancel+re-register using the final ops value.
        flushDirtyKeys();

        // Close idle connections
        maybeCloseOldestConnection(endSelect);
    }

    /**
     * Handle a single io_uring completion queue entry.
     */
    private void handleCompletion(int result, int flags, long userData, long currentTimeNanos) {
        int opType = IoUring.decodeOpType(userData);
        long channelIndex = IoUring.decodeChannelIndex(userData);

        if (opType == IoUring.OP_WAKEUP) {
            // Eventfd read completed: counter was atomically drained by the kernel.
            // Re-arm another read SQE so we can detect the next wakeup() call.
            // (Will be submitted on the next IoUring.submit/submitAndWait call.)
            long wakeupUserData = IoUring.encodeUserData(0, IoUring.OP_WAKEUP);
            IoUring.prepEventFdRead(ring, eventFd, IoUring.bufferAddress(eventFdReadBuf), wakeupUserData);
            return;
        }

        switch (opType) {
            case IoUring.OP_POLL:
                handlePollCompletion(channelIndex, result, currentTimeNanos);
                break;
            case IoUring.OP_RECV:
                handleRecvCompletion(channelIndex, result, currentTimeNanos);
                break;
            case IoUring.OP_SEND:
                handleSendCompletion(channelIndex, result, currentTimeNanos);
                break;
            default:
                log.warn("Unknown io_uring completion op type: {}", opType);
        }
    }

    private void handlePollCompletion(long channelIndex, int result, long currentTimeNanos) {
        if (result < 0) {
            log.debug("Poll completion error for channelIndex={}: errno={}", channelIndex, -result);
            return;
        }

        // Look up the fd by channelIndex
        Integer fd = channelIndexToFd.get(channelIndex);
        if (fd == null) {
            log.trace("Poll completion for unknown channelIndex={}", channelIndex);
            return;
        }

        IoUringSelectionKey key = fdToKey.get(fd);
        KafkaChannel channel = fdToChannel.get(fd);
        if (key == null || channel == null) return;

        // Check if this is a connecting socket (POLLOUT for connect completion)
        if ((key.interestOps() & SelectionKey.OP_CONNECT) != 0 && (result & IoUring.POLLOUT) != 0) {
            IoUringTransportLayer transport = getTransportLayer(channel);
            if (transport != null && !transport.isConnected()) {
                try {
                    boolean finished = IoUring.finishConnect(fd);
                    if (finished) {
                        transport.setConnected(true);
                        if (channel.finishConnect()) {
                            this.connected.add(channel.id());
                            log.debug("io_uring connect completed for channel {}", channel.id());
                        }
                        // Switch from oneshot connect poll to multishot read poll
                        long pollUserData = IoUring.encodeUserData(channelIndex, IoUring.OP_POLL);
                        IoUring.prepPollAddMultishot(ring, fd, IoUring.POLLIN | IoUring.POLLRDHUP, pollUserData);
                        IoUring.submit(ring);
                    }
                } catch (IOException e) {
                    log.debug("Connect completion failed for channel {}", channel.id(), e);
                    channel.state(ChannelState.NOT_CONNECTED);
                    close(channel, true);
                }
                return;
            }
        }

        // Normal poll events: set ready ops on the key
        if ((result & IoUring.POLLIN) != 0) {
            key.addReadyOps(SelectionKey.OP_READ);
        }
        if ((result & IoUring.POLLOUT) != 0) {
            key.addReadyOps(SelectionKey.OP_WRITE);
        }
        if ((result & (IoUring.POLLERR | IoUring.POLLHUP | IoUring.POLLRDHUP)) != 0) {
            key.addReadyOps(SelectionKey.OP_READ); // signal EOF
        }
    }

    private void handleRecvCompletion(long channelIndex, int result, long currentTimeNanos) {
        // Currently using synchronous recv in performRead, so this is a no-op placeholder
        // for future async recv support
    }

    private void handleSendCompletion(long channelIndex, int result, long currentTimeNanos) {
        // Currently using synchronous send in performWrite, so this is a no-op placeholder
        // for future async send support
    }

    /**
     * Process channels that have ready events.
     */
    private void processReadyChannels(long currentTimeNanos) {
        for (Map.Entry<String, KafkaChannel> entry : new ArrayList<>(channels.entrySet())) {
            KafkaChannel channel = entry.getValue();
            String nodeId = entry.getKey();
            IoUringSelectionKey key = fdToKey.get(getFd(channel));

            if (key == null) continue;

            try {
                if (idleExpiryManager != null)
                    idleExpiryManager.update(nodeId, currentTimeNanos);

                // Handle pending connect
                if (!channel.isConnected()) {
                    if (channel.finishConnect()) {
                        this.connected.add(nodeId);
                    } else {
                        continue;
                    }
                }

                // Prepare channel (handshake/authentication)
                if (channel.isConnected() && !channel.ready()) {
                    log.info("prepare() BEFORE channel={} readyOps=0x{} interestOps=0x{}",
                        nodeId, Integer.toHexString(key.readyOps()), Integer.toHexString(key.interestOps()));
                    channel.prepare();
                    log.info("prepare() AFTER  channel={} readyOps=0x{} interestOps=0x{} ready={}",
                        nodeId, Integer.toHexString(key.readyOps()), Integer.toHexString(key.interestOps()), channel.ready());

                    // io_uring multishot POLL_ADD is edge-triggered: POLLOUT fires only when
                    // the socket transitions from not-writable to writable, not on every write
                    // opportunity. NIO selectors are level-triggered and call prepare() again
                    // automatically. For io_uring we must simulate that here: if prepare() added
                    // OP_WRITE (needs to flush handshake data), inject a synthetic POLLOUT and
                    // retry immediately. Repeat until the handshake no longer needs OP_WRITE or
                    // completes. Capped at 32 to prevent infinite loops if the write truly blocks.
                    for (int i = 0; i < 32 && !channel.ready()
                            && (key.interestOps() & SelectionKey.OP_WRITE) != 0; i++) {
                        key.readyOps(SelectionKey.OP_WRITE); // synthetic POLLOUT, no spurious read
                        channel.prepare();
                    }
                }
                if (channel.ready() && channel.state() == ChannelState.NOT_CONNECTED)
                    channel.state(ChannelState.READY);

                Optional<NetworkReceive> responseReceivedDuringReauthentication =
                    channel.pollResponseReceivedDuringReauthentication();
                responseReceivedDuringReauthentication.ifPresent(receive -> {
                    long currentTimeMs = time.milliseconds();
                    addToCompletedReceives(channel, receive, currentTimeMs);
                });

                // Attempt read if channel is ready and readable, or has SSL-buffered bytes
                if (channel.ready()
                        && ((key.readyOps() & SelectionKey.OP_READ) != 0 || channel.hasBytesBuffered())
                        && !hasCompletedReceive(channel)
                        && !explicitlyMutedChannels.contains(channel)) {
                    attemptRead(channel, nodeId);
                }

                // Attempt write if channel has data to send
                if (channel.hasSend() && channel.ready()) {
                    attemptWrite(channel, nodeId);
                }

            } catch (AuthenticationException e) {
                log.info("Failed authentication with {} ({})", nodeId, e.getMessage());
                close(channel, true);
            } catch (IOException e) {
                close(channel, true);
            } catch (Exception e) {
                log.warn("Unexpected error from {}; closing connection", nodeId, e);
                close(channel, true);
            } finally {
                // Clear ready ops for next poll
                if (key != null) {
                    key.readyOps(0);
                }
            }
        }
    }

    private void attemptRead(KafkaChannel channel, String nodeId) throws IOException {
        long bytesReceived = channel.read();
        if (bytesReceived != 0) {
            long currentTimeMs = time.milliseconds();
            madeReadProgressLastPoll = true;

            NetworkReceive receive = channel.maybeCompleteReceive();
            if (receive != null) {
                addToCompletedReceives(channel, receive, currentTimeMs);
            }
        }
        if (channel.isMuted()) {
            outOfMemory = true;
        } else {
            madeReadProgressLastPoll = true;
        }
    }

    private void attemptWrite(KafkaChannel channel, String nodeId) throws IOException {
        long bytesSent = channel.write();
        NetworkSend send = channel.maybeCompleteSend();
        if (bytesSent > 0 || send != null) {
            if (send != null) {
                this.completedSends.add(send);
            }
        }
    }

    /**
     * Perform a synchronous read from an io_uring-managed fd.
     * Called by IoUringTransportLayer.read().
     *
     * <p>For the initial implementation, we use io_uring recv SQE with immediate
     * submit-and-wait to maintain compatibility with KafkaChannel's synchronous
     * read loop. Async recv support can be added later for even better performance.
     */
    int performRead(IoUringTransportLayer transport, ByteBuffer dst) throws IOException {
        if (!dst.isDirect()) {
            ByteBuffer directBuf = ByteBuffer.allocateDirect(dst.remaining());
            try {
                long addr = IoUring.bufferAddress(directBuf);
                int n = nativeRecv(transport.fd(), addr, directBuf.capacity());
                if (n > 0) {
                    directBuf.limit(n);
                    dst.put(directBuf);
                }
                return n;
            } finally {
                // Direct buffer will be cleaned up by GC
            }
        } else {
            long addr = IoUring.bufferAddress(dst) + dst.position();
            int n = nativeRecv(transport.fd(), addr, dst.remaining());
            if (n > 0) {
                dst.position(dst.position() + n);
            }
            return n;
        }
    }

    /**
     * Perform a synchronous write to an io_uring-managed fd.
     * Called by IoUringTransportLayer.write().
     */
    int performWrite(IoUringTransportLayer transport, ByteBuffer src) throws IOException {
        if (!src.isDirect()) {
            ByteBuffer directBuf = ByteBuffer.allocateDirect(src.remaining());
            try {
                directBuf.put(src.duplicate());
                directBuf.flip();
                long addr = IoUring.bufferAddress(directBuf);
                int n = nativeSend(transport.fd(), addr, directBuf.remaining());
                if (n > 0) {
                    src.position(src.position() + n);
                }
                return n;
            } finally {
                // Direct buffer will be cleaned up by GC
            }
        } else {
            long addr = IoUring.bufferAddress(src) + src.position();
            int n = nativeSend(transport.fd(), addr, src.remaining());
            if (n > 0) {
                src.position(src.position() + n);
            }
            return n;
        }
    }

    /**
     * Synchronous recv via io_uring: submit a recv SQE and immediately wait for its CQE.
     */
    private int nativeRecv(int fd, long bufAddr, int len) throws IOException {
        long userData = IoUring.encodeUserData(fd, IoUring.OP_RECV);
        int rc = IoUring.prepRecv(ring, fd, bufAddr, len, userData);
        if (rc < 0) throw new IoUringException("prepRecv failed: rc=" + rc);

        IoUring.submitAndWait(ring, 1, 0);

        int[] resultHolder = {Integer.MIN_VALUE};
        IoUring.processCompletions(ring, (res, flags, udata) -> {
            if (udata == userData) {
                resultHolder[0] = res;
            } else {
                // Forward other completions (e.g., poll events) to the main handler
                handleCompletion(res, flags, udata, time.nanoseconds());
            }
        });

        int result = resultHolder[0];
        if (result == Integer.MIN_VALUE) {
            return 0; // No completion for our recv
        }
        if (result < 0) {
            int errno = -result;
            if (errno == 11 || errno == 35) { // EAGAIN / EWOULDBLOCK
                return 0;
            }
            throw new IoUringException("recv failed: errno=" + errno);
        }
        if (result == 0) {
            return -1; // EOF
        }
        return result;
    }

    /**
     * Synchronous send via io_uring: submit a send SQE and immediately wait for its CQE.
     */
    private int nativeSend(int fd, long bufAddr, int len) throws IOException {
        long userData = IoUring.encodeUserData(fd, IoUring.OP_SEND);
        int rc = IoUring.prepSend(ring, fd, bufAddr, len, userData);
        if (rc < 0) throw new IoUringException("prepSend failed: rc=" + rc);

        IoUring.submitAndWait(ring, 1, 0);

        int[] resultHolder = {Integer.MIN_VALUE};
        IoUring.processCompletions(ring, (res, flags, udata) -> {
            if (udata == userData) {
                resultHolder[0] = res;
            } else {
                // Forward other completions to the main handler
                handleCompletion(res, flags, udata, time.nanoseconds());
            }
        });

        int result = resultHolder[0];
        if (result == Integer.MIN_VALUE) {
            return 0;
        }
        if (result < 0) {
            int errno = -result;
            if (errno == 11 || errno == 35) { // EAGAIN / EWOULDBLOCK
                return 0;
            }
            throw new IoUringException("send failed: errno=" + errno);
        }
        return result;
    }

    /**
     * Called by IoUringSelectionKey when interest ops change.
     * Updates the io_uring poll registration accordingly.
     */
    void onInterestOpsChanged(IoUringSelectionKey key, int oldOps, int newOps) {
        // Defer the actual cancel+re-register to flushDirtyKeys(), which is called once
        // per poll() iteration after processReadyChannels(). This avoids the race where
        // doHandshake() calls interestOps() multiple times rapidly (e.g. add OP_WRITE then
        // clear OP_WRITE in the same handshake step), causing successive cancel+re-register
        // cycles to race against each other and permanently lose the poll registration.
        dirtyKeys.add(key);
    }

    /**
     * Apply deferred interest ops updates: for each dirty key, cancel the existing
     * io_uring poll and re-register with the current (final) interest ops mask.
     * Called once per poll() iteration after all channel processing is done.
     */
    private void flushDirtyKeys() {
        if (dirtyKeys.isEmpty()) return;
        for (IoUringSelectionKey key : dirtyKeys) {
            // Skip keys whose channels have already been closed
            if (!fdToKey.containsKey(key.fd())) continue;

            int newOps = key.interestOps();
            int fd = key.fd();
            int pollMask = 0;
            if ((newOps & SelectionKey.OP_READ) != 0) pollMask |= IoUring.POLLIN;
            if ((newOps & SelectionKey.OP_WRITE) != 0) pollMask |= IoUring.POLLOUT;
            if ((newOps & SelectionKey.OP_CONNECT) != 0) pollMask |= IoUring.POLLOUT;

            long userData = IoUring.encodeUserData(key.channelIndex, IoUring.OP_POLL);
            IoUring.prepCancel(ring, userData);
            if (pollMask != 0) {
                IoUring.prepPollAddMultishot(ring, fd, pollMask | IoUring.POLLRDHUP, userData);
            }
        }
        IoUring.submit(ring);
        dirtyKeys.clear();
    }

    @Override
    public void send(NetworkSend send) {
        String connectionId = send.destinationId();
        KafkaChannel channel = openOrClosingChannelOrFail(connectionId);
        if (closingChannels.containsKey(connectionId)) {
            this.failedSends.add(connectionId);
        } else {
            try {
                channel.setSend(send);
            } catch (Exception e) {
                channel.state(ChannelState.FAILED_SEND);
                this.failedSends.add(connectionId);
                close(channel, false);
                if (!(e instanceof java.nio.channels.CancelledKeyException)) {
                    log.error("Unexpected exception during send, closing connection {} and rethrowing exception.",
                        connectionId, e);
                    throw e;
                }
            }
        }
    }

    @Override
    public List<NetworkSend> completedSends() {
        return this.completedSends;
    }

    @Override
    public Collection<NetworkReceive> completedReceives() {
        return this.completedReceives.values();
    }

    @Override
    public Map<String, ChannelState> disconnected() {
        return this.disconnected;
    }

    @Override
    public List<String> connected() {
        return this.connected;
    }

    @Override
    public void mute(String id) {
        KafkaChannel channel = openOrClosingChannelOrFail(id);
        channel.mute();
        explicitlyMutedChannels.add(channel);
    }

    @Override
    public void unmute(String id) {
        KafkaChannel channel = openOrClosingChannelOrFail(id);
        if (channel.maybeUnmute()) {
            explicitlyMutedChannels.remove(channel);
        }
    }

    @Override
    public void muteAll() {
        for (KafkaChannel channel : this.channels.values()) {
            channel.mute();
            explicitlyMutedChannels.add(channel);
        }
    }

    @Override
    public void unmuteAll() {
        for (KafkaChannel channel : this.channels.values()) {
            if (channel.maybeUnmute()) {
                explicitlyMutedChannels.remove(channel);
            }
        }
    }

    @Override
    public boolean isChannelReady(String id) {
        KafkaChannel channel = this.channels.get(id);
        return channel != null && channel.ready();
    }

    public void clearCompletedReceives() {
        this.completedReceives.clear();
    }

    public void clearCompletedSends() {
        this.completedSends.clear();
    }

    public KafkaChannel channel(String id) {
        return this.channels.get(id);
    }

    @Override
    public void close(String id) {
        KafkaChannel channel = this.channels.get(id);
        if (channel != null) {
            channel.state(ChannelState.LOCAL_CLOSE);
            close(channel, false);
        } else {
            KafkaChannel closingChannel = this.closingChannels.remove(id);
            if (closingChannel != null) {
                doClose(closingChannel);
            }
        }
    }

    private void close(KafkaChannel channel, boolean notifyDisconnect) {
        String id = channel.id();
        channels.remove(id);
        int fd = getFd(channel);
        fdToChannel.remove(fd);
        fdToKey.remove(fd);
        // Clean up channelIndex mapping
        channelIndexToFd.values().remove(fd);
        explicitlyMutedChannels.remove(channel);

        if (notifyDisconnect) {
            this.disconnected.put(id, channel.state());
        }

        doClose(channel);
    }

    private void doClose(KafkaChannel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            log.debug("Exception closing channel {}", channel.id(), e);
        }
    }

    @Override
    public void close() {
        AtomicReference<Throwable> firstException = new AtomicReference<>();

        // Close all channels
        List<String> ids = new ArrayList<>(channels.keySet());
        for (String id : ids) {
            try {
                close(id);
            } catch (Exception e) {
                if (firstException.get() == null) firstException.set(e);
            }
        }

        // Close io_uring
        try {
            IoUring.closeFd(eventFd);
        } catch (Exception e) {
            if (firstException.get() == null) firstException.set(e);
        }

        try {
            IoUring.close(ring);
        } catch (Exception e) {
            if (firstException.get() == null) firstException.set(e);
        }

        Utils.closeQuietly(clientChannelBuilder, "clientChannelBuilder", firstException);
        Utils.closeQuietly(serverChannelBuilder, "serverChannelBuilder", firstException);

        Throwable exception = firstException.get();
        if (exception instanceof RuntimeException) {
            throw (RuntimeException) exception;
        }
    }

    private void clear() {
        this.completedSends.clear();
        this.completedReceives.clear();
        this.connected.clear();
        this.disconnected.clear();

        // Process closing channels
        for (Iterator<Map.Entry<String, KafkaChannel>> it = closingChannels.entrySet().iterator(); it.hasNext(); ) {
            KafkaChannel channel = it.next().getValue();
            boolean sendFailed = failedSends.remove(channel.id());
            if (sendFailed) {
                doClose(channel);
                it.remove();
            }
        }

        for (String channel : this.failedSends)
            this.disconnected.put(channel, ChannelState.FAILED_SEND);
        this.failedSends.clear();
        this.madeReadProgressLastPoll = false;
    }

    private void addToCompletedReceives(KafkaChannel channel, NetworkReceive receive, long currentTimeMs) {
        if (!this.completedReceives.containsKey(channel.id())) {
            this.completedReceives.put(channel.id(), receive);
        }
    }

    private boolean hasCompletedReceive(KafkaChannel channel) {
        return this.completedReceives.containsKey(channel.id());
    }

    private void maybeCloseOldestConnection(long currentTimeNanos) {
        if (idleExpiryManager == null)
            return;

        Map.Entry<String, Long> expiredConnection = idleExpiryManager.pollExpiredConnection(currentTimeNanos);
        if (expiredConnection != null) {
            String connectionId = expiredConnection.getKey();
            KafkaChannel channel = this.channels.get(connectionId);
            if (channel != null) {
                if (log.isTraceEnabled())
                    log.trace("About to close idle connection from {} due to being idle for {} millis",
                        connectionId, (currentTimeNanos - expiredConnection.getValue()) / 1000 / 1000);
                channel.state(ChannelState.EXPIRED);
                close(channel, true);
            }
        }
    }

    private KafkaChannel openOrClosingChannelOrFail(String id) {
        KafkaChannel channel = this.channels.get(id);
        if (channel == null)
            channel = this.closingChannels.get(id);
        if (channel == null)
            throw new IllegalStateException("Attempt to retrieve channel for which there is no connection. Connection id " + id);
        return channel;
    }

    static int getFd(KafkaChannel channel) {
        SelectionKey key = channel.selectionKey();
        if (key instanceof IoUringSelectionKey) {
            return ((IoUringSelectionKey) key).fd();
        }
        throw new IllegalStateException("Channel does not have an IoUringSelectionKey");
    }

    private static IoUringTransportLayer getTransportLayer(KafkaChannel channel) {
        SelectionKey key = channel.selectionKey();
        if (key instanceof IoUringSelectionKey) {
            return ((IoUringSelectionKey) key).transportLayer;
        }
        return null;
    }

    /**
     * Extract the native file descriptor from a SocketChannel.
     * Uses reflection to access the fd field since there's no public API.
     */
    private static int extractFd(SocketChannel socketChannel) throws IOException {
        // Approach 1: SocketChannelImpl.getFDVal() (requires --add-opens java.base/sun.nio.ch=ALL-UNNAMED)
        try {
            java.lang.reflect.Method method = socketChannel.getClass().getMethod("getFDVal");
            method.setAccessible(true);
            return (int) method.invoke(socketChannel);
        } catch (Exception ignored) {
            // Fall through to approach 2
        }

        // Approach 2: SocketChannelImpl.fd (FileDescriptor) -> FileDescriptor.fd (int)
        // Requires --add-opens java.base/sun.nio.ch=ALL-UNNAMED and --add-opens java.base/java.io=ALL-UNNAMED
        try {
            java.lang.reflect.Field fdField = socketChannel.getClass().getDeclaredField("fd");
            fdField.setAccessible(true);
            java.io.FileDescriptor fd = (java.io.FileDescriptor) fdField.get(socketChannel);
            java.lang.reflect.Field fdIntField = java.io.FileDescriptor.class.getDeclaredField("fd");
            fdIntField.setAccessible(true);
            return fdIntField.getInt(fd);
        } catch (Exception e) {
            throw new IOException(
                "Unable to extract fd from SocketChannel. Make sure JVM is started with: " +
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED",
                e);
        }
    }

    /**
     * Generate a connection id for an io_uring-managed fd.
     *
     * @param fd              the file descriptor
     * @param connectionIndex unique index
     * @return connection id string
     */
    public static String generateConnectionId(int fd, int connectionIndex) {
        try {
            InetSocketAddress localAddr = IoUring.getLocalAddress(fd);
            InetSocketAddress remoteAddr = IoUring.getRemoteAddress(fd);
            if (localAddr != null && remoteAddr != null) {
                return localAddr.getAddress().getHostAddress() + ":" + localAddr.getPort() + "-"
                    + remoteAddr.getAddress().getHostAddress() + ":" + remoteAddr.getPort() + "-"
                    + connectionIndex;
            }
        } catch (Exception e) {
            // fall through
        }
        return "fd" + fd + "-" + connectionIndex;
    }

    /**
     * Simple metadata registry for io_uring channels.
     */
    private static class IoUringChannelMetadataRegistry implements ChannelMetadataRegistry {
        private CipherInformation cipherInformation;
        private ClientInformation clientInformation;

        @Override
        public void registerCipherInformation(CipherInformation cipherInformation) {
            this.cipherInformation = cipherInformation;
        }

        @Override
        public CipherInformation cipherInformation() {
            return cipherInformation;
        }

        @Override
        public void registerClientInformation(ClientInformation clientInformation) {
            this.clientInformation = clientInformation;
        }

        @Override
        public ClientInformation clientInformation() {
            return clientInformation;
        }

        @Override
        public void close() {
            cipherInformation = null;
            clientInformation = null;
        }
    }

    /**
     * Manages idle connection expiry.
     */
    private static class IdleExpiryManager {
        private final LinkedHashMap<String, Long> lruConnections;
        private final long connectionsMaxIdleNanos;
        private final Time time;

        IdleExpiryManager(Time time, long connectionsMaxIdleMs) {
            this.time = time;
            this.connectionsMaxIdleNanos = connectionsMaxIdleMs * 1000 * 1000;
            this.lruConnections = new LinkedHashMap<>(16, .75F, true);
        }

        void update(String connectionId, long currentTimeNanos) {
            lruConnections.put(connectionId, currentTimeNanos);
        }

        Map.Entry<String, Long> pollExpiredConnection(long currentTimeNanos) {
            Iterator<Map.Entry<String, Long>> it = lruConnections.entrySet().iterator();
            if (!it.hasNext())
                return null;
            Map.Entry<String, Long> oldestConnection = it.next();
            if (currentTimeNanos - oldestConnection.getValue() > connectionsMaxIdleNanos) {
                it.remove();
                return oldestConnection;
            }
            return null;
        }
    }
}
