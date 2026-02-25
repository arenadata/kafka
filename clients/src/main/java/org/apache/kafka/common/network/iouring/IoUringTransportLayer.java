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

import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.Principal;

/**
 * Transport layer backed by Linux io_uring for plaintext (non-SSL) communication.
 *
 * <p>Instead of using NIO's {@link SocketChannel}, this transport operates on raw
 * file descriptors via io_uring submission/completion queues. Read and write operations
 * go through direct {@link ByteBuffer}s whose native addresses are passed to io_uring SQEs.
 *
 * <p>The actual I/O submission is coordinated by {@link IoUringSelector}, which batches
 * operations into the submission queue and processes completions.
 */
public class IoUringTransportLayer implements TransportLayer {

    private final int fd;
    private final IoUringSelectionKey key;
    private final IoUringSelector selector;
    private final Principal principal = KafkaPrincipal.ANONYMOUS;

    private volatile boolean connected;
    private volatile boolean open = true;

    // Pending I/O state managed by IoUringSelector
    private ByteBuffer pendingReadBuffer;
    private int pendingReadResult = 0;
    private boolean readPending;

    private ByteBuffer pendingSendBuffer;
    private int pendingSendResult = 0;
    private boolean sendPending;

    private boolean connectPending;
    private int connectResult = 0;

    public IoUringTransportLayer(int fd, IoUringSelectionKey key, IoUringSelector selector, boolean connected) {
        this.fd = fd;
        this.key = key;
        this.selector = selector;
        this.connected = connected;
    }

    /**
     * Returns the underlying file descriptor.
     */
    public int fd() {
        return fd;
    }

    @Override
    public boolean ready() {
        return true;
    }

    @Override
    public boolean finishConnect() throws IOException {
        if (connected) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
            return true;
        }

        if (connectPending) {
            if (connectResult < 0) {
                throw new IoUringException("connect failed with errno: " + (-connectResult));
            }
            connected = true;
            connectPending = false;
            key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
            return true;
        }

        return false;
    }

    @Override
    public void disconnect() {
        key.cancel();
    }

    @Override
    public SocketChannel socketChannel() {
        // io_uring does not use NIO SocketChannel; callers should use fd() instead.
        // Return null — callers in Kafka that need socketChannel() for logging/metadata
        // should be guarded for null when io_uring is in use.
        return null;
    }

    @Override
    public SelectionKey selectionKey() {
        return key;
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() throws IOException {
        if (open) {
            open = false;
            IoUring.closeFd(fd);
        }
    }

    @Override
    public void handshake() {
        // No-op for plaintext
    }

    /**
     * Read bytes from the socket into the destination buffer.
     *
     * <p>For io_uring, reads happen in two phases:
     * <ol>
     *   <li>Submit: The IoUringSelector submits a recv SQE for this fd</li>
     *   <li>Complete: The CQE result is stored here and returned to the caller</li>
     * </ol>
     *
     * <p>For synchronous compatibility with KafkaChannel's read loop, we perform a
     * synchronous recv via the selector's blocking read mechanism.
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (!open) throw new IOException("Channel closed");
        return selector.performRead(this, dst);
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return read(dsts, 0, dsts.length);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        long totalRead = 0;
        for (int i = offset; i < offset + length; i++) {
            if (dsts[i].hasRemaining()) {
                int n = read(dsts[i]);
                if (n < 0) {
                    return totalRead > 0 ? totalRead : -1;
                }
                totalRead += n;
                if (dsts[i].hasRemaining()) {
                    break;
                }
            }
        }
        return totalRead;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (!open) throw new IOException("Channel closed");
        return selector.performWrite(this, src);
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return write(srcs, 0, srcs.length);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        long totalWritten = 0;
        for (int i = offset; i < offset + length; i++) {
            if (srcs[i].hasRemaining()) {
                int n = write(srcs[i]);
                if (n <= 0) {
                    break;
                }
                totalWritten += n;
                if (srcs[i].hasRemaining()) {
                    break;
                }
            }
        }
        return totalWritten;
    }

    @Override
    public boolean hasPendingWrites() {
        return false;
    }

    @Override
    public Principal peerPrincipal() {
        return principal;
    }

    @Override
    public void addInterestOps(int ops) {
        key.interestOps(key.interestOps() | ops);
    }

    @Override
    public void removeInterestOps(int ops) {
        key.interestOps(key.interestOps() & ~ops);
    }

    @Override
    public boolean isMute() {
        return key.isValid() && (key.interestOps() & SelectionKey.OP_READ) == 0;
    }

    @Override
    public boolean hasBytesBuffered() {
        return false;
    }

    @Override
    public long transferFrom(FileChannel fileChannel, long position, long count) throws IOException {
        // For io_uring, we read from the file channel into a buffer, then write to the socket.
        // Zero-copy splice via io_uring could be added later.
        int len = (int) Math.min(count, 8192);
        ByteBuffer buf = ByteBuffer.allocateDirect(len);
        try {
            int read = fileChannel.read(buf, position);
            if (read <= 0) return read;
            buf.flip();
            return write(buf);
        } finally {
            // Direct buffer will be GC'd
        }
    }

    // ---- Internal state management used by IoUringSelector ----

    void setConnectResult(int result) {
        this.connectResult = result;
        this.connectPending = true;
    }

    void setConnected(boolean connected) {
        this.connected = connected;
    }

    /**
     * Get a description suitable for logging.
     */
    public String socketDescription() {
        try {
            InetSocketAddress localAddr = IoUring.getLocalAddress(fd);
            InetSocketAddress remoteAddr = IoUring.getRemoteAddress(fd);
            if (localAddr != null && remoteAddr != null) {
                return localAddr + " -> " + remoteAddr;
            }
        } catch (Exception e) {
            // ignore
        }
        return "fd=" + fd;
    }
}
