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

import io.netty.channel.uring.KafkaIoUringBridge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bridge to Linux io_uring via Netty 4.2's native io_uring transport.
 *
 * <p>This class delegates to {@link KafkaIoUringBridge} which lives in Netty's
 * {@code io.netty.channel.uring} package to access the package-private native API.
 *
 * <p>io_uring uses two ring buffers shared between userspace and the kernel:
 * <ul>
 *   <li>Submission Queue (SQ): userspace submits I/O requests (SQEs) here</li>
 *   <li>Completion Queue (CQ): kernel posts I/O results (CQEs) here</li>
 * </ul>
 *
 * <p>Each CQE contains the result of the operation and a user_data field that identifies
 * which operation completed. We encode the channel index and operation type into user_data.
 */
public final class IoUring {
    private static final Logger log = LoggerFactory.getLogger(IoUring.class);

    // Operation types encoded in user_data
    public static final int OP_POLL = 0;
    public static final int OP_RECV = 1;
    public static final int OP_SEND = 2;
    public static final int OP_CONNECT = 3;
    public static final int OP_ACCEPT = 4;
    public static final int OP_WAKEUP = 5;

    // Poll event masks (matching Linux poll.h)
    public static final int POLLIN = 0x0001;
    public static final int POLLOUT = 0x0004;
    public static final int POLLERR = 0x0008;
    public static final int POLLHUP = 0x0010;
    public static final int POLLRDHUP = 0x2000;

    private static final boolean AVAILABLE;

    // Internal mapping from raw fd to bridge socket handle (for IoUring-created sockets)
    private static final ConcurrentHashMap<Integer, Long> MANAGED_SOCKETS = new ConcurrentHashMap<>();

    static {
        boolean available = false;
        try {
            if (isLinux()) {
                available = KafkaIoUringBridge.isAvailable();
                if (available) {
                    log.info("io_uring (via Netty) is available on this system");
                } else {
                    Throwable cause = KafkaIoUringBridge.unavailabilityCause();
                    log.debug("io_uring is not available: {}",
                        cause != null ? cause.getMessage() : "unknown reason");
                }
            } else {
                log.debug("io_uring is only supported on Linux");
            }
        } catch (Throwable e) {
            log.debug("io_uring availability check failed: {}", e.getMessage());
        }
        AVAILABLE = available;
    }

    private IoUring() {}

    /**
     * Returns true if io_uring is available on this platform.
     */
    public static boolean isAvailable() {
        return AVAILABLE;
    }

    private static boolean isLinux() {
        return System.getProperty("os.name", "").toLowerCase().contains("linux");
    }

    // ---- Ring lifecycle ----

    /**
     * Create a new io_uring instance.
     *
     * @param entries the number of SQ entries (will be rounded up to next power of 2 by kernel)
     * @return opaque ring handle
     * @throws IoUringException if setup fails
     */
    public static long setup(int entries) throws IoUringException {
        try {
            return KafkaIoUringBridge.createRing(entries);
        } catch (IOException e) {
            throw new IoUringException("io_uring setup failed: " + e.getMessage());
        }
    }

    /**
     * Destroy an io_uring instance.
     *
     * @param ring opaque ring handle
     */
    public static void close(long ring) {
        KafkaIoUringBridge.closeRing(ring);
    }

    // ---- Event fd for wakeup ----

    /**
     * Create an eventfd for waking up a blocked io_uring_wait.
     *
     * @return the eventfd file descriptor
     * @throws IoUringException if creation fails
     */
    public static int createEventFd() throws IoUringException {
        try {
            return KafkaIoUringBridge.newEventFd();
        } catch (IOException e) {
            throw new IoUringException("eventfd creation failed: " + e.getMessage());
        }
    }

    /**
     * Signal an eventfd to wake up a blocked io_uring_wait.
     *
     * @param eventFd the eventfd file descriptor
     */
    public static void signalEventFd(int eventFd) {
        KafkaIoUringBridge.signalEventFd(eventFd, 1L);
    }

    // ---- Submission queue operations ----

    /**
     * Prepare a poll_add SQE. This monitors a file descriptor for readability/writability.
     *
     * @param ring     opaque ring handle
     * @param fd       the file descriptor to poll
     * @param pollMask poll event mask (POLLIN, POLLOUT, etc.)
     * @param userData user data to identify this operation in the CQE
     */
    public static void prepPollAdd(long ring, int fd, int pollMask, long userData) {
        KafkaIoUringBridge.sqeAddPollAdd(ring, fd, pollMask, false, userData);
    }

    /**
     * Prepare a multishot poll_add SQE. This keeps monitoring until explicitly cancelled.
     *
     * @param ring     opaque ring handle
     * @param fd       the file descriptor to poll
     * @param pollMask poll event mask
     * @param userData user data to identify this operation in the CQE
     */
    public static void prepPollAddMultishot(long ring, int fd, int pollMask, long userData) {
        KafkaIoUringBridge.sqeAddPollAdd(ring, fd, pollMask, true, userData);
    }

    /**
     * Prepare a recv SQE.
     *
     * @param ring       opaque ring handle
     * @param fd         the socket file descriptor
     * @param bufAddress direct ByteBuffer address
     * @param length     buffer capacity
     * @param userData   user data for CQE
     * @return 0 on success, negative on failure
     */
    public static int prepRecv(long ring, int fd, long bufAddress, int length, long userData) {
        long token = KafkaIoUringBridge.sqeAddRecv(ring, fd, bufAddress, length, userData);
        return token < 0 ? (int) token : 0;
    }

    /**
     * Prepare a send SQE.
     *
     * @param ring       opaque ring handle
     * @param fd         the socket file descriptor
     * @param bufAddress direct ByteBuffer address
     * @param length     number of bytes to send
     * @param userData   user data for CQE
     * @return 0 on success, negative on failure
     */
    public static int prepSend(long ring, int fd, long bufAddress, int length, long userData) {
        long token = KafkaIoUringBridge.sqeAddSend(ring, fd, bufAddress, length, userData);
        return token < 0 ? (int) token : 0;
    }

    /**
     * Prepare an eventfd read SQE. The kernel will complete this when the eventfd counter
     * is >= 1, reading 8 bytes (the counter value) and atomically decrementing it.
     * This is used for wakeup signaling: submit one read SQE, get CQE when signaled,
     * then re-submit to arm the next wakeup.
     *
     * @param ring       opaque ring handle
     * @param eventFd    the eventfd file descriptor
     * @param bufAddress address of an 8-byte direct buffer
     * @param userData   user data for the CQE
     */
    public static void prepEventFdRead(long ring, int eventFd, long bufAddress, long userData) {
        KafkaIoUringBridge.sqeAddEventFdRead(ring, eventFd, bufAddress, userData);
    }

    /**
     * Prepare a cancel SQE to cancel a previously submitted operation.
     *
     * @param ring     opaque ring handle
     * @param userData the user_data of the operation to cancel
     */
    public static void prepCancel(long ring, long userData) {
        KafkaIoUringBridge.sqeAddCancel(ring, userData, userData);
    }

    // ---- Submit and wait ----

    /**
     * Submit all pending SQEs and optionally wait for completions.
     *
     * @param ring         opaque ring handle
     * @param waitNr       minimum number of CQEs to wait for (0 for non-blocking)
     * @param timeoutNanos timeout in nanoseconds (ignored; the bridge blocks for at least 1 CQE when waitNr > 0)
     * @return number of SQEs submitted
     */
    public static int submitAndWait(long ring, int waitNr, long timeoutNanos) {
        if (waitNr > 0) {
            return KafkaIoUringBridge.submitAndWait(ring);
        } else {
            return KafkaIoUringBridge.submit(ring);
        }
    }

    /**
     * Submit all pending SQEs without waiting.
     *
     * @param ring opaque ring handle
     * @return number of SQEs submitted
     */
    public static int submit(long ring) {
        return KafkaIoUringBridge.submit(ring);
    }

    // ---- Completion queue operations ----

    /**
     * Process all available completion queue entries via a callback.
     *
     * @param ring    opaque ring handle
     * @param handler callback invoked for each CQE with (result, flags, userData)
     * @return number of CQEs processed
     */
    public static int processCompletions(long ring, KafkaIoUringBridge.CompletionHandler handler) {
        return KafkaIoUringBridge.processCompletions(ring, handler);
    }

    // ---- Socket operations ----

    /**
     * Create a non-blocking TCP socket.
     *
     * @param ipv6 true for AF_INET6, false for AF_INET
     * @return the socket file descriptor, or negative on failure
     */
    public static int openSocket(boolean ipv6) {
        try {
            long handle = KafkaIoUringBridge.newTcpSocket(ipv6);
            int fd = KafkaIoUringBridge.socketFd(handle);
            MANAGED_SOCKETS.put(fd, handle);
            return fd;
        } catch (IOException e) {
            log.debug("Failed to create TCP socket", e);
            return -1;
        }
    }

    /**
     * Configure socket options.
     *
     * @param fd        the socket file descriptor
     * @param noDelay   TCP_NODELAY
     * @param keepAlive SO_KEEPALIVE
     * @param sendBuf   SO_SNDBUF (0 to skip)
     * @param recvBuf   SO_RCVBUF (0 to skip)
     * @return 0 on success, negative on failure
     */
    public static int configureSocket(int fd, boolean noDelay, boolean keepAlive, int sendBuf, int recvBuf) {
        Long handle = MANAGED_SOCKETS.get(fd);
        if (handle == null) return -1;
        try {
            KafkaIoUringBridge.configureSocket(handle, noDelay, keepAlive, sendBuf, recvBuf);
            return 0;
        } catch (IOException e) {
            log.debug("Failed to configure socket fd={}", fd, e);
            return -1;
        }
    }

    /**
     * Initiate a non-blocking connect on a managed socket.
     *
     * @param fd      the socket file descriptor (from {@link #openSocket})
     * @param address remote address to connect to
     * @param port    remote port
     * @return true if the connection was established immediately, false if in progress
     * @throws IOException on fatal connect error
     */
    public static boolean startConnect(int fd, InetAddress address, int port) throws IOException {
        Long handle = MANAGED_SOCKETS.get(fd);
        if (handle == null) throw new IOException("startConnect: unknown fd=" + fd);
        return KafkaIoUringBridge.connect(handle, address, port);
    }

    /**
     * Finish an in-progress non-blocking connect.
     *
     * @param fd the socket file descriptor
     * @return true if connected, false if still in progress
     * @throws IOException if the connection failed
     */
    public static boolean finishConnect(int fd) throws IOException {
        Long handle = MANAGED_SOCKETS.get(fd);
        if (handle == null) throw new IOException("finishConnect: unknown fd=" + fd);
        return KafkaIoUringBridge.finishConnect(handle);
    }

    /**
     * Close a file descriptor. If it's a managed socket (created via {@link #openSocket}),
     * the socket handle is also cleaned up.
     *
     * @param fd the file descriptor to close
     */
    public static void closeFd(int fd) {
        Long handle = MANAGED_SOCKETS.remove(fd);
        if (handle != null) {
            try {
                KafkaIoUringBridge.closeSocket(handle);
            } catch (IOException e) {
                log.debug("Error closing managed socket fd={}", fd, e);
            }
        } else {
            KafkaIoUringBridge.closeFd(fd);
        }
    }

    /**
     * Get the local address and port of a socket.
     *
     * @param fd the socket file descriptor
     * @return local address, or null if unavailable
     */
    public static InetSocketAddress getLocalAddress(int fd) {
        Long handle = MANAGED_SOCKETS.get(fd);
        if (handle != null) {
            return KafkaIoUringBridge.localAddress(handle);
        }
        return null;
    }

    /**
     * Get the remote address and port of a socket.
     *
     * @param fd the socket file descriptor
     * @return remote address, or null if unavailable
     */
    public static InetSocketAddress getRemoteAddress(int fd) {
        Long handle = MANAGED_SOCKETS.get(fd);
        if (handle != null) {
            return KafkaIoUringBridge.remoteAddress(handle);
        }
        return null;
    }

    /**
     * Get the direct buffer address for use with io_uring SQEs.
     *
     * @param buffer a direct ByteBuffer
     * @return the native memory address of the buffer
     */
    public static long bufferAddress(ByteBuffer buffer) {
        return KafkaIoUringBridge.bufferAddress(buffer);
    }

    // ---- User data encoding/decoding ----

    /**
     * Encode channel index and operation type into a single long user_data value.
     *
     * @param channelIndex the channel index (0 - 2^56)
     * @param opType       the operation type (OP_POLL, OP_RECV, etc.)
     * @return encoded user_data
     */
    public static long encodeUserData(long channelIndex, int opType) {
        return (channelIndex << 8) | (opType & 0xFF);
    }

    /**
     * Decode the channel index from user_data.
     */
    public static long decodeChannelIndex(long userData) {
        return userData >>> 8;
    }

    /**
     * Decode the operation type from user_data.
     */
    public static int decodeOpType(long userData) {
        return (int) (userData & 0xFF);
    }
}
