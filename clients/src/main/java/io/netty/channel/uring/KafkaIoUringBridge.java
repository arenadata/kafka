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
package io.netty.channel.uring;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bridge class providing access to Netty 4.2's package-private io_uring API.
 *
 * <p>This class lives in the {@code io.netty.channel.uring} package intentionally,
 * so that it can access Netty's internal (package-private) io_uring classes:
 * {@link RingBuffer}, {@link SubmissionQueue}, {@link CompletionQueue},
 * {@link Native}, and {@link LinuxSocket}.
 *
 * <p>It exposes a stable, opaque-handle-based API to Kafka's io_uring integration
 * in {@code org.apache.kafka.common.network.iouring}.
 */
public final class KafkaIoUringBridge {

    // Opaque handles -> RingBuffer instances
    private static final Map<Long, RingBuffer> RINGS = new ConcurrentHashMap<>();
    private static final Map<Long, LinuxSocket> SOCKETS = new ConcurrentHashMap<>();
    private static final AtomicLong NEXT_HANDLE = new AtomicLong(1);

    private KafkaIoUringBridge() {}

    // ---- Availability ----

    /**
     * Returns true if io_uring is available on this system.
     */
    public static boolean isAvailable() {
        return IoUring.isAvailable();
    }

    /**
     * Returns the cause if io_uring is unavailable, or null if available.
     */
    public static Throwable unavailabilityCause() {
        return IoUring.unavailabilityCause();
    }

    // ---- Ring lifecycle ----

    /**
     * Create an io_uring ring and return an opaque handle.
     *
     * @param ringSize number of SQ entries (rounded up to power of 2 by kernel)
     * @return opaque ring handle
     * @throws IOException if ring creation fails
     */
    public static long createRing(int ringSize) throws IOException {
        try {
            RingBuffer ring = Native.createRingBuffer(ringSize, Native.setupFlags(false));
            ring.enable();
            long handle = NEXT_HANDLE.getAndIncrement();
            RINGS.put(handle, ring);
            return handle;
        } catch (Exception e) {
            throw new IOException("Failed to create io_uring ring", e);
        }
    }

    /**
     * Close and destroy an io_uring ring.
     *
     * @param ringHandle opaque handle returned by {@link #createRing}
     */
    public static void closeRing(long ringHandle) {
        RingBuffer ring = RINGS.remove(ringHandle);
        if (ring != null) {
            ring.close();
        }
    }

    /**
     * Returns the ring file descriptor.
     */
    public static int ringFd(long ringHandle) {
        return RINGS.get(ringHandle).fd();
    }

    // ---- Submission queue operations ----

    /**
     * Enqueue a POLL_ADD SQE to monitor a file descriptor for I/O readiness.
     *
     * @param ringHandle opaque ring handle
     * @param fd         file descriptor to monitor
     * @param pollMask   events to monitor (POLLIN=0x0001, POLLOUT=0x0004, POLLRDHUP=0x2000, etc.)
     * @param multishot  if true, keep firing after each event (kernel 5.13+); false = oneshot
     * @param udata      user data returned in the CQE
     * @return SQE token, or negative on failure
     */
    public static long sqeAddPollAdd(long ringHandle, int fd, int pollMask, boolean multishot, long udata) {
        SubmissionQueue sq = RINGS.get(ringHandle).ioUringSubmissionQueue();
        // For IORING_OP_POLL_ADD the io_uring_sqe fields are:
        //   sqe->len         (enqueueSqe param 7, SQE offset 24) = POLL_ADD flags
        //                    e.g. IORING_POLL_ADD_MULTI (=1) for multishot mode
        //   sqe->poll32_events (enqueueSqe param 8 = union3, SQE offset 28) = event mask
        //                    e.g. POLLIN|POLLRDHUP
        // ioPrio (SQE offset 2) is NOT used for POLL_ADD; the multishot flag goes in len, not ioPrio.
        int sqeLen = multishot ? (Native.IORING_POLL_ADD_MULTI & 0xFFFF) : 0;
        return sq.enqueueSqe(
            Native.IORING_OP_POLL_ADD,
            (byte) 0, (short) 0,  // flags, ioPrio (ioPrio unused for POLL_ADD)
            fd,
            0L,       // off
            0L,       // addr
            sqeLen,   // len (SQE offset 24) = POLL_ADD flags (IORING_POLL_ADD_MULTI for multishot)
            pollMask, // union3 (SQE offset 28) = poll32_events = actual event mask
            udata,
            (short) 0, (short) 0, 0, 0L
        );
    }

    /**
     * Enqueue a POLL_REMOVE SQE to cancel a previous POLL_ADD.
     *
     * @param ringHandle opaque ring handle
     * @param udata      user data of the POLL_ADD SQE to cancel
     * @return SQE token, or negative on failure
     */
    public static long sqeAddPollRemove(long ringHandle, long udata) {
        SubmissionQueue sq = RINGS.get(ringHandle).ioUringSubmissionQueue();
        return sq.enqueueSqe(
            Native.IORING_OP_POLL_REMOVE,
            (byte) 0, (short) 0,
            -1,
            0L,
            udata,  // addr = user_data of SQE to cancel
            0, 0, udata,
            (short) 0, (short) 0, 0, 0L
        );
    }

    /**
     * Enqueue an ASYNC_CANCEL SQE to cancel any pending operation by user data.
     *
     * @param ringHandle opaque ring handle
     * @param sqeUdata   user data of the SQE to cancel
     * @param udata      user data for this cancel CQE
     * @return SQE token, or negative on failure
     */
    public static long sqeAddCancel(long ringHandle, long sqeUdata, long udata) {
        SubmissionQueue sq = RINGS.get(ringHandle).ioUringSubmissionQueue();
        return sq.addCancel(sqeUdata, udata);
    }

    /**
     * Enqueue a RECV SQE.
     *
     * @param ringHandle opaque ring handle
     * @param fd         socket file descriptor
     * @param bufAddress direct buffer native address
     * @param len        maximum bytes to receive
     * @param udata      user data for the CQE
     * @return SQE token, or negative on failure
     */
    public static long sqeAddRecv(long ringHandle, int fd, long bufAddress, int len, long udata) {
        SubmissionQueue sq = RINGS.get(ringHandle).ioUringSubmissionQueue();
        return sq.enqueueSqe(
            Native.IORING_OP_RECV,
            (byte) 0, (short) 0,
            fd,
            0L,         // off
            bufAddress, // addr
            len,        // len
            0,          // msg_flags
            udata,
            (short) 0, (short) 0, 0, 0L
        );
    }

    /**
     * Enqueue a SEND SQE.
     *
     * @param ringHandle opaque ring handle
     * @param fd         socket file descriptor
     * @param bufAddress direct buffer native address
     * @param len        number of bytes to send
     * @param udata      user data for the CQE
     * @return SQE token, or negative on failure
     */
    public static long sqeAddSend(long ringHandle, int fd, long bufAddress, int len, long udata) {
        SubmissionQueue sq = RINGS.get(ringHandle).ioUringSubmissionQueue();
        return sq.enqueueSqe(
            Native.IORING_OP_SEND,
            (byte) 0, (short) 0,
            fd,
            0L,         // off
            bufAddress, // addr
            len,        // len
            0,          // msg_flags
            udata,
            (short) 0, (short) 0, 0, 0L
        );
    }

    /**
     * Enqueue an eventfd read SQE (used for wakeup signaling).
     *
     * @param ringHandle    opaque ring handle
     * @param eventFd       the eventfd file descriptor
     * @param bufAddress    address of an 8-byte direct buffer to hold the eventfd counter value
     * @param udata         user data for the CQE
     * @return SQE token, or negative on failure
     */
    public static long sqeAddEventFdRead(long ringHandle, int eventFd, long bufAddress, long udata) {
        SubmissionQueue sq = RINGS.get(ringHandle).ioUringSubmissionQueue();
        return sq.addEventFdRead(eventFd, bufAddress, 0, 8, udata);
    }

    // ---- Submit ----

    /**
     * Submit all pending SQEs without waiting for completions.
     *
     * @param ringHandle opaque ring handle
     * @return number of SQEs submitted
     */
    public static int submit(long ringHandle) {
        return RINGS.get(ringHandle).ioUringSubmissionQueue().submit();
    }

    /**
     * Submit all pending SQEs and block until at least one CQE is available.
     *
     * @param ringHandle opaque ring handle
     * @return number of SQEs submitted
     */
    public static int submitAndWait(long ringHandle) {
        return RINGS.get(ringHandle).ioUringSubmissionQueue().submitAndGet();
    }

    /**
     * Submit all pending SQEs and poll for any already-available CQEs (non-blocking).
     *
     * @param ringHandle opaque ring handle
     * @return number of SQEs submitted
     */
    public static int submitAndPoll(long ringHandle) {
        return RINGS.get(ringHandle).ioUringSubmissionQueue().submitAndGetNow();
    }

    // ---- Completion queue ----

    /**
     * Functional interface for handling io_uring completions.
     */
    @FunctionalInterface
    public interface CompletionHandler {
        /**
         * Called for each completion queue entry.
         *
         * @param res   result: positive = bytes transferred, negative = -errno
         * @param flags CQE flags (e.g., IORING_CQE_F_MORE for multishot)
         * @param udata user data that was set when the SQE was submitted
         */
        void handle(int res, int flags, long udata);
    }

    /**
     * Process all available completion queue entries.
     *
     * @param ringHandle opaque ring handle
     * @param handler    callback invoked for each CQE
     * @return number of CQEs processed
     */
    public static int processCompletions(long ringHandle, CompletionHandler handler) {
        CompletionQueue cq = RINGS.get(ringHandle).ioUringCompletionQueue();
        return cq.process((res, flags, udata, extraCqeData) -> handler.handle(res, flags, udata));
    }

    /**
     * Returns true if there are pending completions in the ring.
     *
     * @param ringHandle opaque ring handle
     */
    public static boolean hasCompletions(long ringHandle) {
        return RINGS.get(ringHandle).ioUringCompletionQueue().hasCompletions();
    }

    // ---- EventFd ----

    /**
     * Create a blocking eventfd for wakeup. Returns the file descriptor.
     *
     * @throws IOException if eventfd creation fails
     */
    public static int newEventFd() throws IOException {
        try {
            return Native.newBlockingEventFd().intValue();
        } catch (Exception e) {
            throw new IOException("Failed to create eventfd", e);
        }
    }

    /**
     * Signal an eventfd to wake up a blocked io_uring wait.
     *
     * @param fd    the eventfd file descriptor
     * @param value the counter increment to write
     */
    public static void signalEventFd(int fd, long value) {
        Native.eventFdWrite(fd, value);
    }

    // ---- Socket operations ----

    /**
     * Create a new non-blocking TCP socket and return an opaque socket handle.
     *
     * @param ipv6 true to create an AF_INET6 socket, false for AF_INET
     * @return opaque socket handle
     * @throws IOException if socket creation fails
     */
    public static long newTcpSocket(boolean ipv6) throws IOException {
        try {
            // Use reflection to call Socket.newSocketStream0(boolean) which is protected
            java.lang.reflect.Method method =
                io.netty.channel.unix.Socket.class.getDeclaredMethod("newSocketStream0", boolean.class);
            method.setAccessible(true);
            int fd = (int) method.invoke(null, ipv6);
            LinuxSocket sock = new LinuxSocket(fd);
            long handle = NEXT_HANDLE.getAndIncrement();
            SOCKETS.put(handle, sock);
            return handle;
        } catch (java.lang.reflect.InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) throw (IOException) cause;
            throw new IOException("Failed to create TCP socket", cause != null ? cause : e);
        } catch (Exception e) {
            throw new IOException("Failed to create TCP socket", e);
        }
    }

    /**
     * Return the raw file descriptor for a socket handle.
     *
     * @param socketHandle opaque socket handle
     */
    public static int socketFd(long socketHandle) {
        return SOCKETS.get(socketHandle).intValue();
    }

    /**
     * Configure common socket options.
     *
     * @param socketHandle opaque socket handle
     * @param tcpNoDelay   enable TCP_NODELAY
     * @param keepAlive    enable SO_KEEPALIVE
     * @param sendBufSize  SO_SNDBUF in bytes (0 = OS default)
     * @param recvBufSize  SO_RCVBUF in bytes (0 = OS default)
     * @throws IOException if configuration fails
     */
    public static void configureSocket(long socketHandle, boolean tcpNoDelay,
                                       boolean keepAlive, int sendBufSize, int recvBufSize)
            throws IOException {
        LinuxSocket sock = SOCKETS.get(socketHandle);
        if (tcpNoDelay) sock.setTcpNoDelay(true);
        if (keepAlive) sock.setKeepAlive(true);
        if (sendBufSize > 0) sock.setSendBufferSize(sendBufSize);
        if (recvBufSize > 0) sock.setReceiveBufferSize(recvBufSize);
    }

    /**
     * Bind a socket to an address.
     *
     * @param socketHandle opaque socket handle
     * @param address      address to bind to
     * @param port         port to bind to
     * @throws IOException if bind fails
     */
    public static void bind(long socketHandle, InetAddress address, int port) throws IOException {
        SOCKETS.get(socketHandle).bind(new InetSocketAddress(address, port));
    }

    /**
     * Start listening on a bound socket.
     *
     * @param socketHandle opaque socket handle
     * @param backlog      listen backlog
     * @throws IOException if listen fails
     */
    public static void listen(long socketHandle, int backlog) throws IOException {
        SOCKETS.get(socketHandle).listen(backlog);
    }

    /**
     * Connect a socket to a remote address (non-blocking).
     *
     * @param socketHandle opaque socket handle
     * @param address      remote address
     * @param port         remote port
     * @return true if immediately connected, false if connection is in progress
     * @throws IOException if connect fails fatally
     */
    public static boolean connect(long socketHandle, InetAddress address, int port) throws IOException {
        return SOCKETS.get(socketHandle).connect(new InetSocketAddress(address, port));
    }

    /**
     * Finish an in-progress connection (call after POLLOUT).
     *
     * @param socketHandle opaque socket handle
     * @return true if connected
     * @throws IOException if connection failed
     */
    public static boolean finishConnect(long socketHandle) throws IOException {
        return SOCKETS.get(socketHandle).finishConnect();
    }

    /**
     * Close and remove a socket.
     *
     * @param socketHandle opaque socket handle
     */
    public static void closeSocket(long socketHandle) throws IOException {
        LinuxSocket sock = SOCKETS.remove(socketHandle);
        if (sock != null) {
            sock.close();
        }
    }

    /**
     * Close a raw file descriptor (e.g., one accepted via io_uring or NIO extraction).
     *
     * @param fd file descriptor to close
     */
    public static void closeFd(int fd) {
        try {
            new io.netty.channel.unix.FileDescriptor(fd).close();
        } catch (Exception e) {
            // Best effort; fd may already be closed
        }
    }

    // ---- Buffer address ----

    /**
     * Return the native memory address of a direct ByteBuffer (for use in io_uring SQEs).
     *
     * @param buffer a direct ByteBuffer
     * @return the native address of the buffer's current position
     */
    public static long bufferAddress(ByteBuffer buffer) {
        return io.netty.util.internal.PlatformDependent.directBufferAddress(buffer);
    }

    /**
     * Allocate an 8-byte direct ByteBuffer for eventfd reads.
     */
    public static ByteBuffer allocateEventFdBuffer() {
        return ByteBuffer.allocateDirect(8);
    }

    // ---- Local/remote address retrieval ----

    /**
     * Get the local address of a socket handle.
     *
     * @param socketHandle opaque socket handle
     * @return local InetSocketAddress, or null on failure
     */
    public static InetSocketAddress localAddress(long socketHandle) {
        try {
            LinuxSocket sock = SOCKETS.get(socketHandle);
            if (sock == null) return null;
            return sock.localAddress();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get the remote address of a socket handle.
     *
     * @param socketHandle opaque socket handle
     * @return remote InetSocketAddress, or null on failure
     */
    public static InetSocketAddress remoteAddress(long socketHandle) {
        try {
            LinuxSocket sock = SOCKETS.get(socketHandle);
            if (sock == null) return null;
            return sock.remoteAddress();
        } catch (Exception e) {
            return null;
        }
    }
}
