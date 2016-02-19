package com.notnoop.apns.internal.netty;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.notnoop.apns.ApnsDelegate;
import com.notnoop.apns.ApnsNotification;
import com.notnoop.apns.DeliveryResult;
import com.notnoop.apns.internal.ApnsConnection;
import com.notnoop.apns.internal.Utilities;
import com.notnoop.apns.internal.netty.cache.CacheStore;
import com.notnoop.apns.internal.netty.cache.CacheStore.Drainer;
import com.notnoop.apns.internal.netty.channel.ChannelProvider;
import com.notnoop.apns.internal.netty.channel.ChannelProvider.ChannelClosedListener;
import com.notnoop.apns.internal.netty.channel.ChannelProvider.ChannelHandlersProvider;
import com.notnoop.apns.internal.netty.channel.ChannelProvider.WithChannelAction;
import com.notnoop.apns.internal.netty.encoding.ApnsResultDecoder;
import com.notnoop.exceptions.ApnsDeliveryErrorException;
import com.notnoop.exceptions.ChannelProviderClosedException;
import com.notnoop.exceptions.NetworkIOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;

public class NettyApnsConnectionImpl implements ApnsConnection,
        DeliveryResultListener, ChannelClosedListener {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(NettyApnsConnectionImpl.class);

    private static final int RETRIES = 3;
    private static final int DELAY_IN_MS = 1000;

    private final ApnsDelegate delegate;
    private final ChannelProvider channelProvider;
    private final CacheStore cacheStore;

    private final ExecutorService drainBufferExecutorService;
    private final ExecutorService deliveryResultExecutorService;
    private final boolean deliveryResultExecutorServiceProvided;

    private final Object lockSendMessage = new Object();

    // This semaphore is used to control when is allowed to send messages to the
    // channel, it will be disallowed when a response is received from the APNS
    // server (onDeliveryResult is called) and allowed when the cache is moved
    // to the buffer to continue with new connection, avoid the possibility to
    // send a message twice to the APNS server.
    private final Semaphore allowSendSemaphore = new Semaphore(1, true);

    // This semaphore is used to avoid a race condition when a message is added
    // to the cache after sent to the channel and an error response is received
    // from the APNS server, it ensures the message will be added to the buffer
    // to be resent in the draining operation.
    private final Semaphore accessCacheStoreSemaphore = new Semaphore(1, true);

    public NettyApnsConnectionImpl(ChannelProvider channelProvider,
            ApnsDelegate delegate, CacheStore cacheStore,
            ExecutorService deliveryResultExecutorService) {
        this.delegate = delegate;
        this.channelProvider = channelProvider;
        this.cacheStore = cacheStore;

        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 30,
                TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        executor.allowCoreThreadTimeOut(true);

        drainBufferExecutorService = executor;

        if (deliveryResultExecutorService != null) {
            this.deliveryResultExecutorService = deliveryResultExecutorService;
            deliveryResultExecutorServiceProvided = true;
        } else {
            executor = new ThreadPoolExecutor(1, 1, 30, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>());
            executor.allowCoreThreadTimeOut(true);

            this.deliveryResultExecutorService = executor;
            deliveryResultExecutorServiceProvided = false;
        }
    }

    @Override
    public void setCacheLength(int cacheLength) {
        cacheStore.setCacheLength(cacheLength);
    }

    @Override
    public int getCacheLength() {
        return cacheStore.getCacheLength();
    }

    public void init() {
        channelProvider
                .setChannelHandlersProvider(new ChannelHandlersProvider() {
                    @Override
                    public List<ChannelHandler> getChannelHandlers() {
                        return Arrays.<ChannelHandler> asList(
                                new LoggingHandler(LogLevel.TRACE),
                                // After some unexplained CastClassExceptions,we
                                // handle
                                // manually the conversion to ByteBuf
                                /* new ApnsNotificationEncoder(), */
                                new ApnsResultDecoder(),
                                new WriteTimeoutHandler(10, TimeUnit.SECONDS),
                                new ApnsHandler(
                                        NettyApnsConnectionImpl.this));
                    }
                });
        channelProvider.setChannelClosedListener(this);
        channelProvider.init();
    }

    @Override
    public synchronized void close() throws IOException {
        LOGGER.debug("Closing netty APNS connection");

        LOGGER.debug("Shutdown of single-thread executor service for drain buffer...");
        drainBufferExecutorService.shutdown();
        try {
            LOGGER.debug("Waiting termination of single-thread executor service for drain buffer...");
            drainBufferExecutorService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("Termination did not complete in 30 seconds");
        }

        if (!deliveryResultExecutorServiceProvided) {
            LOGGER.debug("Shutdown of single-thread executor service for handling delivery results...");
            deliveryResultExecutorService.shutdown();
            try {
                LOGGER.debug("Waiting termination of single-thread executor service for handling delivery results...");
                deliveryResultExecutorService.awaitTermination(30,
                        TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.error("Termination did not complete in 30 seconds");
            }
        }

        LOGGER.debug("Close of channel provider...");
        channelProvider.close();
    }

    @Override
    public void sendMessage(ApnsNotification m) throws NetworkIOException {
        sendMessage(m, false);
    }

    protected void sendMessage(final ApnsNotification m,
            final boolean fromBuffer) {
        int attempts = 0;
        while (true) {
            attempts++;
            try {
                synchronized (lockSendMessage) {
                    channelProvider.runWithChannel(new WithChannelAction() {
                        @Override
                        public void perform(Channel channel) throws Exception {
                            try {
                                LOGGER.trace("Acquiring allowSendSemaphore in sendMessage");
                                allowSendSemaphore.acquire();
                                LOGGER.trace("Acquired allowSendSemaphore in sendMessage");
                                allowSendSemaphore.release();
                                LOGGER.trace("Released allowSendSemaphore in sendMessage");
                                LOGGER.trace("Acquiring accessCacheStoreSemaphore in sendMessage");
                                accessCacheStoreSemaphore.acquire();
                                LOGGER.trace("Acquired accessCacheStoreSemaphore in sendMessage");
                                write(channel, m);
                                cacheStore.add(m);
                            } finally {
                                accessCacheStoreSemaphore.release();
                                LOGGER.trace("Released accessCacheStoreSemaphore in sendMessage");
                            }

                            delegate.messageSent(m, fromBuffer);
                            LOGGER.trace("Message \"{}\" sent (fromBuffer={})",
                                    m, fromBuffer);
                        }
                    });
                }
                break;
            } catch (ChannelProviderClosedException e) {
                LOGGER.info(
                        "Failed to send message {} (fromBuffer={}, attempts={}: {})",
                        m, fromBuffer, attempts, e.getMessage());
                throw e;
            } catch (Exception e) {
                if (attempts > RETRIES) {
                    delegate.messageSendFailed(m, e);
                    Utilities.wrapAndThrowAsRuntimeException(e);
                }
                LOGGER.info("Failed to send message " + m + " (fromBuffer="
                        + fromBuffer + ", attempts=" + attempts
                        + " trying again after delay...)", e);
                Utilities.sleep(DELAY_IN_MS);
            }
        }
    }

    protected static void write(Channel channel, ApnsNotification m)
            throws InterruptedException {
        byte[] b = m.marshall();
        ByteBuf buf = Unpooled.buffer(b.length);
        buf.writeBytes(b);
        channel.writeAndFlush(buf).sync();
    }

    private void drainBuffer() {
        try {
            drainBufferExecutorService.submit(new Runnable() {

                @Override
                public void run() {
                    LOGGER.debug("Draining buffer of notifications that need to be resent");
                    cacheStore.drain(new Drainer() {
                        @Override
                        public void process(ApnsNotification notification) {
                            sendMessage(notification, true);
                        }
                    });
                }
            });
        } catch (RejectedExecutionException e) {
            LOGGER.warn("Execution of draining buffer rejected, connection must be shutdown");
        }
    }

    @Override
    public void onChannelClosed(Channel ch) {
        LOGGER.debug("Channel was closed");
    }

    @Override
    public void onDeliveryResult(final ChannelHandlerContext ctx,
            final DeliveryResult msg) {
        // We don't allow to send more messages
        LOGGER.trace("Acquiring allowSendSemaphore in onDeliveryResult");
        try {
            allowSendSemaphore.acquire();
        } catch (InterruptedException e) {
            Utilities.wrapAndThrowAsRuntimeException(e);
        }
        LOGGER.trace("Acquired allowSendSemaphore in onDeliveryResult");

        try {
            deliveryResultExecutorService.submit(new Runnable() {

                @Override
                public void run() {
                    Integer newCacheLength = null;
                    try {
                        // Wait if there is a flying send operation to put the
                        // notification in cache, it will be resent
                        LOGGER.trace("Acquiring accessCacheStoreSemaphore in onDeliveryResult");
                        try {
                            accessCacheStoreSemaphore.acquire();
                        } catch (InterruptedException e) {
                            Utilities.wrapAndThrowAsRuntimeException(e);
                        }
                        LOGGER.trace("Acquired accessCacheStoreSemaphore in onDeliveryResult");

                        // Move to the buffer all the notifications sent after
                        // the fail
                        Queue<ApnsNotification> tempCache = new LinkedList<ApnsNotification>();
                        ApnsNotification notification = cacheStore
                                .removeAllBefore(msg, tempCache);

                        if (notification != null) {
                            delegate.messageSendFailed(
                                    notification,
                                    new ApnsDeliveryErrorException(msg
                                            .getError()));
                        } else {
                            LOGGER.warn("Received error for message that wasn't in the cache...");
                            cacheStore.addAll(tempCache);
                            newCacheLength = cacheStore
                                    .resizeCacheIfNeeded(tempCache.size());

                            delegate.messageSendFailed(
                                    null,
                                    new ApnsDeliveryErrorException(msg
                                            .getError()));
                        }

                        delegate.notificationsResent(cacheStore
                                .moveCacheToBuffer());

                        // The current connection is closed or is to be closed,
                        // so we enforce to use a new one for next notifications
                        try {
                            channelProvider.closeChannel(ctx.channel());
                        } catch (IOException e) {
                            LOGGER.error(
                                    "Could not close connection: "
                                            + e.getMessage(), e);
                        }

                        delegate.connectionClosed(msg.getError(), msg.getId());

                        // Drain the buffer to resend the notifications
                        drainBuffer();
                    } finally {
                        if (newCacheLength != null) {
                            delegate.cacheLengthExceeded(newCacheLength);
                        }
                        accessCacheStoreSemaphore.release();
                        LOGGER.trace("Released accessCacheStoreSemaphore in onDeliveryResult");
                        allowSendSemaphore.release();
                        LOGGER.trace("Released allowSendSemaphore in onDeliveryResult");
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            LOGGER.warn("Execution of handling delivery result rejected, connection must be shutdown");
            allowSendSemaphore.release();
            LOGGER.trace("Released allowSendSemaphore in onDeliveryResult");
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            allowSendSemaphore.release();
            LOGGER.trace("Released allowSendSemaphore in onDeliveryResult");
        }

    }

    @Override
    public void testConnection() throws NetworkIOException {
        // TODO Auto-generated method stub

    }

    @Override
    public ApnsConnection copy() {
        // TODO Auto-generated method stub
        return null;
    }

}
