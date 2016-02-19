package com.notnoop.apns.internal.netty.channel;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import com.notnoop.apns.ReconnectPolicy;
import com.notnoop.exceptions.ChannelProviderClosedException;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;

// TODO test
public class NettyChannelProviderImpl extends AbstractChannelProvider {

    private final ReconnectPolicy reconnectPolicy;
    private final SSLContext sslContext;
    private final Bootstrap bootstrap;
    private final String host;

    private final int port;
    private final AtomicReference<ChannelFuture> channelFutureReference = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public NettyChannelProviderImpl(EventLoopGroup eventLoopGroup,
            ReconnectPolicy reconnectPolicy, String host, int port,
            int readTimeout, SSLContext sslContext) {
        this.reconnectPolicy = reconnectPolicy;
        this.host = host;
        this.port = port;
        this.sslContext = sslContext;
        bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
    }

    public ChannelFuture getCurrentChannelFuture() {
        return channelFutureReference.get();
    }

    // @Override
    public Channel getChannel() {
        if (closed.get()) {
            throw new ChannelProviderClosedException();
        }
        
        ChannelFuture channelFuture = channelFutureReference.get();

        if (reconnectPolicy.shouldReconnect() && channelFuture != null) {
            try {
                closeCurrentChannel();
            } catch (Throwable t) {
                LOGGER.error("Error while closing connection", t);
            }
        }
        if (channelFuture == null || !channelFuture.channel().isActive()) {
            try {
                channelFuture = bootstrap.connect(host, port).sync();
                channelFutureReference.set(channelFuture);
                reconnectPolicy.reconnected();
                LOGGER.debug("APNS reconnected");
            } catch (InterruptedException e) {
                LOGGER.error("Error while connecting", e);
            }
        }
        return channelFuture.channel();
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("Closing channel provider...");
        closed.getAndSet(true);
        closeCurrentChannel();
    }
    
    private void closeCurrentChannel() {
        final ChannelFuture channelFuture = channelFutureReference
                .getAndSet(null);
        Channel channel = null;
        if (channelFuture != null
                && (channel = channelFuture.channel()) != null) {
            LOGGER.debug("Async-closing channel...");
            channel.close();
        }
    }

    @Override
    public void closeChannel(Channel channel) throws IOException {
        final ChannelFuture channelFuture = channelFutureReference.get();
        if (channelFuture != null && channelFuture.channel() == channel) {
            channelFutureReference.set(null);
        }

        LOGGER.debug("Async-closing channel...");
        channel.close();
    }

    @Override
    public void runWithChannel(WithChannelAction action) throws Exception {
        Channel channel = getChannel();
        try {
            action.perform(channel);
        } catch (Exception e) {
            channel.read();
            throw e;
        }
    }

    @Override
    public void init() {
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(final SocketChannel ch) throws Exception {
                SSLEngine engine = sslContext.createSSLEngine();
                engine.setUseClientMode(true);
                ch.pipeline().addFirst("ssl", new SslHandler(engine));
                for (ChannelHandler h : NettyChannelProviderImpl.this
                        .getChannelHandlersProvider().getChannelHandlers()) {
                    ch.pipeline().addLast(h);
                }
                ch.config().setAutoClose(false);
                ch.closeFuture().addListener(new ChannelFutureListener() {

                    @Override
                    public void operationComplete(ChannelFuture future)
                            throws Exception {
                        getChannelClosedListener().onChannelClosed(ch);
                    }
                });
            }
        });
    }

}
