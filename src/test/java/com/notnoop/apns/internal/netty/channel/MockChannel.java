package com.notnoop.apns.internal.netty.channel;

import java.util.Date;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.notnoop.apns.ApnsNotification;
import com.notnoop.apns.DeliveryError;
import com.notnoop.apns.DeliveryResult;
import com.notnoop.apns.EnhancedApnsNotification;
import com.notnoop.apns.internal.netty.encoding.ApnsResultEncoder;

public class MockChannel extends EmbeddedChannel {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(MockChannel.class);

    private static final int MAX_PAYLOAD_SIZE = 256;
    private static final ApnsResultEncoder ENCODER = new ApnsResultEncoder();

    private final int failureAt;
    private final DeliveryError error;

    public MockChannel(int failureAt, DeliveryError error,
            ChannelHandler... handlers) {
        super(handlers);
        this.error = error;
        this.failureAt = failureAt;
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg) {
        ApnsNotification notification = null;

        if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            notification = decodeApnsNotification(buf);
        }

        if (msg instanceof ApnsNotification) {
            notification = (ApnsNotification) msg;
        }
        
        ChannelFuture future = super.writeAndFlush(msg);

        if (notification != null) {
            LOGGER.debug("Received message {}", notification);
            if (notification.getIdentifier() == failureAt) {
                ByteBuf buf = alloc().buffer(6);
                buf.retain();
                try {
                    ENCODER.encode(null, new DeliveryResult(error, failureAt),
                            buf);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                writeInbound(buf);
                close();
                buf.release();
            }
        }

        return future;
    }

    private static ApnsNotification decodeApnsNotification(ByteBuf buf) {
        // OPCODE
        final byte opcode = buf.readByte();

        if (opcode != 1) {
            throw new RuntimeException("Invalid opcode " + opcode);
        }

        // SEQUENCE_NUMBER:
        final int sequenceNumber = buf.readInt();

        // EXPIRATION
        final long timestamp = (buf.readInt() & 0xFFFFFFFFL) * 1000L;
        final Date expiration = new Date(timestamp);

        // TOKEN_LENGTH
        final byte[] token = new byte[buf.readShort() & 0x0000FFFF];

        if (token.length == 0) {
            throw new RuntimeException("Invalid token length");
        }

        // TOKEN
        buf.readBytes(token);

        // PAYLOAD_LENGTH
        final int payloadSize = buf.readShort() & 0x0000FFFF;

        if (payloadSize > MAX_PAYLOAD_SIZE || payloadSize == 0) {
            throw new RuntimeException("Invalid payload size " + payloadSize);
        }

        final byte[] payloadBytes = new byte[payloadSize];

        // PAYLOAD
        buf.readBytes(payloadBytes);

        return new EnhancedApnsNotification(sequenceNumber,
                (int) (expiration.getTime() / 1000), token, payloadBytes);

    }
}
