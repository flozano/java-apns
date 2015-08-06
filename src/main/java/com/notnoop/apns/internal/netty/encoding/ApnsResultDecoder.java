package com.notnoop.apns.internal.netty.encoding;

import java.io.IOException;

import com.notnoop.apns.DeliveryError;
import com.notnoop.apns.DeliveryResult;
import com.notnoop.apns.internal.Utilities;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.FixedLengthFrameDecoder;

public class ApnsResultDecoder extends FixedLengthFrameDecoder {
    private static final int FRAME_LENGTH = 6;

    public ApnsResultDecoder() {
        super(FRAME_LENGTH);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in)
            throws Exception {
        //System.out.println("Receive " + in.readableBytes());
        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        if (frame == null) {
            return null;
        }

        byte[] bytes = new byte[FRAME_LENGTH];
        frame.readBytes(bytes).release();

        int command = bytes[0] & 0xFF;
        if (command != 8) {
            throw new IOException("Unexpected command byte " + command);
        }

        int statusCode = bytes[1] & 0xFF;
        DeliveryError e = DeliveryError.ofCode(statusCode);

        int id = Utilities.parseBytes(bytes[2], bytes[3], bytes[4], bytes[5]);
        return new DeliveryResult(e, id);
    }
}
