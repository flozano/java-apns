package com.notnoop.apns.internal.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.notnoop.apns.DeliveryResult;

public class ApnsHandler extends SimpleChannelInboundHandler<DeliveryResult> {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(ApnsHandler.class);

	private final DeliveryResultListener listener;

	public ApnsHandler(DeliveryResultListener listener) {
		super(DeliveryResult.class);
		this.listener = listener;
	}

	@Override
	public void channelRead0(ChannelHandlerContext ctx, DeliveryResult msg)
			throws Exception {
		LOGGER.debug("Received message: {}", msg);
		listener.onDeliveryResult(ctx, msg);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
			throws Exception {
		LOGGER.warn(
				"Exception while listening for result: " + cause.getMessage(),
				cause);
	}

}
