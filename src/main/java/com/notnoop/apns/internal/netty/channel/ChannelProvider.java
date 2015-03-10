package com.notnoop.apns.internal.netty.channel;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;

import java.io.IOException;
import java.util.List;

/**
 * This class separates the channel handling/reconnecting from the actual
 * app-level logic of APNS.
 * 
 * @author flozano
 * 
 */
public interface ChannelProvider {

    /**
     * Closes the channel provider, it implies to close current channel and stop
     * opening new ones. Any call to runWithChannel after calling this method
     * will result in a ChannelProviderClosedException
     * 
     * @throws IOException
     */
    void close() throws IOException;

    /**
     * Closes the provided channel
     * 
     * @param channel
     */
    void closeChannel(Channel channel) throws IOException;

    /**
     * Executes the provided action with the current channel, reconnecting if
     * needed
     * 
     * @param action
     * @throws Exception
     */
    void runWithChannel(WithChannelAction action) throws Exception;

    public static interface WithChannelAction {

        void perform(Channel channel) throws Exception;

    }

    /**
     * Sets the provider for the list of handlers that will apply to the
     * channels used by this channel provider.
     * 
     * (Yes, too many ****Provider...)
     * 
     * @param channelHandlersProvider
     */
    void setChannelHandlersProvider(
            ChannelHandlersProvider channelHandlersProvider);

    /**
     * Channel closed listener
     * 
     * @param listener
     */
    void setChannelClosedListener(ChannelClosedListener listener);

    /**
     * Initialize this channel provider. Like above - it's code smell, to be
     * removed if possible in the future.
     */
    void init();

    public static interface ChannelHandlersProvider {
        List<ChannelHandler> getChannelHandlers();
    }

    public interface ChannelClosedListener {

        void onChannelClosed(Channel ch);

    }

}
