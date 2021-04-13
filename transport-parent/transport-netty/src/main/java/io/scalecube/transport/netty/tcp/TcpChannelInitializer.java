package io.scalecube.transport.netty.tcp;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import java.util.function.BiConsumer;
import reactor.netty.ConnectionObserver;

final class TcpChannelInitializer implements BiConsumer<ConnectionObserver, Channel> {

  private static final int LENGTH_FIELD_LENGTH = 4;

  private final int maxFrameLength;

  TcpChannelInitializer(int maxFrameLength) {
    this.maxFrameLength = maxFrameLength;
  }

  @Override
  public void accept(ConnectionObserver connectionObserver, Channel channel) {
    ChannelPipeline pipeline = channel.pipeline();
    pipeline.addFirst(
        new LengthFieldBasedFrameDecoder(
            maxFrameLength, 0, LENGTH_FIELD_LENGTH, 0, LENGTH_FIELD_LENGTH));
    pipeline.addFirst(new LengthFieldPrepender(LENGTH_FIELD_LENGTH));
  }
}
