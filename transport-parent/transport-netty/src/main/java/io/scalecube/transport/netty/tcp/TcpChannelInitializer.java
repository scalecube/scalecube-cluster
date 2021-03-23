package io.scalecube.transport.netty.tcp;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    pipeline.addLast(new ExceptionHandler());
  }

  @ChannelHandler.Sharable
  static final class ExceptionHandler extends ChannelDuplexHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionHandler.class);

    @Override
    public final void exceptionCaught(ChannelHandlerContext ctx, Throwable ex) {
      LOGGER.debug("Exception caught on channel {}, cause: {}", ctx.channel(), ex.toString());
    }
  }
}
