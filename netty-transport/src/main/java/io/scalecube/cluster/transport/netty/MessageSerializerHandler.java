package io.scalecube.cluster.transport.netty;

import io.scalecube.cluster.transport.Message;
import io.scalecube.cluster.transport.MessageCodec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

@ChannelHandler.Sharable
public final class MessageSerializerHandler extends MessageToByteEncoder<Message> {

  @Override
  protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) throws Exception {
    MessageCodec.serialize(msg, out);
  }

}
