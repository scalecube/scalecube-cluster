package io.scalecube.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/** Contains static methods for message serializing/deserializing logic. */
public final class MessageCodec {

  private static final RecyclableLinkedBuffer recyclableLinkedBuffer = new RecyclableLinkedBuffer();

  static {
    // Register message schema
    if (!RuntimeSchema.isRegistered(Message.class)) {
      RuntimeSchema.register(Message.class, new MessageSchema());
    }
  }

  private MessageCodec() {
    // Do not instantiate
  }

  /**
   * Deserializes message from given byte buffer.
   *
   * @param bb byte buffer
   * @return message from ByteBuf
   */
  public static Message deserialize(ByteBuf bb) {
    Schema<Message> schema = RuntimeSchema.getSchema(Message.class);
    Message message = schema.newMessage();
    try {
      ProtostuffIOUtil.mergeFrom(new ByteBufInputStream(bb), message, schema);
    } catch (Exception e) {
      throw new DecoderException(e.getMessage(), e);
    }

    return message;
  }

  /**
   * Serializes given message into byte buffer.
   *
   * @param message message to serialize
   * @return message as ByteBuf
   */
  public static ByteBuf serialize(Message message) {
    ByteBuf bb = ByteBufAllocator.DEFAULT.buffer();
    Schema<Message> schema = RuntimeSchema.getSchema(Message.class);
    try (RecyclableLinkedBuffer rlb = recyclableLinkedBuffer.get()) {
      try {
        ProtostuffIOUtil.writeTo(new ByteBufOutputStream(bb), message, schema, rlb.buffer());
        return bb;
      } catch (Exception e) {
        bb.release();
        throw new EncoderException(e.getMessage(), e);
      }
    }
  }
}
