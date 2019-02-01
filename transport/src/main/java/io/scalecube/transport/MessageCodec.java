package io.scalecube.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;
import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Contains methods for message serializing/deserializing logic.
 */
public interface MessageCodec {

  /**
   * Deserializes message from given input stream.
   *
   * @param stream input stream
   * @return message from the input stream
   */
  Message deserialize(InputStream stream) throws Exception;

  /**
   * Serializes given message into given output stream.
   *
   * @param message message
   * @param stream output stream
   */
  void serialize(Message message, OutputStream stream) throws Exception;

  default Message toMessage(Payload payload) {
    try (ByteBufInputStream stream = new ByteBufInputStream(payload.sliceData(), true)) {
      return deserialize(stream);
    } catch (Exception e) {
      throw new DecoderException(e);
    }
  }

  default Payload toPayload(Message message) {
    ByteBuf bb = ByteBufAllocator.DEFAULT.buffer();
    ByteBufOutputStream stream = new ByteBufOutputStream(bb);
    try {
      serialize(message, stream);
    } catch (Exception e) {
      bb.release();
      throw new EncoderException(e);
    }
    return ByteBufPayload.create(bb);
  }
}
