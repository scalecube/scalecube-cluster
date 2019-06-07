package io.scalecube.cluster.transport;

import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.MessageCodec;
import io.scalecube.cluster.utils.DefaultObjectMapper;
import java.io.InputStream;
import java.io.OutputStream;

/** Contains methods for message serializing/deserializing logic. */
public final class JacksonMessageCodec implements MessageCodec {

  /**
   * Deserializes message from given input stream.
   *
   * @param stream input stream
   * @return message from the input stream
   */
  @Override
  public Message deserialize(InputStream stream) throws Exception {
    return DefaultObjectMapper.OBJECT_MAPPER.readValue(stream, Message.class);
  }

  /**
   * Serializes given message into given output stream.
   *
   * @param message message
   * @param stream output stream
   */
  @Override
  public void serialize(Message message, OutputStream stream) throws Exception {
    DefaultObjectMapper.OBJECT_MAPPER.writeValue(stream, message);
  }
}
