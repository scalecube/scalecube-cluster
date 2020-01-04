package io.scalecube.cluster.codec.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.MessageCodec;

import java.io.InputStream;
import java.io.OutputStream;

/** Contains methods for message serializing/deserializing logic. */
public class JacksonMessageCodec implements MessageCodec {

  private final ObjectMapper delegate;

  /**
   * Create instance with external {@link ObjectMapper}.
   *
   * @param delegate jackson object mapper
   */
  public JacksonMessageCodec(ObjectMapper delegate) {
    this.delegate = delegate;
  }

  /**
   * Create instance with default {@link ObjectMapper}.
   */
  public JacksonMessageCodec() {
    this.delegate = DefaultObjectMapper.OBJECT_MAPPER;
  }

  /**
   * Deserializes message from given input stream.
   *
   * @param stream input stream
   * @return message from the input stream
   */
  @Override
  public Message deserialize(InputStream stream) throws Exception {
    return this.delegate.readValue(stream, Message.class);
  }

  /**
   * Serializes given message into given output stream.
   *
   * @param message message
   * @param stream output stream
   */
  @Override
  public void serialize(Message message, OutputStream stream) throws Exception {
    this.delegate.writeValue(stream, message);
  }
}
