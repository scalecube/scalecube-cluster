package io.scalecube.cluster.codec.jackson.smile;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.MessageCodec;
import java.io.InputStream;
import java.io.OutputStream;

/** JacksonSmile based message codec. */
public class JacksonSmileMessageCodec implements MessageCodec {

  private final ObjectMapper delegate;

  /** Default constructor. */
  public JacksonSmileMessageCodec() {
    this(DefaultObjectMapper.OBJECT_MAPPER);
  }

  /**
   * Create instance with external {@link ObjectMapper}.
   *
   * @param delegate jackson object mapper
   */
  public JacksonSmileMessageCodec(ObjectMapper delegate) {
    this.delegate = delegate;
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
