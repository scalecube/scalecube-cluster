package io.scalecube.cluster.codec.jackson.smile;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.MessageCodec;
import java.io.InputStream;
import java.io.OutputStream;

public class JacksonSmileMessageCodec implements MessageCodec {

  private final ObjectMapper delegate;

  public JacksonSmileMessageCodec() {
    this(DefaultObjectMapper.OBJECT_MAPPER);
  }

  public JacksonSmileMessageCodec(ObjectMapper delegate) {
    this.delegate = delegate;
  }

  @Override
  public Message deserialize(InputStream stream) throws Exception {
    return this.delegate.readValue(stream, Message.class);
  }

  @Override
  public void serialize(Message message, OutputStream stream) throws Exception {
    stream.write(this.delegate.writeValueAsBytes(message));
  }
}
