package io.scalecube.cluster.codec.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.scalecube.cluster.metadata.MetadataCodec;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import reactor.core.Exceptions;

/**
 * Jackson-based metadata codec.
 *
 * @author eutkin
 */
public class JacksonMetadataCodec implements MetadataCodec {

  private final ObjectMapper delegate;

  public JacksonMetadataCodec(ObjectMapper delegate) {
    this.delegate = delegate;
  }

  public JacksonMetadataCodec() {
    this.delegate = DefaultObjectMapper.OBJECT_MAPPER;
  }

  @Override
  public Object deserialize(ByteBuffer buffer, Type type) {
    if (buffer.remaining() == 0) {
      return null;
    }
    try {
      return this.delegate.readValue(
          buffer.array(), this.delegate.getTypeFactory().constructType(type));
    } catch (IOException e) {
      throw Exceptions.propagate(e);
    }
  }

  @Override
  public ByteBuffer serialize(Object metadata) {
    if (metadata == null) {
      return null;
    }
    try {
      return ByteBuffer.wrap(this.delegate.writeValueAsBytes(metadata));
    } catch (IOException e) {
      throw Exceptions.propagate(e);
    }
  }
}
