package io.scalecube.cluster.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import io.scalecube.cluster.utils.DefaultObjectMapper;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import reactor.core.Exceptions;

public final class SimpleMapMetadataCodec implements MetadataEncoder, MetadataDecoder {

  public static final SimpleMapMetadataCodec INSTANCE = new SimpleMapMetadataCodec();

  private static final TypeReference TYPE = new TypeReference<Map<String, String>>() {};

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, String> decode(ByteBuffer buffer) {
    try {
      if (buffer.remaining() == 0) {
        return Collections.emptyMap();
      }
      return DefaultObjectMapper.OBJECT_MAPPER.readValue(buffer.array(), TYPE);
    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
  }

  @Override
  public ByteBuffer encode(Object metadata) {
    try {
      return ByteBuffer.wrap(DefaultObjectMapper.OBJECT_MAPPER.writeValueAsBytes(metadata));
    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
  }
}
