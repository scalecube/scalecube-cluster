package io.scalecube.cluster.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import reactor.core.Exceptions;

public class SimpleMapMetadataCodec implements MetadataEncoder, MetadataDecoder {

  public static final SimpleMapMetadataCodec INSTANCE = new SimpleMapMetadataCodec();

  private static final ObjectMapper mapper = new ObjectMapper();

  private static final TypeReference TYPE = new TypeReference<Map<String, String>>() {};

  @Override
  public <T> T decode(ByteBuffer buffer) {
    try {
      if (buffer.remaining() == 0) {
        return (T) Collections.emptyMap();
      }
      return mapper.readValue(buffer.array(), TYPE);
    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
  }

  @Override
  public ByteBuffer encode(Object metadata) {
    try {
      byte[] bytes = mapper.writeValueAsBytes(metadata);
      return ByteBuffer.wrap(bytes);
    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
  }
}
