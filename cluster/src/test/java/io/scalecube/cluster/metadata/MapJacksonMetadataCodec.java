package io.scalecube.cluster.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.util.Map;
import reactor.core.Exceptions;

public class MapJacksonMetadataCodec implements MetadataCodec<Map<String, String>> {

  private static final ObjectMapper mapper = new ObjectMapper();

  private static final TypeReference TYPE = new TypeReference<Map<String, String>>() {};

  @Override
  public Map<String, String> deserialize(ByteBuffer buffer) {
    try {
      return mapper.readValue(buffer.array(), TYPE);
    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
  }

  @Override
  public ByteBuffer serialize(Map<String, String> metadata) {
    try {
      byte[] bytes = mapper.writeValueAsBytes(metadata);
      return ByteBuffer.wrap(bytes);
    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
  }
}
