package io.scalecube.cluster.metadata;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Map;
import reactor.core.Exceptions;

public class MapJacksonMetadataCodec implements MetadataCodec<Map<String, String>> {

  public static final ByteBuffer EMPTY = ByteBuffer.wrap(new byte[0]);

  private static final ObjectMapper mapper = initMapper();

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

  private static ObjectMapper initMapper() {
    ObjectMapper mapper = new ObjectMapper();
    //    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    //    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    //    mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
    //    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    //    mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
    //    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    //    mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
    //    mapper.enableDefaultTyping(DefaultTyping.JAVA_LANG_OBJECT, JsonTypeInfo.As.PROPERTY);
    return mapper;
  }
}
