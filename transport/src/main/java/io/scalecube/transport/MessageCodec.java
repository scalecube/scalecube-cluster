package io.scalecube.transport;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.InputStream;
import java.io.OutputStream;

/** Contains static methods for message serializing/deserializing logic. */
public final class MessageCodec {

  private static final ObjectMapper mapper = initMapper();

  /**
   * Deserializes message from given input stream.
   *
   * @param stream input stream
   * @return message from the input stream
   */
  public static Message deserialize(InputStream stream) throws Exception {
    return mapper.readValue(stream, Message.class);
  }

  /**
   * Serializes given message into given output stream.
   *
   * @param message message
   * @param stream output stream
   */
  public static void serialize(Message message, OutputStream stream) throws Exception {
    mapper.writeValue(stream, message);
  }

  private static ObjectMapper initMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
    mapper.enableDefaultTyping(DefaultTyping.JAVA_LANG_OBJECT, JsonTypeInfo.As.PROPERTY);
    return mapper;
  }
}
