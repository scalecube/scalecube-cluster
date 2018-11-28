package io.scalecube.transport;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;
import java.io.InputStream;
import java.io.OutputStream;

/** Contains static methods for message serializing/deserializing logic. */
public final class MessageCodec {

  private static final ObjectMapper mapper = initMapper();

  /**
   * Deserializes message from given byte buffer.
   *
   * @param bb byte buffer
   * @return message from ByteBuf
   */
  public static Message deserialize(ByteBuf bb) {
    try (ByteBufInputStream stream = new ByteBufInputStream(bb, true)) {
      return mapper.readValue((InputStream) stream, Message.class);
    } catch (Exception e) {
      //      ReferenceCountUtil.safeRelease(bb);//todo ?
      throw new DecoderException(e.getMessage(), e);
    }
  }

  /**
   * Serializes given message into byte buffer.
   *
   * @param message message to serialize
   * @return message as ByteBuf
   */
  public static ByteBuf serialize(Message message) {
    ByteBuf bb = ByteBufAllocator.DEFAULT.buffer();
    ByteBufOutputStream stream = new ByteBufOutputStream(bb);
    try {
      mapper.writeValue((OutputStream) stream, message);
    } catch (Exception e) {
      bb.release();
      throw new EncoderException(e.getMessage(), e);
    }
    return bb;
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
    return mapper;
  }
}
