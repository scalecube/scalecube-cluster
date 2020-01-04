package io.scalecube.cluster.codec.jackson;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.scalecube.cluster.metadata.MetadataDecoder;
import io.scalecube.cluster.metadata.MetadataEncoder;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import reactor.core.Exceptions;

/**
 * Jackson-based metadata encoder/decoder.
 *
 * @author eutkin
 */
public class JacksonMetadataEncoder implements MetadataEncoder, MetadataDecoder {

  private static final TypeReference<Map<String, String>> DEFAULT_TYPE =
      new TypeReference<Map<String, String>>() {};

  private final ObjectMapper delegate;
  private final TypeReference<?> type;

  /**
   * Create instance with external {@link ObjectMapper}.
   *
   * @param delegate jackson object mapper
   * @param type type of metadata
   */
  public JacksonMetadataEncoder(ObjectMapper delegate, TypeReference<?> type) {
    this.delegate = delegate;
    this.type = type;
  }

  /**
   * Create instance with default {@link ObjectMapper}.
   */
  public JacksonMetadataEncoder() {
    this.delegate = DefaultObjectMapper.OBJECT_MAPPER;
    this.type = DEFAULT_TYPE;
  }

  /**
   * Deserialize metadata as {@link Map Map&lt;String, String&gt;}.
   *
   * @param buffer binary metadata
   * @return medata as map
   */
  @Override
  public Object decode(ByteBuffer buffer) {
    try {
      if (buffer.remaining() == 0) {
        return Collections.emptyMap();
      }
      return this.delegate.readValue(buffer.array(), this.type);
    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
  }

  /**
   * Serialize metadata to binary format.
   *
   * @param metadata metadata
   * @return binary metadata
   */
  @Override
  public ByteBuffer encode(Object metadata) {
    try {
      return ByteBuffer.wrap(this.delegate.writerFor(this.type).writeValueAsBytes(metadata));
    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
  }
}
