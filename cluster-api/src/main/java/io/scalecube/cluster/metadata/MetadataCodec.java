package io.scalecube.cluster.metadata;

import io.scalecube.utils.ServiceLoaderUtil;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;

/** Contains methods for metadata serializing/deserializing logic. */
public interface MetadataCodec {

  MetadataCodec INSTANCE = ServiceLoaderUtil.findFirst(MetadataCodec.class).orElse(null);

  /**
   * Deserializes metadata from buffer.
   *
   * @param buffer metadata buffer; if {@code buffer} is empty then returned result shall be null.
   * @param type metadata object type
   * @return metadata object from metadata buffer or null
   */
  Object deserialize(ByteBuffer buffer, Type type);

  /**
   * Serializes given metadata into buffer.
   *
   * @param metadata metadata object (optional); if {@code metadata} is null then returned result
   *     may be null or empty buffer.
   * @return buffer or null
   */
  ByteBuffer serialize(Object metadata);
}
