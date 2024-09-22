package io.scalecube.cluster.metadata;

import java.nio.ByteBuffer;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

/** Contains methods for metadata serializing/deserializing logic. */
public interface MetadataCodec {

  MetadataCodec INSTANCE =
      StreamSupport.stream(ServiceLoader.load(MetadataCodec.class).spliterator(), false)
          .findFirst()
          .orElse(null);

  /**
   * Deserializes metadata from buffer.
   *
   * @param buffer metadata buffer; if {@code buffer} is empty then returned result shall be null.
   * @return metadata object from metadata buffer or null
   */
  Object deserialize(ByteBuffer buffer);

  /**
   * Serializes given metadata into buffer.
   *
   * @param metadata metadata object (optional); if {@code metadata} is null then returned result
   *     may be null or empty buffer.
   * @return buffer or null
   */
  ByteBuffer serialize(Object metadata);
}
