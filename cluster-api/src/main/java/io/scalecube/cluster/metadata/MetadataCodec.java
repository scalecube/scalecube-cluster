package io.scalecube.cluster.metadata;

import io.scalecube.utils.ServiceLoaderUtil;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import reactor.util.annotation.Nullable;

/** Contains methods for metadata serializing/deserializing logic. */
public interface MetadataCodec {

  MetadataCodec INSTANCE = ServiceLoaderUtil.findFirst(MetadataCodec.class).orElse(null);

  /**
   * Deserializes metadata from buffer.
   *
   * @param buffer metadata buffer
   * @param type metadata object type
   * @return metadata object from metadata buffer
   */
  Object deserialize(ByteBuffer buffer, Type type) throws Exception;

  /**
   * Serializes given metadata into buffer.
   *
   * @param metadata metadata object; optional parameter
   * @return buffer
   */
  ByteBuffer serialize(@Nullable Object metadata) throws Exception;
}
