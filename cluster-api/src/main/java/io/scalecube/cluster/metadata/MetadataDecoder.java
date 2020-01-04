package io.scalecube.cluster.metadata;

import io.scalecube.utils.ServiceLoaderUtil;
import java.nio.ByteBuffer;

/**
 * MetadataDecoder. <br>
 * Deprecated since {@code 2.4.10} in favor of {@link MetadataCodec}.
 */
@Deprecated
@FunctionalInterface
public interface MetadataDecoder {

  MetadataDecoder INSTANCE = ServiceLoaderUtil.findFirst(MetadataDecoder.class).orElse(null);

  Object decode(ByteBuffer buffer);
}
