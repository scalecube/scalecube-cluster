package io.scalecube.cluster.metadata;

import io.scalecube.utils.ServiceLoaderUtil;
import java.nio.ByteBuffer;

@Deprecated
@FunctionalInterface
public interface MetadataDecoder {

  MetadataDecoder INSTANCE = ServiceLoaderUtil.findFirst(MetadataDecoder.class).orElse(null);

  Object decode(ByteBuffer buffer);
}
