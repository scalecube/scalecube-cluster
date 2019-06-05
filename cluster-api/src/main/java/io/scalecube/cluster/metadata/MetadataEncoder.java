package io.scalecube.cluster.metadata;

import io.scalecube.utils.ServiceLoaderUtil;
import java.nio.ByteBuffer;

@FunctionalInterface
public interface MetadataEncoder {

  MetadataEncoder INSTANCE = ServiceLoaderUtil.findFirst(MetadataEncoder.class).orElse(null);

  ByteBuffer encode(Object metadata);
}
