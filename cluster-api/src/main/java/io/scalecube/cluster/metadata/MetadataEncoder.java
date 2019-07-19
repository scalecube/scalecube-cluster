package io.scalecube.cluster.metadata;

import io.scalecube.utils.ServiceLoaderUtil;
import java.nio.ByteBuffer;
import reactor.util.annotation.Nullable;

@FunctionalInterface
public interface MetadataEncoder {

  MetadataEncoder INSTANCE = ServiceLoaderUtil.findFirst(MetadataEncoder.class).orElse(null);

  ByteBuffer encode(@Nullable Object metadata);
}
