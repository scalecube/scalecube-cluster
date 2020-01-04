package io.scalecube.cluster.metadata;

import io.scalecube.utils.ServiceLoaderUtil;
import java.nio.ByteBuffer;
import reactor.util.annotation.Nullable;

/**
 * MetadataEncoder. <br>
 * Deprecated since {@code 2.4.10} in favor of {@link MetadataCodec}.
 */
@Deprecated
@FunctionalInterface
public interface MetadataEncoder {

  MetadataEncoder INSTANCE = ServiceLoaderUtil.findFirst(MetadataEncoder.class).orElse(null);

  ByteBuffer encode(@Nullable Object metadata);
}
