package io.scalecube.cluster.metadata;

import java.nio.ByteBuffer;

@FunctionalInterface
public interface MetadataEncoder {

  ByteBuffer encode(Object metadata);
}
