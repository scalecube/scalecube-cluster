package io.scalecube.cluster.metadata;

import java.nio.ByteBuffer;

@FunctionalInterface
public interface MetadataDecoder {

  <T> T decode(ByteBuffer buffer);
}
