package io.scalecube.cluster.metadata;

import java.nio.ByteBuffer;

public interface MetadataCodec<T> {

  T deserialize(ByteBuffer buffer);

  ByteBuffer serialize(T metadata);
}
