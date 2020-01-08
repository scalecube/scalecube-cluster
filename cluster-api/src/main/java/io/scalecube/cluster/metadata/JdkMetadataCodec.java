package io.scalecube.cluster.metadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import reactor.core.Exceptions;

public class JdkMetadataCodec implements MetadataCodec {

  @Override
  public Object deserialize(ByteBuffer buffer) {
    byte[] bytes = buffer.array();
    try (ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      return is.readObject();
    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
  }

  @Override
  public ByteBuffer serialize(Object metadata) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream os = new ObjectOutputStream(baos)) {
      os.writeObject(metadata);
      os.flush();
      return ByteBuffer.wrap(baos.toByteArray());
    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
  }
}
