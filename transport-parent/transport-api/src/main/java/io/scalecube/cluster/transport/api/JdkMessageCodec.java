package io.scalecube.cluster.transport.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

public class JdkMessageCodec implements MessageCodec {

  @Override
  public Message deserialize(InputStream is) throws IOException, ClassNotFoundException {
    Message message = new Message();
    try (ObjectInputStream inputStream = new ObjectInputStream(is)) {
      message.readExternal(inputStream);
      return message;
    }
  }

  @Override
  public void serialize(Message message, OutputStream os) throws IOException {
    try (ObjectOutputStream outputStream = new ObjectOutputStream(os)) {
      message.writeExternal(outputStream);
      outputStream.flush();
    }
  }
}
