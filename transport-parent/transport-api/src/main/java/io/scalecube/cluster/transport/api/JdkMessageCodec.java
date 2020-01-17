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
    try (ObjectInputStream ois = new ObjectInputStream(is)) {
      message.readExternal(ois);
    }
    return message;
  }

  @Override
  public void serialize(Message message, OutputStream os) throws IOException {
    try (ObjectOutputStream oos = new ObjectOutputStream(os)) {
      message.writeExternal(oos);
      oos.flush();
    }
  }
}
