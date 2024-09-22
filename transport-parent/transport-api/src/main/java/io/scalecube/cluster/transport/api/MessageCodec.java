package io.scalecube.cluster.transport.api;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

/** Contains methods for message serializing/deserializing logic. */
public interface MessageCodec {

  MessageCodec INSTANCE =
      StreamSupport.stream(ServiceLoader.load(MessageCodec.class).spliterator(), false)
          .findFirst()
          .orElseGet(JdkMessageCodec::new);

  /**
   * Deserializes message from given input stream.
   *
   * @param stream input stream
   * @return message from the input stream
   */
  Message deserialize(InputStream stream) throws Exception;

  /**
   * Serializes given message into given output stream.
   *
   * @param message message
   * @param stream output stream
   */
  void serialize(Message message, OutputStream stream) throws Exception;
}
