package io.scalecube.transport;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Contains methods for message serializing/deserializing logic.
 */
public interface MessageCodec {

  /**
   * Deserializes message from given input stream.
   *
   * @param stream input stream
   *
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
