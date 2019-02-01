package io.scalecube.transport;

/**
 * Class to be passed to server's message handler in order to provide ability to optionally respond
 * the inbound message.
 */
public interface Responder {

  /**
   * Write back the message to remote node
   *
   * @param response response to be written
   */
  void writeBack(Message response);

}
