package io.scalecube.cluster.transport.api2;

import org.agrona.concurrent.MessageHandler;

public interface Transport extends AutoCloseable {

  void send(String address, byte[] bytes, int offset, int length);

  MessagePoller newMessagePoller();

  interface MessagePoller {

    int poll(MessageHandler messageHandler);
  }
}
