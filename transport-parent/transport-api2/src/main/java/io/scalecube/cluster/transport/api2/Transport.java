package io.scalecube.cluster.transport.api2;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.MessageHandler;

public interface Transport extends AutoCloseable {

  void send(String address, DirectBuffer buffer, int offset, int length);

  MessagePoller newMessagePoller();

  interface MessagePoller {

    int poll(MessageHandler messageHandler);
  }
}
