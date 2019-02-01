package io.scalecube.transport.rsocket;

import io.rsocket.RSocket;
import io.scalecube.transport.Message;
import io.scalecube.transport.MessageCodec;
import io.scalecube.transport.Responder;


public class RSocketResponder implements Responder {

  private final RSocket sendingSocket;
  private final MessageCodec codec;

  public RSocketResponder(RSocket sendingSocket, MessageCodec codec) {
    this.sendingSocket = sendingSocket;
    this.codec = codec;
  }

  @Override
  public void writeBack(Message response) {
    // We want to respond optionally,  that's why on both sides we use fireAndForget
    sendingSocket.fireAndForget(codec.toPayload(response));
  }
}
