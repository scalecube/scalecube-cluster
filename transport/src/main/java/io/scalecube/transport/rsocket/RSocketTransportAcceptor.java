package io.scalecube.transport.rsocket;

import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import reactor.core.publisher.Mono;

public class RSocketTransportAcceptor implements SocketAcceptor {

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
    return Mono.just(
        new AbstractRSocket() {
          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            // TODO: implement transport server behavior:
            // 1. deserialize message
            // 2. pass message to message handler earlier registered
            // 3. post it to the legacy "listen()" sink
            return super.requestResponse(payload);
          }
        });
  }
}
