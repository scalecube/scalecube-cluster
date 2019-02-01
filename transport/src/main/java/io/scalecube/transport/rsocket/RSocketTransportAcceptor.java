package io.scalecube.transport.rsocket;

import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.scalecube.transport.Message;
import io.scalecube.transport.MessageCodec;
import java.util.Map;
import java.util.function.Consumer;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

public class RSocketTransportAcceptor implements SocketAcceptor {

  private final FluxSink<Message> sink;
  private final Map<String, Consumer<Message>> serverMessageListeners;
  private final MessageCodec messageCodec;

  public RSocketTransportAcceptor(
      FluxSink<Message> sink,
      Map<String, Consumer<Message>> serverMessageListeners,
      MessageCodec messageCodec) {

    this.sink = sink;
    this.serverMessageListeners = serverMessageListeners;
    this.messageCodec = messageCodec;
  }

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
