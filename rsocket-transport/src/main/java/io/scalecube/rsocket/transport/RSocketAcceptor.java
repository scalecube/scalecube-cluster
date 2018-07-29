package io.scalecube.rsocket.transport;

import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

public class RSocketAcceptor implements SocketAcceptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketAcceptor.class);

  private final FluxSink<Message> sink;

  public RSocketAcceptor(DirectProcessor<Message> listen) {
    this.sink = listen.serialize().sink();
  }

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket rSocket) {
    LOGGER.info("Accepted rSocket: {}, connectionSetup: {}", rSocket, setup);

    return Mono.just(new AbstractRSocket() {
      @Override
      public Mono<Void> fireAndForget(Payload payload) {
        return Mono.fromRunnable(() -> {
          Message message = MessageCodec.deserialize(payload.sliceData());
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Received: {}", message);
          }
          sink.next(message);
        });
      }
    });
  }
}
