package io.scalecube.transport.rsocket;

import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.scalecube.transport.Message;
import io.scalecube.transport.MessageCodec;
import java.util.Map;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

public class RSocketTransportAcceptor implements SocketAcceptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketTransportAcceptor.class);
  private final FluxSink<Message> exposeIncomingMessages;
  private final Map<String, Function<Message, Mono<Message>>> serverMessageListeners;
  private final MessageCodec messageCodec;

  public RSocketTransportAcceptor(
    FluxSink<Message> exposeIncomingMessages,
    Map<String, Function<Message, Mono<Message>>> serverMessageListeners,
    MessageCodec messageCodec) {

    this.exposeIncomingMessages = exposeIncomingMessages;
    this.serverMessageListeners = serverMessageListeners;
    this.messageCodec = messageCodec;
  }

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
    return Mono.just(
      new Handler(exposeIncomingMessages, serverMessageListeners, messageCodec));
  }

  private static class Handler extends AbstractRSocket {

    private final MessageCodec codec;
    private final Function<Payload, Mono<Message>> requestHandler;

    public Handler(FluxSink<Message> exposeIncomingMessages,
      Map<String, Function<Message, Mono<Message>>> serverMessageListeners,
      MessageCodec codec) {
      this.codec = codec;
      this.requestHandler = payload ->
        Mono
          .fromCallable(() -> codec.toMessage(payload))
          .doOnNext(next -> LOGGER.debug("Rcv: {}", next))
          .flatMap(
            message -> {
              exposeIncomingMessages.next(message);
              return serverMessageListeners
                .getOrDefault(message.qualifier(), req -> Mono.empty())
                .apply(message);
            })
          .doOnNext(next -> LOGGER.debug("Snd: {}", next));
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      // Received message from remote node:
      // 1. deserialize
      // 2. pass to message handler earlier registered
      // 3. post to exposeIncomingMessages (to expose to legacy "listen()")
      return requestHandler
        .apply(payload)
        .map(codec::toPayload);
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
      return requestHandler
        .apply(payload)
        .then();
    }
  }
}
