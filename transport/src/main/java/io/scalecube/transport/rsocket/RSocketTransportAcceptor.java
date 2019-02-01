package io.scalecube.transport.rsocket;

import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.scalecube.transport.Message;
import io.scalecube.transport.MessageCodec;
import io.scalecube.transport.Responder;
import java.util.Map;
import java.util.function.BiConsumer;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

public class RSocketTransportAcceptor implements SocketAcceptor {

  private final FluxSink<Message> exposeIncomingMessages;
  private final Map<String, BiConsumer<Message, Responder>> serverMessageListeners;
  private final MessageCodec messageCodec;

  public RSocketTransportAcceptor(
    FluxSink<Message> exposeIncomingMessages,
    Map<String, BiConsumer<Message, Responder>> serverMessageListeners,
    MessageCodec messageCodec) {

    this.exposeIncomingMessages = exposeIncomingMessages;
    this.serverMessageListeners = serverMessageListeners;
    this.messageCodec = messageCodec;
  }

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
    return Mono.just(
      new Handler(exposeIncomingMessages, serverMessageListeners, messageCodec, sendingSocket));
  }

  private static class Handler extends AbstractRSocket {

    private final FluxSink<Message> exposeIncomingMessages;
    private final Map<String, BiConsumer<Message, Responder>> serverMessageListeners;
    private final MessageCodec codec;
    private final RSocketResponder responder;

    public Handler(FluxSink<Message> exposeIncomingMessages,
      Map<String, BiConsumer<Message, Responder>> serverMessageListeners,
      MessageCodec codec, RSocket sendingSocket) {
      this.exposeIncomingMessages = exposeIncomingMessages;
      this.serverMessageListeners = serverMessageListeners;
      this.codec = codec;
      this.responder = new RSocketResponder(sendingSocket, codec);
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
      // Received message from remote node:
      // 1. deserialize it
      // 2. pass it to message handler earlier registered
      // 3. post it to exposeIncomingMessages (to expose to legacy "listen()")
      return Mono
        .fromCallable(() -> codec.toMessage(payload))
        .flatMap(
          message -> {
            exposeIncomingMessages.next(message);
            BiConsumer<Message, Responder> handler = serverMessageListeners
              .getOrDefault(message.qualifier(), (msg, resp) -> {
              });
            return Mono.fromRunnable(() -> handler.accept(message, responder));
          });

    }
  }
}
