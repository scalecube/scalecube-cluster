package io.scalecube.transport.websocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;
import io.netty.util.ReferenceCountUtil;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.MessageCodec;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.net.Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;
import reactor.netty.http.websocket.WebsocketInbound;
import reactor.netty.http.websocket.WebsocketOutbound;
import reactor.netty.resources.LoopResources;

public final class TransportImpl implements Transport {

  public static void main(String[] args) throws InterruptedException {
    Transport block = TransportImpl.bind().block();
    Thread.currentThread().join();
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Transport.class);

  private final TransportConfig config;
  private final LoopResources loopResources;

  // Subject
  private final DirectProcessor<Message> messagesSubject;
  private final FluxSink<Message> messageSink;

  private final Map<Address, Mono<? extends Connection>> connections;

  // Close handler
  private final MonoProcessor<Void> stop;
  private final MonoProcessor<Void> onStop;

  // Server
  private final Address address;
  private final DisposableServer server;

  // Message codec
  private final MessageCodec messageCodec;

  /**
   * Constructor with cofig as parameter.
   *
   * @param config transport configuration
   */
  public TransportImpl(TransportConfig config) {
    this.config = config;
    this.loopResources = LoopResources.create("sc-cluster-io", 1, true);
    this.messagesSubject = DirectProcessor.create();
    this.messageSink = messagesSubject.sink();
    this.connections = new ConcurrentHashMap<>();
    this.stop = MonoProcessor.create();
    this.onStop = MonoProcessor.create();
    this.messageCodec = config.messageCodec();
    this.address = null;
    this.server = null;
  }

  /**
   * Copying constructor.
   *
   * @param server bound server
   * @param other instance of transport to copy from
   */
  private TransportImpl(DisposableServer server, TransportImpl other) {
    this.server = server;
    this.address = prepareAddress(server);
    this.config = other.config;
    this.loopResources = other.loopResources;
    this.messagesSubject = other.messagesSubject;
    this.messageSink = other.messageSink;
    this.connections = other.connections;
    this.stop = other.stop;
    this.onStop = other.onStop;
    this.messageCodec = other.messageCodec;

    // Setup cleanup
    stop.then(doStop())
        .doFinally(s -> onStop.onComplete())
        .subscribe(
            null, ex -> LOGGER.warn("[{}][doStop] Exception occurred: {}", address, ex.toString()));
  }

  private static Address prepareAddress(DisposableServer server) {
    InetAddress address = server.address().getAddress();
    int port = server.address().getPort();
    if (address.isAnyLocalAddress()) {
      return Address.create(Address.getLocalIpAddress().getHostAddress(), port);
    } else {
      return Address.create(address.getHostAddress(), port);
    }
  }

  /**
   * Init transport with the default configuration synchronously. Starts to accept connections on
   * local address.
   *
   * @return transport
   */
  public static Transport bindAwait() {
    return bindAwait(TransportConfig.defaultConfig());
  }

  /**
   * Init transport with the given configuration synchronously. Starts to accept connections on
   * local address.
   *
   * @return transport
   */
  public static Transport bindAwait(TransportConfig config) {
    try {
      return bind(config).block();
    } catch (Exception e) {
      throw Exceptions.propagate(e.getCause() != null ? e.getCause() : e);
    }
  }

  /**
   * Init transport with the default configuration asynchronously. Starts to accept connections on
   * local address.
   *
   * @return promise for bind operation
   */
  public static Mono<Transport> bind() {
    return bind(TransportConfig.defaultConfig());
  }

  /**
   * Init transport with the given configuration asynchronously. Starts to accept connections on
   * local address.
   *
   * @param config transport config
   * @return promise for bind operation
   */
  public static Mono<Transport> bind(TransportConfig config) {
    return new TransportImpl(config).bind0();
  }

  private Mono<Transport> bind0() {
    return prepareHttpServer(loopResources, config.port())
        .handle(this::onMessage)
        .bind()
        .doOnSuccess(t -> LOGGER.info("[bind0][{}] Bound cluster transport", t.address()))
        .doOnError(
            ex -> LOGGER.error("[bind0][{}] Exception occurred: {}", config.port(), ex.toString()))
        .map(server -> new TransportImpl(server, this))
        .cast(Transport.class);
  }

  private static HttpServer prepareHttpServer(LoopResources loopResources, int port) {
    return HttpServer.create()
        .tcpConfiguration(
            tcpServer -> {
              if (loopResources != null) {
                tcpServer = tcpServer.runOn(loopResources);
              }
              return tcpServer.bindAddress(() -> new InetSocketAddress(port));
            });
  }

  @Override
  public Address address() {
    return address;
  }

  @Override
  public boolean isStopped() {
    return onStop.isDisposed();
  }

  @Override
  public Mono<Void> send(Address address, Message message) {
    return null;
  }

  @Override
  public final Flux<Message> listen() {
    return messagesSubject.onBackpressureBuffer();
  }

  @Override
  public Mono<Message> requestResponse(Address address, final Message request) {
    return Mono.create(
        sink -> {
          Objects.requireNonNull(request, "request must be not null");
          Objects.requireNonNull(request.correlationId(), "correlationId must be not null");

          Disposable receive =
              listen()
                  .filter(resp -> resp.correlationId() != null)
                  .filter(resp -> resp.correlationId().equals(request.correlationId()))
                  .take(1)
                  .subscribe(sink::success, sink::error, sink::success);

          Disposable send =
              send(address, request)
                  .subscribe(
                      null,
                      ex -> {
                        receive.dispose();
                        sink.error(ex);
                      });

          sink.onDispose(Disposables.composite(send, receive));
        });
  }

  @Override
  public final Mono<Void> stop() {
    return Mono.defer(
        () -> {
          stop.onComplete();
          return onStop;
        });
  }

  private Mono<Void> doStop() {
    return Mono.defer(
        () -> {
          LOGGER.info("[{}][doStop] Stopping", address);
          // Complete incoming messages observable
          messageSink.complete();
          return Flux.concatDelayError(closeServer(), shutdownLoopResources())
              .then()
              .doFinally(s -> connections.clear())
              .doOnSuccess(avoid -> LOGGER.info("[{}][doStop] Stopped", address));
        });
  }

  private Mono<Void> closeServer() {
    return Mono.defer(
        () -> {
          if (server == null) {
            return Mono.empty();
          }
          LOGGER.info("[{}][closeServer] Closing server channel", address);
          return Mono.fromRunnable(server::dispose)
              .then(server.onDispose())
              .doOnSuccess(avoid -> LOGGER.info("[{}][closeServer] Closed server channel", address))
              .doOnError(
                  e ->
                      LOGGER.warn(
                          "[{}][closeServer] Exception occurred: {}", address, e.toString()));
        });
  }

  private Mono<Void> shutdownLoopResources() {
    return Mono.fromRunnable(loopResources::dispose).then(loopResources.disposeLater());
  }

  private Mono<Void> onMessage(HttpServerRequest request, HttpServerResponse response) {
    return response.sendWebsocket(
        (WebsocketInbound inbound, WebsocketOutbound outbound) ->
            inbound
                .receive()
                .retain()
                .doOnNext(
                    byteBuf -> {
                      if (!byteBuf.isReadable()) {
                        ReferenceCountUtil.safeRelease(byteBuf);
                        return;
                      }
                      messageSink.next(toMessage(byteBuf));
                    })
                .then());
  }

  private Message toMessage(ByteBuf byteBuf) {
    try (ByteBufInputStream stream = new ByteBufInputStream(byteBuf, true)) {
      return messageCodec.deserialize(stream);
    } catch (Exception e) {
      LOGGER.warn("[{}][toMessage] Exception occurred: {}", address, e.toString());
      throw new DecoderException(e);
    }
  }

  private ByteBuf toByteBuf(Message message) {
    ByteBuf bb = ByteBufAllocator.DEFAULT.buffer();
    ByteBufOutputStream stream = new ByteBufOutputStream(bb);
    try {
      messageCodec.serialize(message, stream);
    } catch (Exception e) {
      bb.release();
      LOGGER.warn("[{}][toByteBuf] Exception occurred: {}", address, e.toString());
      throw new EncoderException(e);
    }
    return bb;
  }
}
