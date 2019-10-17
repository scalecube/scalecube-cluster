package io.scalecube.transport.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.MessageCodec;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.net.Address;
import java.net.InetAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
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
import reactor.netty.ConnectionObserver;
import reactor.netty.DisposableServer;
import reactor.netty.NettyInbound;
import reactor.netty.NettyOutbound;
import reactor.netty.NettyPipeline.SendOptions;
import reactor.netty.channel.BootstrapHandlers;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

public final class TransportImpl implements Transport {

  private static final Logger LOGGER = LoggerFactory.getLogger(Transport.class);

  private final TransportConfig config;
  private final LoopResources loopResources;

  // Subject
  private final DirectProcessor<Message> messagesSubject;
  private final FluxSink<Message> messageSink;

  private final Map<Address, Mono<? extends Connection>> connections;

  // Pipeline
  private final ExceptionHandler exceptionHandler;
  private final TransportChannelInitializer channelInitializer;

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
    this.exceptionHandler = new ExceptionHandler();
    this.channelInitializer = new TransportChannelInitializer();
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
    this.exceptionHandler = other.exceptionHandler;
    this.channelInitializer = other.channelInitializer;
    this.stop = other.stop;
    this.onStop = other.onStop;
    this.messageCodec = other.messageCodec;

    // Setup cleanup
    stop.then(doStop())
        .doFinally(s -> onStop.onComplete())
        .subscribe(null, ex -> LOGGER.warn("Exception occurred on transport stop: " + ex));
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

  /**
   * Starts to accept connections on local address.
   *
   * @return mono transport
   */
  public Mono<Transport> bind0() {
    return newTcpServer()
        .handle(this::onMessage)
        .bind()
        .doOnSuccess(
            server -> {
              InetAddress address = server.address().getAddress();
              if (address.isAnyLocalAddress()) {
                LOGGER.debug("Bound cluster transport on *:{}", server.port());
              } else {
                LOGGER.debug(
                    "Bound cluster transport on {}:{}", address.getHostAddress(), server.port());
              }
            })
        .doOnError(
            ex ->
                LOGGER.error(
                    "Failed to bind cluster transport on port={}, cause: {}",
                    config.port(),
                    ex.toString()))
        .map(server -> new TransportImpl(server, this));
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
          LOGGER.debug("Transport is shutting down on {}", address);
          // Complete incoming messages observable
          messageSink.complete();
          return Flux.concatDelayError(closeServer(), shutdownLoopResources())
              .then()
              .doFinally(s -> connections.clear())
              .doOnSuccess(avoid -> LOGGER.debug("Transport has been shut down on {}", address));
        });
  }

  @Override
  public final Flux<Message> listen() {
    return messagesSubject.onBackpressureBuffer();
  }

  @Override
  public Mono<Void> send(Address address, Message message) {
    return getOrConnect(address).flatMap(conn -> send0(conn, message)).then();
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

  @SuppressWarnings("unused")
  private Mono<Void> onMessage(NettyInbound in, NettyOutbound out) {
    return in.receive() //
        .retain()
        .map(this::toMessage)
        .doOnNext(messageSink::next)
        .then();
  }

  private Message toMessage(ByteBuf byteBuf) {
    try (ByteBufInputStream stream = new ByteBufInputStream(byteBuf, true)) {
      return messageCodec.deserialize(stream);
    } catch (Exception e) {
      LOGGER.warn(
          "Message codec exception occurred at converting bytes to message: " + e.toString());
      throw new DecoderException(e);
    }
  }

  private Mono<? extends Void> send0(Connection conn, Message message) {
    // do send
    return conn.outbound()
        .options(SendOptions::flushOnEach)
        .send(Mono.just(message).map(this::toByteBuf))
        .then();
  }

  private ByteBuf toByteBuf(Message message) {
    ByteBuf bb = ByteBufAllocator.DEFAULT.buffer();
    ByteBufOutputStream stream = new ByteBufOutputStream(bb);
    try {
      messageCodec.serialize(message, stream);
    } catch (Exception e) {
      bb.release();
      LOGGER.warn(
          "Message codec exception occurred at converting message to bytes: " + e.toString());
      throw new EncoderException(e);
    }
    return bb;
  }

  private Mono<Connection> getOrConnect(Address address) {
    return Mono.create(
        sink ->
            connections
                .computeIfAbsent(address, this::connect0)
                .subscribe(sink::success, sink::error));
  }

  private Mono<? extends Connection> connect0(Address address) {
    return newTcpClient(address)
        .doOnDisconnected(
            c -> {
              LOGGER.debug("Disconnected from: {} {}", address, c.channel());
              connections.remove(address);
            })
        .doOnConnected(c -> LOGGER.debug("Connected to {}: {}", address, c.channel()))
        .connect()
        .doOnError(
            th -> {
              LOGGER.debug("Failed to connect on address {}, cause: {}", address, th.toString());
              connections.remove(address);
            })
        .cache();
  }

  private Mono<Void> closeServer() {
    return Mono.defer(
        () -> {
          if (server == null) {
            return Mono.empty();
          }
          server.dispose();
          return server.onDispose().doOnError(e -> LOGGER.warn("Failed to close server: " + e));
        });
  }

  private Mono<Void> shutdownLoopResources() {
    return Mono.defer(
        () -> {
          loopResources.dispose();
          return loopResources.disposeLater();
        });
  }

  /**
   * Creates TcpServer.
   *
   * @return tcp server
   */
  private TcpServer newTcpServer() {
    TcpServer tcpServer =
        TcpServer.create()
            .runOn(loopResources)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.SO_REUSEADDR, true)
            .port(config.port());

    if (config.host() != null) {
      tcpServer = tcpServer.host(config.host());
    }

    return tcpServer.bootstrap(
        b -> BootstrapHandlers.updateConfiguration(b, "inbound", channelInitializer));
  }

  /**
   * Creates TcpClient for target address.
   *
   * @param address connect address
   * @return tcp client
   */
  private TcpClient newTcpClient(Address address) {
    return TcpClient.create(ConnectionProvider.newConnection())
        .runOn(loopResources)
        .host(address.host())
        .port(address.port())
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.connectTimeout())
        .bootstrap(b -> BootstrapHandlers.updateConfiguration(b, "outbound", channelInitializer));
  }

  private final class TransportChannelInitializer
      implements BiConsumer<ConnectionObserver, Channel> {

    private static final int LENGTH_FIELD_LENGTH = 4;

    @Override
    public void accept(ConnectionObserver connectionObserver, Channel channel) {
      ChannelPipeline pipeline = channel.pipeline();
      pipeline.addLast(new LengthFieldPrepender(LENGTH_FIELD_LENGTH));
      pipeline.addLast(
          new LengthFieldBasedFrameDecoder(
              config.maxFrameLength(), 0, LENGTH_FIELD_LENGTH, 0, LENGTH_FIELD_LENGTH));
      pipeline.addLast(exceptionHandler);
    }
  }
}
