package io.scalecube.transport;

import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.netty.Connection;
import reactor.netty.ConnectionObserver;
import reactor.netty.DisposableChannel;
import reactor.netty.DisposableServer;
import reactor.netty.NettyInbound;
import reactor.netty.NettyOutbound;
import reactor.netty.NettyPipeline.SendOptions;
import reactor.netty.channel.BootstrapHandlers;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

/**
 * Default transport implementation based on reactor-netty tcp client and server implementation and
 * protobuf codec.
 */
final class TransportImpl implements Transport {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransportImpl.class);

  private final TransportConfig config;
  private final LoopResources loopResources;

  // Subject
  private final DirectProcessor<Message> messagesSubject = DirectProcessor.create();
  private final FluxSink<Message> messageSink = messagesSubject.sink();

  private final Map<Address, Mono<? extends Connection>> connections = new ConcurrentHashMap<>();

  // Pipeline
  private final ExceptionHandler exceptionHandler = new ExceptionHandler();
  private final InboundChannelInitializer inboundPipeline = new InboundChannelInitializer();
  private final OutboundChannelInitializer outboundPipeline = new OutboundChannelInitializer();

  // Close handler
  private final MonoProcessor<Void> onClose = MonoProcessor.create();

  // Network emulator
  private NetworkEmulator networkEmulator;

  private Address address;
  private DisposableServer server;

  /**
   * TransportImpl constructor with cofig as parameter.
   *
   * @param config transport configuration
   */
  public TransportImpl(TransportConfig config) {
    this.config = Objects.requireNonNull(config);
    this.loopResources = LoopResources.create("cluster-transport", 1, 1, true);
  }

  private static Address toAddress(SocketAddress address) {
    InetSocketAddress inetAddress = ((InetSocketAddress) address);
    return Address.create(inetAddress.getHostString(), inetAddress.getPort());
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
        .doOnSuccessOrError(this::onBind)
        .thenReturn(this);
  }

  @Override
  public Address address() {
    return address;
  }

  @Override
  public boolean isStopped() {
    return onClose.isDisposed();
  }

  @Override
  public NetworkEmulator networkEmulator() {
    return networkEmulator;
  }

  @Override
  public final Mono<Void> stop() {
    return Mono.defer(
        () -> {
          if (!onClose.isDisposed()) {
            // Complete incoming messages observable
            messageSink.complete();
            closeServer()
                .then(closeConnections())
                .doOnTerminate(loopResources::dispose)
                .doOnTerminate(onClose::onComplete)
                .subscribe();
          }
          return onClose;
        });
  }

  @Override
  public final Flux<Message> listen() {
    return messagesSubject.onBackpressureBuffer();
  }

  @Override
  public Mono<Void> send(Address address, Message message) {
    return getOrConnect(address)
        .flatMap(conn -> send0(conn, message, address))
        .then()
        .doOnError(
            ex ->
                LOGGER.debug(
                    "Failed to send {} from {} to {}, cause: {}",
                    message,
                    this.address,
                    address,
                    ex));
  }

  private Mono<Void> onMessage(NettyInbound in, NettyOutbound out) {
    return in.receive() //
        .retain()
        .map(MessageCodec::deserialize)
        .doOnNext(messageSink::next)
        .then();
  }

  private void onBind(DisposableServer s, Throwable ex) {
    if (s != null) {
      this.server = s;
      address = toAddress(s.address());
      networkEmulator = new NetworkEmulator(address, config.isUseNetworkEmulator());
      LOGGER.info("Bound cluster transport on: {}", address);
    }
    if (ex != null) {
      LOGGER.error("Failed to bind cluster transport on port={}, cause: {}", config.getPort(), ex);
    }
  }

  private Mono<? extends Void> send0(Connection conn, Message message, Address address) {
    // set local address as outgoing address
    message.setSender(this.address);
    // do send
    return conn.outbound()
        .options(SendOptions::flushOnEach)
        .send(
            Mono.just(message)
                .flatMap(msg -> networkEmulator.tryFail(msg, address))
                .flatMap(msg -> networkEmulator.tryDelay(msg, address))
                .map(MessageCodec::serialize))
        .then();
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
        .doOnConnected(
            c ->
                LOGGER.debug(
                    "Connected from {} to {}: {}",
                    TransportImpl.this.address,
                    address,
                    c.channel()))
        .connect()
        .doOnError(
            t -> {
              LOGGER.warn("Failed to connect to remote address {}, cause: {}", address, t);
              connections.remove(address);
            })
        .cache();
  }

  private Mono<Void> closeServer() {
    return Mono.defer(
        () ->
            Optional.ofNullable(server)
                .map(
                    server -> {
                      server.dispose();
                      return server
                          .onDispose()
                          .doOnError(e -> LOGGER.warn("Failed to close server: " + e))
                          .onErrorResume(e -> Mono.empty());
                    })
                .orElse(Mono.empty()));
  }

  private Mono<Void> closeConnections() {
    return Mono.fromRunnable(
        () ->
            connections
                .values()
                .forEach(
                    connectionMono ->
                        connectionMono
                            .doOnNext(DisposableChannel::dispose)
                            .flatMap(DisposableChannel::onDispose)
                            .subscribe(
                                null, e -> LOGGER.warn("Failed to close connection: " + e))));
  }

  /**
   * Creates TcpServer.
   *
   * @return tcp server
   */
  private TcpServer newTcpServer() {
    return TcpServer.create()
        .runOn(loopResources)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .addressSupplier(() -> new InetSocketAddress(config.getPort()))
        .bootstrap(b -> BootstrapHandlers.updateConfiguration(b, "inbound", inboundPipeline));
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
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeout())
        .bootstrap(b -> BootstrapHandlers.updateConfiguration(b, "outbound", outboundPipeline));
  }

  private final class InboundChannelInitializer implements BiConsumer<ConnectionObserver, Channel> {

    @Override
    public void accept(ConnectionObserver connectionObserver, Channel channel) {
      ChannelPipeline pipeline = channel.pipeline();
      pipeline.addLast(new ProtobufVarint32FrameDecoder());
      pipeline.addLast(exceptionHandler);
    }
  }

  private final class OutboundChannelInitializer
      implements BiConsumer<ConnectionObserver, Channel> {

    @Override
    public void accept(ConnectionObserver connectionObserver, Channel channel) {
      ChannelPipeline pipeline = channel.pipeline();
      pipeline.addLast(new ProtobufVarint32LengthFieldPrepender());
      pipeline.addLast(exceptionHandler);
    }
  }
}
