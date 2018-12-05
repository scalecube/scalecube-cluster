package io.scalecube.transport;

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
import java.net.InetSocketAddress;
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
 * jackson codec.
 */
final class TransportImpl implements Transport {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransportImpl.class);

  private final TransportConfig config;
  private final LoopResources loopResources;

  // Subject
  private final DirectProcessor<Message> messagesSubject;
  private final FluxSink<Message> messageSink;

  private final Map<Address, Mono<? extends Connection>> connections;

  // Pipeline
  private final ExceptionHandler exceptionHandler;
  private final InboundChannelInitializer inboundPipeline;
  private final OutboundChannelInitializer outboundPipeline;

  // Close handler
  private final MonoProcessor<Void> onClose;

  // Network emulator
  private final NetworkEmulator networkEmulator;

  // Server
  private final Address address;
  private final DisposableServer server;

  /**
   * Constructor with cofig as parameter.
   *
   * @param config transport configuration
   */
  public TransportImpl(TransportConfig config) {
    this.config = Objects.requireNonNull(config);
    this.loopResources = LoopResources.create("sc-cluster-io", 1, 1, true);
    this.messagesSubject = DirectProcessor.create();
    this.messageSink = messagesSubject.sink();
    this.connections = new ConcurrentHashMap<>();
    this.exceptionHandler = new ExceptionHandler();
    this.inboundPipeline = new InboundChannelInitializer();
    this.outboundPipeline = new OutboundChannelInitializer();
    this.onClose = MonoProcessor.create();
    this.networkEmulator = null;
    this.address = null;
    this.server = null;
  }

  /**
   * Copying constructor.
   *
   * @param address server addtess
   * @param server bound server
   * @param networkEmulator network emulator
   * @param other instance of transport to copy from
   */
  private TransportImpl(
      Address address,
      DisposableServer server,
      NetworkEmulator networkEmulator,
      TransportImpl other) {
    this.address = Objects.requireNonNull(address);
    this.server = Objects.requireNonNull(server);
    this.networkEmulator = Objects.requireNonNull(networkEmulator);
    this.config = other.config;
    this.loopResources = other.loopResources;
    this.messagesSubject = other.messagesSubject;
    this.messageSink = other.messageSink;
    this.connections = other.connections;
    this.exceptionHandler = other.exceptionHandler;
    this.inboundPipeline = other.inboundPipeline;
    this.outboundPipeline = other.outboundPipeline;
    this.onClose = other.onClose;
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
            server -> LOGGER.info("Bound cluster transport on {}:{}", server.host(), server.port()))
        .doOnError(
            ex ->
                LOGGER.error(
                    "Failed to bind cluster transport on port={}, cause: {}", config.getPort(), ex))
        .map(this::onBind);
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
        .doOnError(ex -> LOGGER.debug("Failed to send {} to {}, cause: {}", message, address, ex));
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
      return MessageCodec.deserialize(stream);
    } catch (Exception e) {
      throw new DecoderException(e);
    }
  }

  private TransportImpl onBind(DisposableServer server) {
    Address address = Address.create(server.address().getHostString(), server.address().getPort());
    NetworkEmulator networkEmulator = new NetworkEmulator(address, config.isUseNetworkEmulator());
    return new TransportImpl(address, server, networkEmulator, this);
  }

  private Mono<? extends Void> send0(Connection conn, Message message, Address address) {
    // check sender not null
    Objects.requireNonNull(message.sender(), "sender must be not null");
    // do send
    return conn.outbound()
        .options(SendOptions::flushOnEach)
        .send(
            Mono.just(message)
                .flatMap(msg -> networkEmulator.tryFail(msg, address))
                .flatMap(msg -> networkEmulator.tryDelay(msg, address))
                .map(this::toByteBuf))
        .then();
  }

  private ByteBuf toByteBuf(Message message) {
    ByteBuf bb = ByteBufAllocator.DEFAULT.buffer();
    ByteBufOutputStream stream = new ByteBufOutputStream(bb);
    try {
      MessageCodec.serialize(message, stream);
    } catch (Exception e) {
      bb.release();
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
      pipeline.addLast(new LengthFieldBasedFrameDecoder(32768, 0, 4, 0, 4));
      pipeline.addLast(exceptionHandler);
    }
  }

  private final class OutboundChannelInitializer
      implements BiConsumer<ConnectionObserver, Channel> {

    @Override
    public void accept(ConnectionObserver connectionObserver, Channel channel) {
      ChannelPipeline pipeline = channel.pipeline();
      pipeline.addLast(new LengthFieldPrepender(4));
      pipeline.addLast(exceptionHandler);
    }
  }
}
