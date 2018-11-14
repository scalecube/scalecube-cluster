package io.scalecube.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.netty.ConnectionObserver;
import reactor.netty.DisposableServer;
import reactor.netty.NettyPipeline.SendOptions;
import reactor.netty.channel.BootstrapHandlers;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

/**
 * Default transport implementation based on tcp netty client and server implementation and protobuf
 * codec.
 */
final class TransportImpl implements Transport {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransportImpl.class);

  private final TransportConfig config;
  private final LoopResources loopResources;

  // Subject

  private final FluxProcessor<Message, Message> incomingMessagesSubject =
      DirectProcessor.<Message>create().serialize();

  private final FluxSink<Message> messageSink = incomingMessagesSubject.sink();

  // Pipeline
  private final ExceptionHandler exceptionHandler = new ExceptionHandler();
  private final MessageToByteEncoder<Message> serializerHandler = new MessageSerializerHandler();
  private final MessageToMessageDecoder<ByteBuf> deserializerHandler =
      new MessageDeserializerHandler();
  private final MessageHandler messageHandler = new MessageHandler(messageSink);
  private final InboundChannelInitializer incomingPipeline = new InboundChannelInitializer();
  private final OutgoingChannelInitializer outcomingPipeline = new OutgoingChannelInitializer();

  // Close logic handler
  private final MonoProcessor<Void> onClose = MonoProcessor.create();

  // Network emulator
  private NetworkEmulator networkEmulator;
  private NetworkEmulatorHandler networkEmulatorHandler;

  private Address address;
  private DisposableServer server;

  private final ConnectionProvider connections = ConnectionProvider.fixed("cluster-connections", 1);

  public TransportImpl(TransportConfig config) {
    this.config = Objects.requireNonNull(config);
    this.loopResources = LoopResources.create("cluster-transport", config.getWorkerThreads(), true);
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
    return Mono.defer(() -> bind0(config.getPort()));
  }

  private Mono<Transport> bind0(int port) {
    return TcpServer.create()
        .runOn(loopResources)
        .addressSupplier(() -> new InetSocketAddress(config.getPort()))
        .bootstrap(b -> BootstrapHandlers.updateConfiguration(b, "inbound", incomingPipeline))
        // .handle((inb, outb) -> inb.receive().aggregate().retain().then())
        .bind()
        .doOnSuccess(this::onBind)
        .doOnError(
            th -> LOGGER.error("Failed to bind cluster transport on port={}, cause: {}", port, th))
        .thenReturn(this);
  }

  private void onBind(DisposableServer server1) {
    server = server1;
    address = toAddress(server.address());
    networkEmulator = new NetworkEmulator(address, config.isUseNetworkEmulator());
    networkEmulatorHandler =
        config.isUseNetworkEmulator() ? new NetworkEmulatorHandler(networkEmulator) : null;
    LOGGER.info("Bound cluster transport on: {}", address);
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
            closeServerChannel()
                .then(connections.disposeLater())
                .then(loopResources.disposeLater())
                .doOnTerminate(onClose::onComplete)
                .subscribe();
          }
          return onClose;
        });
  }

  @Override
  public final Flux<Message> listen() {
    return incomingMessagesSubject.onBackpressureBuffer();
  }

  @Override
  public Mono<Void> send(Address address, Message message) {
    return Mono.defer(
        () -> {
          Objects.requireNonNull(address, "address");
          Objects.requireNonNull(message, "message");

          return tcpClient()
              .host(address.host())
              .port(address.port())
              .connect()
              .doOnError(
                  th ->
                      LOGGER.warn("Failed to connect to remote address {}, cause: {}", address, th))
              .flatMap(
                  conn -> {
                    message.setSender(this.address); // set local address as outgoing address
                    return conn.outbound()
                        .options(SendOptions::flushOnEach)
                        .sendObject(message)
                        .then()
                        .log(">>>>>>>>>>>>>>>>>>>>>");
                  })
              .doOnError(
                  ex ->
                      LOGGER.debug(
                          "Failed to send {} from {} to {}, cause: {}",
                          message,
                          this.address,
                          address,
                          ex));
        });
  }

  private Mono<Void> closeServerChannel() {
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

  private TcpClient tcpClient() {
    return TcpClient.create(connections)
        .runOn(loopResources)
        .bootstrap(b -> BootstrapHandlers.updateConfiguration(b, "outbound", outcomingPipeline))
        .observe(
            (connection, newState) -> LOGGER.debug("{} has new state -> {}", connection, newState));
  }

  private final class InboundChannelInitializer implements BiConsumer<ConnectionObserver, Channel> {

    @Override
    public void accept(ConnectionObserver connectionObserver, Channel channel) {
      ChannelPipeline pipeline = channel.pipeline();
      pipeline.addLast(new ProtobufVarint32FrameDecoder());
      pipeline.addLast(deserializerHandler);
      pipeline.addLast(messageHandler);
      pipeline.addLast(exceptionHandler);
    }
  }

  private final class OutgoingChannelInitializer
      implements BiConsumer<ConnectionObserver, Channel> {

    @Override
    public void accept(ConnectionObserver connectionObserver, Channel channel) {
      ChannelPipeline pipeline = channel.pipeline();
      pipeline.addLast(new ProtobufVarint32LengthFieldPrepender());
      pipeline.addLast(serializerHandler);
      Optional.ofNullable(networkEmulatorHandler).ifPresent(pipeline::addLast);
      pipeline.addLast(exceptionHandler);
    }
  }
}
