package io.scalecube.transport.rsocket;

import io.netty.channel.ChannelOption;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import io.scalecube.transport.MessageCodec;
import io.scalecube.transport.NetworkEmulator;
import io.scalecube.transport.Transport;
import io.scalecube.transport.TransportConfig;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpServer;

public class RsocketTransportImpl implements Transport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RsocketTransportImpl.class);

  private final TransportConfig config;
  private final LoopResources loopResources;

  // Close handler
  private final MonoProcessor<Void> stop;
  private final MonoProcessor<Void> onStop;

  // Network emulator
  private final NetworkEmulator networkEmulator;

  // Server
  private final Address address;

  // Use for legacy contract only. Consider adding , message listeners for every component and
  // register them to rsocket server before starting it
  private final DirectProcessor<Message> messagesSubject;
  // Message codec

  private final MessageCodec messageCodec;
  private final Map<String, Consumer<Message>> serverMessageListeners = new HashMap<>();
  private CloseableChannel server;

  /**
   * Constructor with cofig as parameter.
   *
   * @param config transport configuration
   */
  public RsocketTransportImpl(TransportConfig config) {
    this.config = Objects.requireNonNull(config);
    this.loopResources = LoopResources.create("sc-cluster-io", 1, true);
    this.messagesSubject = DirectProcessor.create();
    this.messageSink = messagesSubject.sink();
    this.stop = MonoProcessor.create();
    this.onStop = MonoProcessor.create();
    this.messageCodec = config.getMessageCodec();
    this.networkEmulator = null;
    this.address = null;
  }

  private RsocketTransportImpl(
      Address address,
      CloseableChannel server,
      NetworkEmulator networkEmulator,
      RsocketTransportImpl other) {
    this.address = Objects.requireNonNull(address);
    this.server = Objects.requireNonNull(server);
    this.networkEmulator = Objects.requireNonNull(networkEmulator);
    this.config = other.config;
    this.loopResources = other.loopResources;
    this.messagesSubject = other.messagesSubject;
    this.messageSink = other.messageSink;
    this.stop = other.stop;
    this.onStop = other.onStop;
    this.messageCodec = other.messageCodec;

    // Setup cleanup
    stop.then(doStop())
        .doFinally(s -> onStop.onComplete())
        .subscribe(null, ex -> LOGGER.warn("Exception occurred on transport stop: " + ex));
  }

  @Override
  public Address address() {
    return address;
  }

  @Override
  public Mono<Void> stop() {
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
          return Flux.concatDelayError(closeServer(), closeConnections())
              .doOnTerminate(loopResources::dispose)
              .then()
              .doOnSuccess(avoid -> LOGGER.debug("Transport has shut down on {}", address));
        });
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
  public Mono<Message> requestResponse(Message request, Address address) {
    return null;
  }

  @Override
  public Flux<Message> listen() {
    return messagesSubject.onBackpressureBuffer();
  }

  @Override
  public NetworkEmulator networkEmulator() {
    return networkEmulator;
  }

  /////////////////////////////////////////////
  //  Server bind methods
  /////////////////////////////////////////////

  @Override
  public Transport bindAwait() {
    return bindAwait(TransportConfig.defaultConfig());
  }

  @Override
  public Transport bindAwait(boolean useNetworkEmulator) {
    return bindAwait(TransportConfig.builder().useNetworkEmulator(useNetworkEmulator).build());
  }

  @Override
  public Transport bindAwait(TransportConfig config) {
    try {
      return bind(config).block();
    } catch (Exception e) {
      throw Exceptions.propagate(e.getCause() != null ? e.getCause() : e);
    }
  }

  @Override
  public Mono<Transport> bind() {
    return bind(TransportConfig.defaultConfig());
  }

  @Override
  public Mono<Transport> bind(TransportConfig config) {
    return new RsocketTransportImpl(config).bind0();
  }

  private Mono<Transport> bind0() {
    TcpServer tcpServer = newTcpServer();
    return RSocketFactory.receive()
        .frameDecoder(
            frame ->
                ByteBufPayload.create(frame.sliceData().retain(), frame.sliceMetadata().retain()))
        .acceptor(
            new RSocketTransportAcceptor(
                messagesSubject.sink(), serverMessageListeners, messageCodec))
        .transport(() -> TcpServerTransport.create(tcpServer))
        .start()
        .map(server -> this.server = server)
        .map(
            server -> {
              Address address =
                  Address.create(server.address().getHostString(), server.address().getPort());
              NetworkEmulator networkEmulator =
                  new NetworkEmulator(address, config.isUseNetworkEmulator());
              return new RsocketTransportImpl(address, server, networkEmulator, this);
            });
  }

  private TcpServer newTcpServer() {
    return TcpServer.create()
        .runOn(loopResources)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .addressSupplier(() -> new InetSocketAddress(config.getPort()));
  }

  /////////////////////////////////////////////
  //  Shutdown area
  /////////////////////////////////////////////
  private Mono<Void> closeServer() {
    return Mono.defer(
        () ->
            Optional.ofNullable(server)
                .map(
                    server -> {
                      server.dispose();
                      return server
                          .onClose()
                          .doOnError(e -> LOGGER.warn("Failed to close server: " + e))
                          .onErrorResume(e -> Mono.empty());
                    })
                .orElse(Mono.empty()));
  }
}
