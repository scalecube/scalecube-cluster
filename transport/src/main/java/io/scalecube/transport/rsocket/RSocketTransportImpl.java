package io.scalecube.transport.rsocket;

import io.netty.channel.ChannelOption;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

public class RSocketTransportImpl implements Transport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketTransportImpl.class);
  public static final String THREAD_PREFIX = "sc-cluster-io";

  // TODO: get rid of it finally; find another way out to emulate network failures [snripa]
  // Network emulator
  private final NetworkEmulator networkEmulator;
  // TODO: get rid of it finally. Use server handlers instead.
  private final DirectProcessor<Message> messagesSubject;

  // Injected
  private final TransportConfig config;
  private final LoopResources loopResources;
  private final MessageCodec messageCodec;

  // Calculated
  private final Address address;
  private CloseableChannel server;
  private final MonoProcessor<Void> stop;
  private final MonoProcessor<Void> onStop;

  // State
  private final Map<String, Function<Message, Mono<Message>>> messageHandlers = new HashMap<>();
  private final Map<Address, Mono<? extends RSocket>> clients;

  /**
   * Constructor with config as parameter.
   *
   * @param config transport configuration
   */
  public RSocketTransportImpl(TransportConfig config) {
    this.config = Objects.requireNonNull(config, "TransportConfig can't be null");
    this.loopResources = LoopResources.create(THREAD_PREFIX, 1, true);
    this.messagesSubject = DirectProcessor.create();
    this.clients = new ConcurrentHashMap<>();
    this.stop = MonoProcessor.create();
    this.onStop = MonoProcessor.create();
    this.messageCodec = config.getMessageCodec();
    this.networkEmulator = null;
    this.address = null;
  }

  private RSocketTransportImpl(
    Address address,
    CloseableChannel server,
    NetworkEmulator networkEmulator,
    RSocketTransportImpl other) {
    this.address = Objects.requireNonNull(address);
    this.server = Objects.requireNonNull(server);
    this.networkEmulator = Objects.requireNonNull(networkEmulator, "NetworkEmulator can't be null");
    this.config = other.config;
    this.loopResources = other.loopResources;
    this.messagesSubject = other.messagesSubject;
    this.clients = other.clients;
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
  public Flux<Message> listen() {
    return messagesSubject.onBackpressureBuffer();
  }

  @Override
  public boolean registerServerHandler(String qualifier,
    Function<Message, Mono<Message>> handler) {
    return messageHandlers.put(qualifier, handler) == null;
  }

  @Override
  public NetworkEmulator networkEmulator() {
    return networkEmulator;
  }

  /////////////////////////////////////////////
  //  Server bind methods
  /////////////////////////////////////////////

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
  public Mono<Transport> bind(TransportConfig config) {
    return new RSocketTransportImpl(config).bind0();
  }

  public Mono<Transport> bind0() {
    TcpServer tcpServer = newTcpServer();
    return RSocketFactory.receive()
      .frameDecoder(
        frame ->
          ByteBufPayload.create(frame.sliceData().retain(), frame.sliceMetadata().retain()))
      .acceptor(
        new RSocketTransportAcceptor(
          messagesSubject.sink(), messageHandlers, messageCodec))
      .transport(() -> TcpServerTransport.create(tcpServer))
      .start()
      .map(server -> this.server = server)
      .map(
        server -> {
          Address address =
            Address.create(server.address().getHostString(), server.address().getPort());
          NetworkEmulator networkEmulator =
            new NetworkEmulator(address, config.isUseNetworkEmulator());
          return new RSocketTransportImpl(address, server, networkEmulator, this);
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
  //  Client area
  /////////////////////////////////////////////
  @Override
  public Mono<Void> fireAndForget(Address dst, Message request) {
    return Mono.just(request)
      .flatMap(msg -> networkEmulator.tryFail(msg, address))
      .flatMap(msg -> networkEmulator.tryDelay(msg, address))
      .flatMap(msg -> {
        Mono<? extends RSocket> rsocketMono = clients.computeIfAbsent(dst, this::connect);
        return rsocketMono.flatMap(rsocket ->
          rsocket.fireAndForget(messageCodec.toPayload(request)));
      });
  }

  @Override
  public Mono<Message> requestResponse(Address dst, Message request) {
    return Mono.just(request)
      .flatMap(msg -> networkEmulator.tryFail(msg, address))
      .flatMap(msg -> networkEmulator.tryDelay(msg, address))
      .flatMap(msg -> {
        Mono<? extends RSocket> rsocketMono = clients.computeIfAbsent(dst, this::connect);
        return rsocketMono.flatMap(rsocket ->
          rsocket.requestResponse(messageCodec.toPayload(request)))
          .map(messageCodec::toMessage);
      });
  }

  private Mono<RSocket> connect(Address address) {
    TcpClient tcpClient =
      TcpClient.newConnection() // create non-pooled
        .runOn(loopResources)
        .host(address.host())
        .port(address.port());

    Mono<RSocket> rsocketMono =
      RSocketFactory.connect()
        .frameDecoder(
          frame ->
            ByteBufPayload.create(
              frame.sliceData().retain(), frame.sliceMetadata().retain()))
        .transport(() -> TcpClientTransport.create(tcpClient))
        .start();

    return rsocketMono
      .doOnSuccess(
        rsocket -> {
          LOGGER.info("Connected successfully on {}", address);
          // setup shutdown hook
          rsocket
            .onClose()
            .doOnTerminate(
              () -> {
                clients.remove(address);
                LOGGER.info("Connection closed on {} and removed from the pool", address);
              })
            .subscribe(null, th -> LOGGER.warn("Exception on closing rSocket: {}", th));
        })
      .doOnError(
        throwable -> {
          LOGGER.warn("Connect failed on {}, cause: {}", address, throwable);
          clients.remove(address);
        })
      .cache();
  }

  /////////////////////////////////////////////
  //  Shutdown area
  /////////////////////////////////////////////

  @Override
  public Mono<Void> stop() {
    return Mono.defer(
      () -> {
        stop.onComplete();
        return onStop;
      });
  }

  @Override
  public boolean isStopped() {
    return onStop.isDisposed();
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

  private Mono<Void> closeConnections() {
    return Mono.fromRunnable(
      () ->
        clients
          .values()
          .forEach(
            rsocketMono ->
              rsocketMono
                .doOnNext(Disposable::dispose)
                .subscribe(
                  null, e -> LOGGER.warn("Failed to close connection: " + e))));
  }
}
