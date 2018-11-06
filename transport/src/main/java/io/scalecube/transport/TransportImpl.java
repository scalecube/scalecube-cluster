package io.scalecube.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ServerChannel;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.MonoSink;

/**
 * Default transport implementation based on tcp netty client and server implementation and protobuf
 * codec.
 */
final class TransportImpl implements Transport {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransportImpl.class);

  private final TransportConfig config;

  // SUbject

  private final FluxProcessor<Message, Message> incomingMessagesSubject =
      DirectProcessor.<Message>create().serialize();

  private final FluxSink<Message> messageSink = incomingMessagesSubject.sink();

  private final Map<Address, Mono<Channel>> outgoingChannels = new ConcurrentHashMap<>();

  // Pipeline
  private final BootstrapFactory bootstrapFactory;
  private final IncomingChannelInitializer incomingChannelInitializer =
      new IncomingChannelInitializer();
  private final ExceptionHandler exceptionHandler = new ExceptionHandler();
  private final MessageToByteEncoder<Message> serializerHandler;
  private final MessageToMessageDecoder<ByteBuf> deserializerHandler;
  private final MessageHandler messageHandler;

  // Network emulator
  private NetworkEmulator networkEmulator;
  private NetworkEmulatorHandler networkEmulatorHandler;

  private Address address;
  private ServerChannel serverChannel;

  private final MonoProcessor<Void> onClose = MonoProcessor.create();

  public TransportImpl(TransportConfig config) {
    this.config = Objects.requireNonNull(config);
    this.serializerHandler = new MessageSerializerHandler();
    this.deserializerHandler = new MessageDeserializerHandler();
    this.messageHandler = new MessageHandler(messageSink);
    this.bootstrapFactory = new BootstrapFactory(config);
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
    return Mono.defer(
        () ->
            toMono(
                    bootstrapFactory
                        .serverBootstrap()
                        .childHandler(incomingChannelInitializer)
                        .bind(port))
                .doOnSuccess(
                    channel -> {
                      serverChannel = (ServerChannel) channel;
                      address = toAddress(serverChannel.localAddress());
                      networkEmulator = new NetworkEmulator(address, config.isUseNetworkEmulator());
                      networkEmulatorHandler =
                          config.isUseNetworkEmulator()
                              ? new NetworkEmulatorHandler(networkEmulator)
                              : null;
                      LOGGER.info("Bound cluster transport on: {}", address);
                    })
                .doOnError(
                    cause ->
                        LOGGER.error(
                            "Failed to bind cluster transport on port={}, cause: {}", port, cause))
                .thenReturn(TransportImpl.this));
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
            stop0().doOnTerminate(onClose::onComplete).subscribe();
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
          Objects.requireNonNull(address);
          Objects.requireNonNull(message);

          message.setSender(this.address); // set local address as outgoing address

          return getOrConnect(address)
              .flatMap(channel -> toMono(channel.writeAndFlush(message)).then())
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

  private Mono<Channel> getOrConnect(Address address) {
    return Mono.create(
        sink ->
            outgoingChannels
                .computeIfAbsent(address, this::connect0)
                .subscribe(sink::success, sink::error));
  }

  private Mono<Channel> connect0(Address address) {
    return connect1(address)
        .doOnSuccess(
            channel ->
                LOGGER.debug(
                    "Connected from {} to {}: {}", TransportImpl.this.address, address, channel))
        .doOnError(throwable -> outgoingChannels.remove(address))
        .cache();
  }

  private Mono<Channel> connect1(Address address) {
    return Mono.create(
        sink ->
            bootstrapFactory
                .clientBootstrap()
                .handler(new OutgoingChannelInitializer(address))
                .connect(address.host(), address.port())
                .addListener(
                    future ->
                        toMono((ChannelFuture) future)
                            .subscribe(sink::success, sink::error, sink::success)));
  }

  @ChannelHandler.Sharable
  private final class IncomingChannelInitializer extends ChannelInitializer {
    @Override
    protected void initChannel(Channel channel) {
      ChannelPipeline pipeline = channel.pipeline();
      pipeline.addLast(new ProtobufVarint32FrameDecoder());
      pipeline.addLast(deserializerHandler);
      pipeline.addLast(messageHandler);
      pipeline.addLast(exceptionHandler);
    }
  }

  @ChannelHandler.Sharable
  private final class OutgoingChannelInitializer extends ChannelInitializer {
    private final Address address;

    public OutgoingChannelInitializer(Address address) {
      this.address = address;
    }

    @Override
    protected void initChannel(Channel channel) {
      ChannelPipeline pipeline = channel.pipeline();
      pipeline.addLast(
          new ChannelDuplexHandler() {
            @Override
            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
              LOGGER.debug("Disconnected from: {} {}", address, ctx.channel());
              outgoingChannels.remove(address);
              super.channelInactive(ctx);
            }
          });
      pipeline.addLast(new ProtobufVarint32LengthFieldPrepender());
      pipeline.addLast(serializerHandler);
      if (networkEmulatorHandler != null) {
        pipeline.addLast(networkEmulatorHandler);
      }
      pipeline.addLast(exceptionHandler);
    }
  }

  private static Address toAddress(SocketAddress address) {
    InetSocketAddress inetAddress = ((InetSocketAddress) address);
    return Address.create(inetAddress.getHostString(), inetAddress.getPort());
  }

  private static Mono<Channel> toMono(ChannelFuture channelFuture) {
    return Mono.create(
        sink -> channelFuture.addListener((ChannelFutureListener) future -> toMono0(sink, future)));
  }

  private static void toMono0(MonoSink<Channel> sink, ChannelFuture future) {
    if (future.isSuccess()) {
      sink.success(future.channel());
    } else {
      sink.error(future.cause());
    }
  }

  private Mono<Void> stop0() {
    return Mono.defer(
        () -> {
          // Complete incoming messages observable
          try {
            messageSink.complete();
          } catch (Exception ignore) {
            // ignore
          }

          List<Mono<Void>> stopList = new ArrayList<>();

          // close server channel
          Optional.ofNullable(serverChannel)
              .map(ChannelOutboundInvoker::close)
              .map(TransportImpl::toMono)
              .map(Mono::then)
              .ifPresent(stopList::add);

          // close connected channels
          for (Address address : outgoingChannels.keySet()) {
            Optional.ofNullable(outgoingChannels.get(address))
                .ifPresent(
                    channelMono ->
                        channelMono
                            .map(ChannelOutboundInvoker::close)
                            .map(TransportImpl::toMono)
                            .map(Mono::then)
                            .subscribe(stopList::add));
          }
          outgoingChannels.clear();

          bootstrapFactory.shutdown();

          return Mono.when(stopList).then();
        });
  }
}
