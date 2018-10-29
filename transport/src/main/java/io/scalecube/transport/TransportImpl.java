package io.scalecube.transport;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ServerChannel;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
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

  private volatile boolean stopped = false;

  public TransportImpl(TransportConfig config) {
    this.config = Objects.requireNonNull(config);
    this.serializerHandler = new MessageSerializerHandler();
    this.deserializerHandler = new MessageDeserializerHandler();
    this.messageHandler = new MessageHandler(messageSink);
    this.bootstrapFactory = new BootstrapFactory(config);
  }

  /** Starts to accept connections on local address. */
  public Mono<Transport> bind0() {
    return Mono.defer(
        () -> {
          ServerBootstrap server =
              bootstrapFactory.serverBootstrap().childHandler(incomingChannelInitializer);

          // Resolve listen IP address
          InetAddress listenAddress =
              Addressing.getLocalIpAddress(
                  config.getListenAddress(), config.getListenInterface(), config.isPreferIPv6());

          // Listen port
          int bindPort = config.getPort();

          return bind0(server, listenAddress, bindPort);
        });
  }

  /**
   * Helper bind method to start accepting connections on {@code listenAddress} and {@code
   * bindPort}.
   *
   * @param listenAddress listen address of cluster transport.
   * @param bindPort listen port of cluster transport.
   * @param server a server bootstrap.
   */
  private Mono<Transport> bind0(ServerBootstrap server, InetAddress listenAddress, int bindPort) {
    return Mono.create(
        sink -> {
          // Get address object and bind
          server
              .bind(listenAddress, bindPort)
              .addListener(
                  channelFuture -> {
                    if (channelFuture.isSuccess()) {
                      serverChannel = (ServerChannel) ((ChannelFuture) channelFuture).channel();
                      address = toAddress(serverChannel.localAddress());
                      networkEmulator = new NetworkEmulator(address, config.isUseNetworkEmulator());
                      networkEmulatorHandler =
                          config.isUseNetworkEmulator()
                              ? new NetworkEmulatorHandler(networkEmulator)
                              : null;
                      LOGGER.info("Bound to: {}", address);
                      sink.success(TransportImpl.this);
                    } else {
                      Throwable cause = channelFuture.cause();
                      LOGGER.error("Failed to bind to: {}, cause: {}", listenAddress, cause);
                      sink.error(cause);
                    }
                  });
        });
  }

  @Override
  public Address address() {
    return address;
  }

  @Override
  public boolean isStopped() {
    return stopped;
  }

  @Override
  public NetworkEmulator networkEmulator() {
    return networkEmulator;
  }

  @Override
  public final Mono<Void> stop() {
    return Mono.fromRunnable(
        () -> {
          if (stopped) {
            throw new IllegalStateException("Transport is stopped");
          }
          stopped = true;
          // Complete incoming messages observable
          try {
            messageSink.complete();
          } catch (Exception ignore) {
            // ignore
          }

          /*
          // close connected channels
          for (Address address : outgoingChannels.keySet()) {
            ChannelFuture channelFuture = outgoingChannels.get(address);
            if (channelFuture == null) {
              continue;
            }
            if (channelFuture.isSuccess()) {
              channelFuture.channel().close();
            } else {
              channelFuture.addListener(ChannelFutureListener.CLOSE);
            }
          }
          outgoingChannels.clear();

          // close server channel
          if (serverChannel != null) {
            serverChannel.close().awaitUninterruptibly(); // TODO [AV]: check this
          }
          */

          // TODO [AK]: shutdown boss/worker threads and listen for their futures
          bootstrapFactory.shutdown();
        });
  }

  @Override
  public final Flux<Message> listen() {
    return incomingMessagesSubject.onBackpressureBuffer();
  }

  @Override
  public Mono<Void> send(Address address, Message message) {
    return Mono.create(
        sink -> {
          if (stopped) {
            throw new IllegalStateException("Transport is stopped");
          }
          Objects.requireNonNull(address);
          Objects.requireNonNull(message);

          message.setSender(this.address); // set local address as outgoing address

          outgoingChannels
              .computeIfAbsent(address, this::connect)
              .subscribe(channel -> send(channel, message, sink), sink::error);
        });
  }

  private void send(Channel channel, Message message, MonoSink<Void> sink) {
    channel
        .writeAndFlush(message)
        .addListener(
            future -> {
              if (future.isSuccess()) {
                sink.success();
              } else {
                sink.error(future.cause());
              }
            });
  }

  private Mono<Channel> connect(Address address) {
    return Mono.<Channel>create(
            sink -> {
              // Register logger and cleanup listener
              bootstrapFactory
                  .clientBootstrap()
                  .handler(new OutgoingChannelInitializer(address))
                  .connect(address.host(), address.port())
                  .addListener(
                      future -> {
                        if (future.isSuccess()) {
                          Channel channel = ((ChannelFuture) future).channel();
                          LOGGER.debug(
                              "Connected from {} to {}: {}",
                              TransportImpl.this.address,
                              address,
                              channel);
                          sink.success(channel);
                        } else {
                          LOGGER.warn(
                              "Failed to connect from {} to {}",
                              TransportImpl.this.address,
                              address);
                          outgoingChannels.remove(address);
                          sink.error(future.cause());
                        }
                      });
            })
        .cache();
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
}
