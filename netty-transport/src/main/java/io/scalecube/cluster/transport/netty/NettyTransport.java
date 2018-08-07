package io.scalecube.cluster.transport.netty;

import static io.scalecube.cluster.transport.Addressing.MAX_PORT_NUMBER;

import io.scalecube.cluster.transport.Address;
import io.scalecube.cluster.transport.Addressing;
import io.scalecube.cluster.transport.Message;
import io.scalecube.cluster.transport.NetworkEmulator;
import io.scalecube.cluster.transport.NetworkEmulatorHandler;
import io.scalecube.cluster.transport.Transport;
import io.scalecube.cluster.transport.TransportConfig;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ServerChannel;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;

import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

final class NettyTransport implements Transport {

  private static final Logger LOGGER = LoggerFactory.getLogger(NettyTransport.class);
  private static final CompletableFuture<Void> COMPLETED_PROMISE = CompletableFuture.completedFuture(null);

  private final TransportConfig config;

  // SUbject

  private final FluxProcessor<Message, Message> incomingMessagesSubject =
      DirectProcessor.<Message>create().serialize();

  private final FluxSink<Message> messageSink = incomingMessagesSubject.sink();

  private final Map<Address, ChannelFuture> outgoingChannels = new ConcurrentHashMap<>();

  // Pipeline
  private final BootstrapFactory bootstrapFactory;
  private final IncomingChannelInitializer incomingChannelInitializer = new IncomingChannelInitializer();
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

  public NettyTransport(TransportConfig config) {
    this.config = Objects.requireNonNull(config);
    this.serializerHandler = new MessageSerializerHandler();
    this.deserializerHandler = new MessageDeserializerHandler();
    this.messageHandler = new MessageHandler(messageSink);
    this.bootstrapFactory = new BootstrapFactory(config);
  }

  /**
   * Starts to accept connections on local address.
   */
  @Override
  public CompletableFuture<Transport> start() {
    ServerBootstrap server = bootstrapFactory.serverBootstrap().childHandler(incomingChannelInitializer);

    // Resolve listen IP address
    InetAddress listenAddress =
        Addressing.getLocalIpAddress(config.getListenAddress(), config.getListenInterface(), config.isPreferIPv6());

    // Listen port
    int bindPort = config.getPort();

    return bind0(server, listenAddress, bindPort, bindPort + config.getPortCount());
  }

  /**
   * Helper bind method to start accepting connections on {@code listenAddress} and {@code bindPort}.
   *
   * @param bindPort bind port.
   * @param finalBindPort maximum port to bind.
   * @throws NoSuchElementException if {@code bindPort} greater than {@code finalBindPort}.
   * @throws IllegalArgumentException if {@code bindPort} doesnt belong to the range [{@link Addressing#MIN_PORT_NUMBER}
   *         .. {@link Addressing#MAX_PORT_NUMBER}].
   */
  private CompletableFuture<Transport> bind0(ServerBootstrap server, InetAddress listenAddress, int bindPort,
      int finalBindPort) {

    final CompletableFuture<Transport> result = new CompletableFuture<>();

    // Perform basic bind port validation
    if (bindPort > MAX_PORT_NUMBER) {
      result.completeExceptionally(
          new IllegalArgumentException("Invalid port number: " + bindPort));
      return result;
    }
    if (bindPort > finalBindPort) {
      result.completeExceptionally(
          new NoSuchElementException("Could not find an available port from: " + bindPort + " to: " + finalBindPort));
      return result;
    }

    // Get address object and bind
    address = Address.create(listenAddress.getHostAddress(), bindPort);
    ChannelFuture bindFuture = server.bind(listenAddress, address.port());
    bindFuture.addListener((ChannelFutureListener) channelFuture -> {
      if (channelFuture.isSuccess()) {
        serverChannel = (ServerChannel) channelFuture.channel();
        address = toAddress(serverChannel.localAddress());
        networkEmulator = new NetworkEmulator(address, config.isUseNetworkEmulator());
        networkEmulatorHandler = config.isUseNetworkEmulator() ? new NetworkEmulatorHandler(networkEmulator) : null;
        LOGGER.info("Bound to: {}", address);
        result.complete(NettyTransport.this);
      } else {
        Throwable cause = channelFuture.cause();
        if (config.isPortAutoIncrement() && isAddressAlreadyInUseException(cause)) {
          LOGGER.warn("Can't bind to address {}, try again on different port [cause={}]", address, cause.toString());
          bind0(server, listenAddress, bindPort + 1, finalBindPort).thenAccept(result::complete);
        } else {
          LOGGER.error("Failed to bind to: {}, cause: {}", address, cause);
          result.completeExceptionally(cause);
        }
      }
    });
    return result;
  }

  private boolean isAddressAlreadyInUseException(Throwable exception) {
    return exception instanceof BindException
        || (exception.getMessage() != null && exception.getMessage().contains("Address already in use"));
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
  public final void stop() {
    stop(COMPLETED_PROMISE);
  }

  @Override
  public final void stop(CompletableFuture<Void> promise) {
    if (stopped) {
      throw new IllegalStateException("Transport is stopped");
    }
    Objects.requireNonNull(promise);
    stopped = true;
    // Complete incoming messages observable
    try {
      messageSink.complete();
    } catch (Exception ignore) {
      // ignore
    }

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
      composeFutures(serverChannel.close(), promise);
    }

    // TODO [AK]: shutdown boss/worker threads and listen for their futures
    bootstrapFactory.shutdown();
  }


  @Override
  public final Flux<Message> listen() {
    if (stopped) {
      throw new IllegalStateException("Transport is stopped");
    }
    return incomingMessagesSubject.onBackpressureBuffer();
  }

  @Override
  public void send(Address address, Message message) {
    send(address, message, COMPLETED_PROMISE);
  }

  @Override
  public void send(Address address, Message message,
      CompletableFuture<Void> promise) {
    if (stopped) {
      throw new IllegalStateException("Transport is stopped");
    }
    Objects.requireNonNull(address);
    Objects.requireNonNull(message);
    Objects.requireNonNull(promise);
    Message target = message.withSender(this.address);

    final ChannelFuture channelFuture = outgoingChannels.computeIfAbsent(address, this::connect);

    if (channelFuture.isSuccess()) {
      send(channelFuture.channel(), target, promise);
    } else {
      channelFuture.addListener((ChannelFuture chFuture) -> {
        if (chFuture.isSuccess()) {
          send(channelFuture.channel(), target, promise);
        } else {
          promise.completeExceptionally(chFuture.cause());
        }
      });
    }
  }

  private void send(Channel channel, Message message, CompletableFuture<Void> promise) {
    if (promise.equals(COMPLETED_PROMISE)) {
      channel.writeAndFlush(message, channel.voidPromise());
    } else {
      composeFutures(channel.writeAndFlush(message), promise);
    }
  }

  /**
   * Converts netty {@link ChannelFuture} to the given {@link CompletableFuture}.
   *
   * @param channelFuture netty channel future
   * @param promise future; can be null
   */
  private void composeFutures(ChannelFuture channelFuture, final CompletableFuture<Void> promise) {
    channelFuture.addListener((ChannelFuture future) -> {
      if (channelFuture.isSuccess()) {
        promise.complete(channelFuture.get());
      } else {
        promise.completeExceptionally(channelFuture.cause());
      }
    });
  }

  private ChannelFuture connect(Address address) {
    OutgoingChannelInitializer channelInitializer = new OutgoingChannelInitializer(address);
    Bootstrap client = bootstrapFactory.clientBootstrap().handler(channelInitializer);
    ChannelFuture connectFuture = client.connect(address.host(), address.port());

    // Register logger and cleanup listener
    connectFuture.addListener((ChannelFutureListener) channelFuture -> {
      if (channelFuture.isSuccess()) {
        LOGGER.debug("Connected from {} to {}: {}", NettyTransport.this.address, address, channelFuture.channel());
      } else {
        LOGGER.warn("Failed to connect from {} to {}", NettyTransport.this.address, address);
        outgoingChannels.remove(address);
      }
    });
    return connectFuture;
  }

  @ChannelHandler.Sharable
  private final class IncomingChannelInitializer extends ChannelInitializer {
    @Override
    protected void initChannel(Channel channel) throws Exception {
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
    protected void initChannel(Channel channel) throws Exception {
      ChannelPipeline pipeline = channel.pipeline();
      pipeline.addLast(new ChannelDuplexHandler() {
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
