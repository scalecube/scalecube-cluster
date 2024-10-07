package io.scalecube.transport.netty;

import static reactor.core.publisher.Sinks.EmitFailureHandler.busyLooping;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;
import io.netty.util.ReferenceCountUtil;
import io.scalecube.cluster.transport.api.DistinctErrors;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.MessageCodec;
import io.scalecube.cluster.transport.api.Transport;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.resources.LoopResources;

public final class TransportImpl implements Transport {

  private static final Logger LOGGER = System.getLogger(Transport.class.getName());

  private static final DistinctErrors DISTINCT_ERRORS = new DistinctErrors(Duration.ofMinutes(1));

  private final MessageCodec messageCodec;

  // Subject
  private final Sinks.Many<Message> sink = Sinks.many().multicast().directBestEffort();

  // Close handler
  private final Sinks.One<Void> stop = Sinks.one();
  private final Sinks.One<Void> onStop = Sinks.one();

  // Server
  private String address;
  private DisposableServer server;
  private final Map<String, Mono<? extends Connection>> connections = new ConcurrentHashMap<>();
  private final LoopResources loopResources = LoopResources.create("sc-cluster-io", 1, true);

  // Transport factory
  private final Receiver receiver;
  private final Sender sender;
  private final Function<String, String> addressMapper;

  /**
   * Constructor with config as parameter.
   *
   * @param messageCodec message codec
   * @param receiver transport receiver part
   * @param sender transport sender part
   * @param addressMapper function to map addresses. Useful when running against NAT-ed
   *     environments. Used during connection setup so that the actual connection is established
   *     against <code>addressMapper.apply(origAddress) destination</code>
   */
  public TransportImpl(
      MessageCodec messageCodec,
      Receiver receiver,
      Sender sender,
      Function<String, String> addressMapper) {
    this.messageCodec = messageCodec;
    this.receiver = receiver;
    this.sender = sender;
    this.addressMapper = addressMapper;
  }

  private static String prepareAddress(DisposableServer server) {
    final InetSocketAddress serverAddress = (InetSocketAddress) server.address();
    final InetAddress inetAddress = serverAddress.getAddress();
    final int port = serverAddress.getPort();

    if (inetAddress.isAnyLocalAddress()) {
      return getLocalHostAddress() + ":" + port;
    } else {
      return inetAddress.getHostAddress() + ":" + port;
    }
  }

  private static String getLocalHostAddress() {
    try {
      return InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  private void init(DisposableServer server) {
    this.server = server;
    this.address = prepareAddress(server);
    // Setup cleanup
    stop.asMono()
        .then(doStop())
        .doFinally(s -> onStop.emitEmpty(busyLooping(Duration.ofSeconds(3))))
        .subscribe(
            null,
            ex ->
                LOGGER.log(
                    Level.WARNING,
                    "[{0}][doStop] Exception occurred: {1}",
                    address,
                    ex.toString()));
  }

  /**
   * Starts to accept connections on local address.
   *
   * @return mono transport
   */
  @Override
  public Mono<Transport> start() {
    return Mono.deferContextual(context -> receiver.bind())
        .doOnNext(this::init)
        .doOnSuccess(
            t -> LOGGER.log(Level.INFO, "[start][{0}] Bound cluster transport", t.address()))
        .doOnError(
            ex ->
                LOGGER.log(
                    Level.ERROR, "[start][{0}] Exception occurred: {1}", address, ex.toString()))
        .thenReturn(this)
        .cast(Transport.class)
        .contextWrite(
            context ->
                context.put(
                    ReceiverContext.class,
                    new ReceiverContext(address, sink, loopResources, this::decodeMessage)));
  }

  @Override
  public String address() {
    return address;
  }

  @Override
  public boolean isStopped() {
    return onStop.asMono().toFuture().isDone();
  }

  @Override
  public Mono<Void> stop() {
    return Mono.defer(
        () -> {
          stop.emitEmpty(busyLooping(Duration.ofSeconds(3)));
          return onStop.asMono();
        });
  }

  private Mono<Void> doStop() {
    return Mono.defer(
        () -> {
          LOGGER.log(Level.INFO, "[{0}][doStop] Stopping", address);
          // Complete incoming messages observable
          sink.emitComplete(busyLooping(Duration.ofSeconds(3)));
          return Flux.concatDelayError(closeServer(), shutdownLoopResources())
              .then()
              .doFinally(s -> connections.clear())
              .doOnSuccess(avoid -> LOGGER.log(Level.INFO, "[{0}][doStop] Stopped", address));
        });
  }

  @Override
  public Flux<Message> listen() {
    return sink.asFlux().onBackpressureBuffer();
  }

  @Override
  public Mono<Void> send(String address, Message message) {
    return Mono.deferContextual(context -> connections.computeIfAbsent(address, this::connect))
        .flatMap(
            connection ->
                Mono.deferContextual(context -> sender.send(message))
                    .contextWrite(context -> context.put(Connection.class, connection)))
        .contextWrite(
            context ->
                context.put(
                    SenderContext.class, new SenderContext(loopResources, this::encodeMessage)));
  }

  @Override
  public Mono<Message> requestResponse(String address, final Message request) {
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

  private Message decodeMessage(ByteBuf byteBuf) {
    try (ByteBufInputStream stream = new ByteBufInputStream(byteBuf, true)) {
      return messageCodec.deserialize(stream);
    } catch (Exception e) {
      if (!DISTINCT_ERRORS.contains(e)) {
        LOGGER.log(
            Level.WARNING, "[{0}][decodeMessage] Exception occurred: {1}", address, e.toString());
      }
      throw new DecoderException(e);
    }
  }

  private ByteBuf encodeMessage(Message message) {
    ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
    ByteBufOutputStream stream = new ByteBufOutputStream(byteBuf);
    try {
      messageCodec.serialize(message, stream);
    } catch (Exception e) {
      byteBuf.release();
      if (!DISTINCT_ERRORS.contains(e)) {
        LOGGER.log(
            Level.WARNING, "[{0}][encodeMessage] Exception occurred: {1}", address, e.toString());
      }
      throw new EncoderException(e);
    }
    return byteBuf;
  }

  private Mono<? extends Connection> connect(String remoteAddress) {
    final String mappedAddr = addressMapper.apply(remoteAddress);
    return sender
        .connect(mappedAddr)
        .doOnSuccess(
            connection -> {
              connection
                  .onDispose()
                  .doOnTerminate(() -> connections.remove(remoteAddress))
                  .subscribe();
              LOGGER.log(
                  Level.DEBUG,
                  "[{0}][connect][success] remoteAddress: {1}, channel: {2}",
                  address,
                  remoteAddress,
                  connection.channel());
            })
        .doOnError(
            th -> {
              if (!DISTINCT_ERRORS.contains(th)) {
                LOGGER.log(
                    Level.WARNING,
                    "[{0}][connect][error] remoteAddress: {1}, cause: {2}",
                    address,
                    remoteAddress,
                    th.toString());
              }
              connections.remove(remoteAddress);
            })
        .cache();
  }

  private Mono<Void> closeServer() {
    return Mono.defer(
        () -> {
          if (server == null) {
            return Mono.empty();
          }
          LOGGER.log(Level.INFO, "[{0}][closeServer] Closing server channel", address);
          return Mono.fromRunnable(server::dispose)
              .then(server.onDispose())
              .doOnSuccess(
                  avoid ->
                      LOGGER.log(Level.INFO, "[{0}][closeServer] Closed server channel", address))
              .doOnError(
                  e ->
                      LOGGER.log(
                          Level.WARNING,
                          "[{0}][closeServer] Exception occurred: {1}",
                          address,
                          e.toString()));
        });
  }

  private Mono<Void> shutdownLoopResources() {
    return Mono.fromRunnable(loopResources::dispose).then(loopResources.disposeLater());
  }

  public static final class ReceiverContext {

    private final String address;
    private final Sinks.Many<Message> sink;
    private final LoopResources loopResources;
    private final Function<ByteBuf, Message> messageDecoder;

    private ReceiverContext(
        String address,
        Sinks.Many<Message> sink,
        LoopResources loopResources,
        Function<ByteBuf, Message> messageDecoder) {
      this.address = address;
      this.sink = sink;
      this.loopResources = loopResources;
      this.messageDecoder = messageDecoder;
    }

    public LoopResources loopResources() {
      return loopResources;
    }

    /**
     * Inbound message handler method. Filters out junk byteBufs, decodes accepted byteBufs into
     * messages, and then publishing messages on sink instance.
     *
     * @param byteBuf byteBuf
     */
    public void onMessage(ByteBuf byteBuf) {
      try {
        if (byteBuf == Unpooled.EMPTY_BUFFER) {
          return;
        }
        if (!byteBuf.isReadable()) {
          ReferenceCountUtil.safeRelease(byteBuf);
          return;
        }
        final Message message = messageDecoder.apply(byteBuf);
        sink.emitNext(message, busyLooping(Duration.ofSeconds(3)));
      } catch (Exception e) {
        LOGGER.log(Level.ERROR, "[{0}][onMessage] Exception occurred", address, e);
      }
    }
  }

  public static final class SenderContext {

    private final LoopResources loopResources;
    private final Function<Message, ByteBuf> messageEncoder;

    private SenderContext(LoopResources loopResources, Function<Message, ByteBuf> messageEncoder) {
      this.loopResources = loopResources;
      this.messageEncoder = messageEncoder;
    }

    public LoopResources loopResources() {
      return loopResources;
    }

    public Function<Message, ByteBuf> messageEncoder() {
      return messageEncoder;
    }
  }
}
