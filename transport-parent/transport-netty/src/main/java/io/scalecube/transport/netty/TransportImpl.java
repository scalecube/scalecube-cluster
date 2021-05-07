package io.scalecube.transport.netty;

import static io.scalecube.cluster.RetryNotSerializedEmitFailureHandler.RETRY_NOT_SERIALIZED;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;
import io.netty.util.ReferenceCountUtil;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.MessageCodec;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.net.Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.resources.LoopResources;

public final class TransportImpl implements Transport {

  private static final Logger LOGGER = LoggerFactory.getLogger(Transport.class);

  private final MessageCodec messageCodec;

  // Subject
  private final Sinks.Many<Message> sink = Sinks.many().multicast().directBestEffort();

  // Close handler
  private final Sinks.One<Void> stop = Sinks.one();
  private final Sinks.One<Void> onStop = Sinks.one();

  // Server
  private Address address;
  private DisposableServer server;
  private final Map<Address, Mono<? extends Connection>> connections = new ConcurrentHashMap<>();
  private final LoopResources loopResources = LoopResources.create("sc-cluster-io", 1, true);

  // Transport factory
  private final Receiver receiver;
  private final Sender sender;

  /**
   * Constructor with cofig as parameter.
   *
   * @param messageCodec message codec
   * @param receiver transport receiver part
   * @param sender transport sender part
   */
  public TransportImpl(MessageCodec messageCodec, Receiver receiver, Sender sender) {
    this.messageCodec = messageCodec;
    this.receiver = receiver;
    this.sender = sender;
  }

  private static Address prepareAddress(DisposableServer server) {
    final InetSocketAddress serverAddress = (InetSocketAddress) server.address();
    InetAddress inetAddress = serverAddress.getAddress();
    int port = serverAddress.getPort();
    if (inetAddress.isAnyLocalAddress()) {
      return Address.create(Address.getLocalIpAddress().getHostAddress(), port);
    } else {
      return Address.create(inetAddress.getHostAddress(), port);
    }
  }

  private void init(DisposableServer server) {
    this.server = server;
    this.address = prepareAddress(server);
    // Setup cleanup
    stop.asMono()
        .then(doStop())
        .doFinally(s -> onStop.emitEmpty(RETRY_NOT_SERIALIZED))
        .subscribe(
            null, ex -> LOGGER.warn("[{}][doStop] Exception occurred: {}", address, ex.toString()));
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
        .doOnSuccess(t -> LOGGER.info("[start][{}] Bound cluster transport", t.address()))
        .doOnError(ex -> LOGGER.error("[start][{}] Exception occurred: {}", address, ex.toString()))
        .thenReturn(this)
        .cast(Transport.class)
        .contextWrite(
            context ->
                context.put(
                    ReceiverContext.class,
                    new ReceiverContext(address, sink, loopResources, this::decodeMessage)));
  }

  @Override
  public Address address() {
    return address;
  }

  @Override
  public boolean isStopped() {
    return onStop.asMono().toFuture().isDone();
  }

  @Override
  public final Mono<Void> stop() {
    return Mono.defer(
        () -> {
          stop.emitEmpty(RETRY_NOT_SERIALIZED);
          return onStop.asMono();
        });
  }

  private Mono<Void> doStop() {
    return Mono.defer(
        () -> {
          LOGGER.info("[{}][doStop] Stopping", address);
          // Complete incoming messages observable
          sink.emitComplete(RETRY_NOT_SERIALIZED);
          return Flux.concatDelayError(closeServer(), shutdownLoopResources())
              .then()
              .doFinally(s -> connections.clear())
              .doOnSuccess(avoid -> LOGGER.info("[{}][doStop] Stopped", address));
        });
  }

  @Override
  public final Flux<Message> listen() {
    return sink.asFlux().onBackpressureBuffer();
  }

  @Override
  public Mono<Void> send(Address address, Message message) {
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

  private Message decodeMessage(ByteBuf byteBuf) {
    try (ByteBufInputStream stream = new ByteBufInputStream(byteBuf, true)) {
      return messageCodec.deserialize(stream);
    } catch (Exception e) {
      LOGGER.warn("[{}][decodeMessage] Exception occurred: {}", address, e.toString());
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
      LOGGER.warn("[{}][encodeMessage] Exception occurred: {}", address, e.toString());
      throw new EncoderException(e);
    }
    return byteBuf;
  }

  private Mono<? extends Connection> connect(Address remoteAddress) {
    return sender
        .connect(remoteAddress)
        .doOnSuccess(
            connection -> {
              connection
                  .onDispose()
                  .doOnTerminate(() -> connections.remove(remoteAddress))
                  .subscribe();
              LOGGER.debug(
                  "[{}][connect][success] remoteAddress: {}, channel: {}",
                  address,
                  remoteAddress,
                  connection.channel());
            })
        .doOnError(
            th -> {
              LOGGER.warn(
                  "[{}][connect][error] remoteAddress: {}, cause: {}",
                  address,
                  remoteAddress,
                  th.toString());
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
          LOGGER.info("[{}][closeServer] Closing server channel", address);
          return Mono.fromRunnable(server::dispose)
              .then(server.onDispose())
              .doOnSuccess(avoid -> LOGGER.info("[{}][closeServer] Closed server channel", address))
              .doOnError(
                  e ->
                      LOGGER.warn(
                          "[{}][closeServer] Exception occurred: {}", address, e.toString()));
        });
  }

  private Mono<Void> shutdownLoopResources() {
    return Mono.fromRunnable(loopResources::dispose).then(loopResources.disposeLater());
  }

  public static final class ReceiverContext {

    private final Address address;
    private final Sinks.Many<Message> sink;
    private final LoopResources loopResources;
    private final Function<ByteBuf, Message> messageDecoder;

    private ReceiverContext(
        Address address,
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
        sink.emitNext(message, RETRY_NOT_SERIALIZED);
      } catch (Exception e) {
        LOGGER.error("[{}][onMessage] Exception occurred:", address, e);
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
