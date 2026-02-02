package io.scalecube.transport.netty.http;

import static io.scalecube.cluster.transport.api.Transport.parseHost;
import static io.scalecube.cluster.transport.api.Transport.parsePort;
import static reactor.core.publisher.Sinks.EmitFailureHandler.busyLooping;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.transport.api.TransportConfig;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.DisposableServer;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.server.HttpServer;

public final class HttpTransport implements Transport {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpTransport.class);
  private static final reactor.netty.resources.LoopResources LOOP_RESOURCES =
      reactor.netty.resources.LoopResources.create("sc-cluster-io");

  private final TransportConfig config;
  private final Sinks.Many<Message> sink = Sinks.many().multicast().directBestEffort();

  // Map to hold HttpClient and ConnectionProvider per destination address
  private final Map<String, ClientContext> clientContexts = new ConcurrentHashMap<>();

  private DisposableServer server;
  private String address;

  public HttpTransport(TransportConfig config) {
    this.config = config;
  }

  @Override
  public String address() {
    return address;
  }

  @Override
  public Mono<Transport> start() {
    return HttpServer.create()
        .runOn(LOOP_RESOURCES)
        .port(config.port())
        .childOption(ChannelOption.TCP_NODELAY, true)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(ChannelOption.SO_REUSEADDR, true)
        .handle(
            (request, response) ->
                request
                    .receive()
                    .aggregate()
                    .retain()
                    .doOnNext(this::onMessage)
                    .then(response.status(200).send().then()))
        .bind()
        .doOnNext(this::init)
        .thenReturn(this);
  }

  private void init(DisposableServer server) {
    this.server = server;
    this.address = prepareAddress(server);
    LOGGER.info("[start][{}] Bound cluster transport", address);
  }

  @Override
  public Mono<Void> stop() {
    return Mono.defer(
        () -> {
          if (server == null) {
            return Mono.empty();
          }
          sink.emitComplete(busyLooping(Duration.ofSeconds(3)));
          server.dispose();
          return server
              .onDispose()
              .doOnTerminate(
                  () -> {
                    clientContexts.values().forEach(ctx -> ctx.connectionProvider.dispose());
                    clientContexts.clear();
                  })
              .doOnSuccess(avoid -> LOGGER.info("[stop][{}] Stopped", address));
        });
  }

  @Override
  public boolean isStopped() {
    return server != null && server.isDisposed();
  }

  @Override
  public Mono<Void> send(String address, Message message) {
    return clientContexts
        .computeIfAbsent(address, this::createClientContext)
        .httpClient
        .post()
        .uri("/")
        .send(Mono.fromCallable(() -> encodeMessage(message)))
        .responseContent()
        .then();
  }

  private ClientContext createClientContext(String address) {
    reactor.netty.resources.ConnectionProvider connectionProvider =
        reactor.netty.resources.ConnectionProvider.builder("http-client-" + address)
            .maxConnections(1)
            .pendingAcquireMaxCount(-1)
            .build();

    HttpClient httpClient =
        HttpClient.create(connectionProvider)
            .runOn(LOOP_RESOURCES)
            .host(parseHost(address))
            .port(parsePort(address))
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.SO_REUSEADDR, true)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.connectTimeout())
            .headers(headers -> headers.set("Content-Type", "application/octet-stream"));

    if (config.isClientSecured()) {
      httpClient = httpClient.secure();
    }

    return new ClientContext(httpClient, connectionProvider);
  }

  private static class ClientContext {
    final HttpClient httpClient;
    final reactor.netty.resources.ConnectionProvider connectionProvider;

    ClientContext(
        HttpClient httpClient, reactor.netty.resources.ConnectionProvider connectionProvider) {
      this.httpClient = httpClient;
      this.connectionProvider = connectionProvider;
    }
  }

  @Override
  public Mono<Message> requestResponse(String address, Message request) {
    return Mono.create(
        sink -> {
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

  @Override
  public Flux<Message> listen() {
    return sink.asFlux().onBackpressureBuffer();
  }

  private void onMessage(ByteBuf byteBuf) {
    try {
      if (byteBuf == Unpooled.EMPTY_BUFFER) {
        return;
      }
      if (!byteBuf.isReadable()) {
        return;
      }
      final Message message = decodeMessage(byteBuf);
      sink.emitNext(message, busyLooping(Duration.ofSeconds(3)));
    } catch (Exception e) {
      LOGGER.error("[{}][onMessage] Exception occurred: {}", address, e.toString());
    }
  }

  private Message decodeMessage(ByteBuf byteBuf) {
    try (ByteBufInputStream stream = new ByteBufInputStream(byteBuf, true)) {
      return config.messageCodec().deserialize(stream);
    } catch (Exception e) {
      throw new DecoderException(e);
    }
  }

  private ByteBuf encodeMessage(Message message) {
    ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
    ByteBufOutputStream stream = new ByteBufOutputStream(byteBuf);
    try {
      config.messageCodec().serialize(message, stream);
    } catch (Exception e) {
      byteBuf.release();
      throw new EncoderException(e);
    }
    return byteBuf;
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
}
