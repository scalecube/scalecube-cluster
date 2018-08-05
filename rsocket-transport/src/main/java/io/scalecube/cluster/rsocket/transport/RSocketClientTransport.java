package io.scalecube.cluster.rsocket.transport;

import io.scalecube.cluster.transport.api.Address;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.MessageCodec;
import io.scalecube.cluster.transport.api.NetworkEmulatorHandler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.util.ByteBufPayload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.resources.LoopResources;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class RSocketClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketClientTransport.class);

  private final LoopResources loopResources;
  private final NetworkEmulatorHandler networkEmulatorHandler;
  private final Map<Address, Mono<RSocket>> rSockets = new ConcurrentHashMap<>();

  public RSocketClientTransport(LoopResources loopResources, NetworkEmulatorHandler networkEmulatorHandler) {
    this.loopResources = loopResources;
    this.networkEmulatorHandler = networkEmulatorHandler;
  }

  public Mono<Void> fireAndForget(Address address, Message message) {
    System.err.println("invoked client fireAndForget");
    return getOrConnect(address)
        .map(rSocket -> rSocket.fireAndForget(toPayload(message))
            .takeUntilOther(listenConnectionClose(rSocket)))
        .then().log("client ------> ");
  }

  public Mono<Void> requestResponse(Address address, Message message) {
    System.err.println("invoked client requestResponse");
    return getOrConnect(address)
        .map(rSocket -> rSocket.requestResponse(toPayload(message))
            .takeUntilOther(listenConnectionClose(rSocket)))
        .then().log("client ------> ");
  }

  public Mono<Void> close() {
    return Mono.defer(() -> {
      rSockets.forEach((address, rSocketMono) -> {
        rSocketMono.subscribe(Disposable::dispose);
      });
      rSockets.clear();
      return Mono.empty();
    });
  }

  private Mono<RSocket> getOrConnect(Address address) {
    // noinspection unchecked
    return rSockets.computeIfAbsent(address, key -> Mono.defer(() -> {

      InetSocketAddress socketAddress = InetSocketAddress.createUnresolved(key.host(), key.port());

      return RSocketFactory.connect()
          // .metadataMimeType(settings.contentType())
          .frameDecoder(frame -> ByteBufPayload.create(frame.sliceData().retain(), frame.sliceMetadata().retain()))
          .keepAlive()
          .transport(createRSocketTransport(socketAddress))
          .start()
          .doOnSuccess(rSocket -> {
            LOGGER.info("Connected successfully on {}", socketAddress);
            // setup shutdown hook
            rSocket.onClose().doOnTerminate(() -> {
              rSockets.remove(key); // clean reference
              LOGGER.info("Connection closed on {}", socketAddress);
            }).subscribe();
          })
          .doOnError(throwable -> {
            LOGGER.warn("Connect failed on {}, cause: {}", socketAddress, throwable);
            rSockets.remove(key); // clean reference
          })
          .cache();
    }));
  }


  private WebsocketClientTransport createRSocketTransport(InetSocketAddress address) {
    return WebsocketClientTransport.create(HttpClient.create(options -> options.disablePool()
        .afterChannelInit(channel -> {
          if (networkEmulatorHandler != null) {
            channel.pipeline().addLast(networkEmulatorHandler);
          }
        })
        .connectAddress(() -> address)
        .loopResources(LoopResources.create("CLIENT"))), "/");
    // .loopResources(loopResources)), "/");
  }

  private Payload toPayload(Message message) {
    ByteBuf data = ByteBufAllocator.DEFAULT.buffer();
    MessageCodec.serialize(message, data);
    return ByteBufPayload.create(data);
  }

  @SuppressWarnings("unchecked")
  private <T> Mono<T> listenConnectionClose(RSocket rSocket) {
    return rSocket.onClose()
        .map(aVoid -> (T) aVoid)
        .switchIfEmpty(Mono.defer(this::toConnectionClosedException));
  }

  private <T> Mono<T> toConnectionClosedException() {
    return Mono.error(new ConnectionClosedException("Connection closed"));
  }
}
