package io.scalecube.rsocket.transport;

import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import io.rsocket.util.ByteBufPayload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.server.HttpServer;
import reactor.ipc.netty.resources.LoopResources;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

public class RsocketTransportImpl implements RsocketTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RsocketTransportImpl.class);

  private final DirectProcessor<Message> subject = DirectProcessor.create();
  private final AtomicReference<NettyContextCloseable> server = new AtomicReference<>();
  private final LoopResources loopResources = LoopResources.create("rsocket");
  private final RSocketClientTransport rSocketClientTransport = new RSocketClientTransport(loopResources);

  public Mono<Void> bind(InetSocketAddress address) {
    HttpServer httpServer = HttpServer.create(options -> options
        .listenAddress(address)
        .loopResources(loopResources));

    WebsocketServerTransport transport = WebsocketServerTransport.create(httpServer);

    return RSocketFactory.receive()
        .frameDecoder(frame -> ByteBufPayload.create(frame.sliceData().retain(), frame.sliceMetadata().retain()))
        .acceptor(new RSocketAcceptor(subject))
        .transport(transport)
        .start().doOnSuccess(server -> {
          if (this.server.compareAndSet(null, server)) {
            LOGGER.info("Bound to: {}", server.address());
          } else {
            server.dispose();
            throw new IllegalStateException("server is already running");
          }
        })
        .then();
  }

  @Override
  public Address address() {
    NettyContextCloseable server = this.server.get();
    if (server == null) {
      throw new IllegalStateException("server not running");
    }
    return Address.create(server.address().getHostName(), server.address().getPort());
  }

  @Override
  public Mono<Void> send(Address address, Message message) {
    return rSocketClientTransport.fireAndForget(address, message);
  }

  @Override
  public Flux<Message> listen() {
    return subject;
  }

  @Override
  public Mono<Void> stop() {
    return Mono.defer(() -> {
      server.getAndUpdate(server -> {
        if (server != null) {
          server.dispose();
        }
        return null;
      });
      return Mono.when(rSocketClientTransport.close(), loopResources.disposeLater());
    });
  }

  @Override
  public boolean isStopped() {
    return server.get() == null;
  }

  @Override
  public NetworkEmulator networkEmulator() {
    return null;
  }
}
