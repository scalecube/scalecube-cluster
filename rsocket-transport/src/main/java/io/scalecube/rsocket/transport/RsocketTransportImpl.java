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

public class RsocketTransportImpl implements RsocketTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RsocketTransportImpl.class);

  private final DirectProcessor<Message> listen = DirectProcessor.create();

  private InetSocketAddress address;
  private LoopResources loopResources;
  private NettyContextCloseable server;

  private volatile boolean stopped = false;

  public void bind() {
    InetSocketAddress address =
        InetSocketAddress.createUnresolved(this.address.getHostName(), this.address.getPort());

    loopResources = LoopResources.create("rsocket-transport");

    HttpServer httpServer = HttpServer.create(options -> options
        .listenAddress(address)
        .loopResources(loopResources));

    WebsocketServerTransport transport = WebsocketServerTransport.create(httpServer);

    server = RSocketFactory.receive()
        .frameDecoder(frame -> ByteBufPayload.create(frame.sliceData().retain(), frame.sliceMetadata().retain()))
        .acceptor(new RSocketAcceptor(listen))
        .transport(transport)
        .start()
        .block();

    LOGGER.info("Bound to: {}", server.address());
  }

  @Override
  public Address address() {
    return Address.create(server.address().getHostName(), server.address().getPort());
  }

  @Override
  public Mono<Void> send(Address address, Message message) {
    socketAcceptor.accept()
    return null;
  }

  @Override
  public Flux<Message> listen() {
    return listen;
  }

  @Override
  public Mono<Void> stop() {
    return null;
  }

  @Override
  public boolean isStopped() {
    return false;
  }

  @Override
  public NetworkEmulator networkEmulator() {
    return null;
  }
}
