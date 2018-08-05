package io.scalecube.cluster.rsocket.transport;

import static io.scalecube.cluster.transport.api.Addressing.MAX_PORT_NUMBER;
import static io.scalecube.cluster.transport.api.Addressing.MIN_PORT_NUMBER;

import io.scalecube.cluster.transport.api.Address;
import io.scalecube.cluster.transport.api.Addressing;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.NetworkEmulator;
import io.scalecube.cluster.transport.api.NetworkEmulatorHandler;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.transport.api.TransportConfig;

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

import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

public class RSocketTransport implements Transport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketTransport.class);

  private final TransportConfig config;

  private final DirectProcessor<Message> subject = DirectProcessor.create();
  private final AtomicReference<NettyContextCloseable> server = new AtomicReference<>();

  private LoopResources loopResources;
  private RSocketClientTransport rSocketClientTransport;
  private NetworkEmulator networkEmulator;

  public RSocketTransport(TransportConfig config) {
    this.config = config;
  }

  @Override
  public Mono<Transport> start() {
    // Resolve listen IP address
    InetAddress listenAddress =
        Addressing.getLocalIpAddress(config.getListenAddress(), config.getListenInterface(), config.isPreferIPv6());

    this.loopResources = LoopResources.create("rsocket8888", config.getBossThreads(), config.getWorkerThreads(), true);

    // Listen port
    int bindPort = config.getPort();

    return bind(listenAddress, loopResources, bindPort, bindPort + config.getPortCount())
        .then(Mono.just(this));
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
    // return rSocketClientTransport.fireAndForget(address, message);

    return rSocketClientTransport.requestResponse(address, message);
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
    return networkEmulator;
  }

  private Mono<NettyContextCloseable> bind(InetAddress listenAddress, LoopResources loopResources, int bindPort,
      int finalBindPort) {
    // Perform basic bind port validation
    if (bindPort < MIN_PORT_NUMBER || bindPort > MAX_PORT_NUMBER) {
      return Mono.error(new IllegalArgumentException("Invalid port number: " + bindPort));
    }
    if (bindPort > finalBindPort) {
      return Mono.error(
          new NoSuchElementException("Could not find an available port from: " + bindPort + " to: " + finalBindPort));
    }

    InetSocketAddress address = new InetSocketAddress(listenAddress, bindPort);

    HttpServer httpServer = HttpServer.create(options -> options
        .listenAddress(address)
        .loopResources(loopResources));

    WebsocketServerTransport transport = WebsocketServerTransport.create(httpServer);

    return RSocketFactory.receive()
        .frameDecoder(frame -> ByteBufPayload.create(frame.sliceData().retain(), frame.sliceMetadata().retain()))
        .acceptor(new RSocketAcceptor(subject))
        .transport(transport)
        .start()
        .doOnSuccess(server -> {
          if (this.server.compareAndSet(null, server)) {
            if (config.isUseNetworkEmulator()) {
              Address address0 = Address.create(server.address().getHostString(), server.address().getPort());
              networkEmulator = new NetworkEmulator(address0, config.isUseNetworkEmulator());
              NetworkEmulatorHandler emulatorHandler = new NetworkEmulatorHandler(networkEmulator);
              this.rSocketClientTransport = new RSocketClientTransport(loopResources, emulatorHandler);
            } else {
              this.rSocketClientTransport = new RSocketClientTransport(loopResources, null);
            }

            LOGGER.info("Bound to: {}", server.address());
          } else {
            server.dispose();
            throw new IllegalStateException("server is already running");
          }
        })
        .onErrorResume(cause -> {
          if (config.isPortAutoIncrement() && isAddressAlreadyInUseException(cause)) {
            LOGGER.warn("Can't bind to address {}, try again on different port [cause={}]", address, cause);
            return bind(listenAddress, loopResources, bindPort + 1, finalBindPort);
          }
          LOGGER.error("Failed to bind to: {}, cause: {}", address, cause);
          return Mono.error(cause);
        });
  }

  private boolean isAddressAlreadyInUseException(Throwable exception) {
    return exception instanceof BindException
        || (exception.getMessage() != null && exception.getMessage().contains("Address already in use"));
  }
}
