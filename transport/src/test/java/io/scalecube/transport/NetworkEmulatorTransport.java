package io.scalecube.transport;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class NetworkEmulatorTransport implements Transport {

  private final Transport transport;
  private final NetworkEmulator networkEmulator;

  public NetworkEmulatorTransport(Transport transport) {
    this.transport = transport;
    Address address = transport.address();
    this.networkEmulator = new NetworkEmulator(address);
  }

  public NetworkEmulator networkEmulator() {
    return networkEmulator;
  }

  @Override
  public Address address() {
    return transport.address();
  }

  @Override
  public Mono<Void> stop() {
    return transport.stop();
  }

  @Override
  public boolean isStopped() {
    return transport.isStopped();
  }

  @Override
  public Mono<Void> send(Address address, Message message) {
    return Mono.just(message)
        .flatMap(msg -> networkEmulator.tryFailOutbound(msg, address))
        .flatMap(msg -> networkEmulator.tryDelayOutbound(msg, address))
        .flatMap(msg -> transport.send(address, msg));
  }

  @Override
  public Mono<Message> requestResponse(Address address, Message request) {
    return Mono.just(request)
        .flatMap(msg -> networkEmulator.tryFailOutbound(msg, address))
        .flatMap(msg -> networkEmulator.tryDelayOutbound(msg, address))
        .flatMap(msg -> transport.requestResponse(address, msg));
  }

  @Override
  public Flux<Message> listen() {
    return transport
        .listen()
        .filter(message -> networkEmulator.inboundSettings(message.sender()).shallPass())
        .onBackpressureBuffer();
  }
}
