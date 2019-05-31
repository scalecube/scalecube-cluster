package io.scalecube.transport;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class SenderAwareTransport implements Transport {

  private final Transport transport;
  private final Address sender;

  public SenderAwareTransport(Transport transport) {
    this(transport, transport.address());
  }

  public SenderAwareTransport(Transport transport, Address sender) {
    this.transport = transport;
    this.sender = sender;
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
    return transport.send(address, Message.with(message).sender(sender).build());
  }

  @Override
  public Mono<Message> requestResponse(Message request, Address address) {
    return transport.requestResponse(Message.with(request).sender(sender).build(), address);
  }

  @Override
  public Flux<Message> listen() {
    return transport.listen();
  }

  @Override
  public NetworkEmulator networkEmulator() {
    return transport.networkEmulator();
  }
}
