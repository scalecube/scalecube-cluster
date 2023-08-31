package io.scalecube.cluster;

import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.net.Address;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import reactor.core.publisher.Mono;

public class TransportWrapper {

  private final Transport transport;

  private final Map<Member, Address> connections = new ConcurrentHashMap<>();

  public TransportWrapper(Transport transport) {
    this.transport = transport;
  }

  public Mono<Message> requestResponse(Member member, Message request) {
    // return requestResponse(transport, addresses, 0, request);
    connections.computeIfAbsent(member, m -> requestResponse0(m, request));
  }

  private Address requestResponse0(Member member, Message request) {
    return null;
  }

  private Mono<Message> requestResponse(
      List<Address> addresses, int currentIndex, Message request) {
    if (currentIndex >= addresses.size()) {
      return Mono.error(new RuntimeException("All addresses have been tried and failed"));
    }

    return transport
        .requestResponse(addresses.get(currentIndex), request)
        .onErrorResume(th -> requestResponse(addresses, currentIndex + 1, request));
  }

  public static Mono<Void> send(Transport transport, List<Address> addresses, Message request) {
    return send(transport, addresses, 0, request);
  }

  private static Mono<Void> send(
      Transport transport, List<Address> addresses, int currentIndex, Message request) {
    if (currentIndex >= addresses.size()) {
      return Mono.error(new RuntimeException("All addresses have been tried and failed."));
    }

    return transport
        .send(addresses.get(currentIndex), request)
        .onErrorResume(th -> send(transport, addresses, currentIndex + 1, request));
  }
}
