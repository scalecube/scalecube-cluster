package io.scalecube.cluster.transport.api;

import io.scalecube.net.Address;
import java.util.List;
import reactor.core.publisher.Mono;

public class TransportWrapper {

  public static Mono<Message> requestResponse(
      Transport transport, List<Address> addresses, Message request) {
    return requestResponse(transport, addresses, 0, request);
  }

  private static Mono<Message> requestResponse(
      Transport transport, List<Address> addresses, int currentIndex, Message request) {
    if (currentIndex >= addresses.size()) {
      return Mono.error(new RuntimeException("All addresses have been tried and failed"));
    }

    return transport
        .requestResponse(addresses.get(currentIndex), request)
        .onErrorResume(th -> requestResponse(transport, addresses, currentIndex + 1, request));
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
