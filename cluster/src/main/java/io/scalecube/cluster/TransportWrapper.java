package io.scalecube.cluster;

import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.net.Address;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import reactor.core.publisher.Mono;

public class TransportWrapper {

  private final Transport transport;

  private final Map<Member, Mono<Result>> connections = new ConcurrentHashMap<>();

  public TransportWrapper(Transport transport) {
    this.transport = transport;
  }

  public Mono<Message> requestResponse(Member member, Message request) {
    return connections
        .compute(
            member,
            (m, resultMono) -> {
              if (resultMono == null) {
                return requestResponse(member.addresses(), request);
              }
              return resultMono.flatMap(
                  result ->
                      transport
                          .requestResponse(result.address, request)
                          .map(message -> new Result(result.address, message)));
            })
        .map(result -> result.message);
  }

  private Mono<Result> requestResponse(List<Address> addresses, Message request) {
    final AtomicInteger currentIndex = new AtomicInteger();
    return Mono.defer(
            () -> {
              final int index = currentIndex.getAndIncrement();
              return transport.requestResponse(addresses.get(index), request);
            })
        .retry(addresses.size() - 1)
        .map(
            message -> {
              final int index = currentIndex.get();
              return new Result(addresses.get(index), message);
            });
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

  private static class Result {

    private final Address address;
    private final Message message;

    private Result(Address address) {
      this(address, null);
    }

    private Result(Address address, Message message) {
      this.address = address;
      this.message = message;
    }
  }
}
