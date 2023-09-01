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

  private final Map<Member, Integer> addressIndexByMember = new ConcurrentHashMap<>();

  public TransportWrapper(Transport transport) {
    this.transport = transport;
  }

  /**
   * Execute request response call.
   *
   * @param member member
   * @param request request
   * @return mono result
   */
  public Mono<Message> requestResponse(Member member, Message request) {
    return Mono.defer(
        () -> {
          final List<Address> addresses = member.addresses();
          final int numRetries = addresses.size() - 1;
          final Integer index = addressIndexByMember.getOrDefault(member, 0);
          final AtomicInteger currentIndex = new AtomicInteger(index);

          return Mono.defer(
                  () -> {
                    int increment = currentIndex.getAndIncrement();

                    if (increment == addresses.size()) {
                      currentIndex.set(increment = 0);
                    }

                    final Address address = addresses.get(increment);
                    return transport.requestResponse(address, request);
                  })
              .retry(numRetries);
        });
  }

  /**
   * Execute send call.
   *
   * @param member member
   * @param request request
   * @return mono result
   */
  public Mono<Void> send(Member member, Message request) {
    return Mono.defer(
        () -> {
          final List<Address> addresses = member.addresses();
          final AtomicInteger currentIndex = new AtomicInteger();
          return Mono.defer(
                  () -> {
                    final int index = currentIndex.getAndIncrement();
                    return transport.send(addresses.get(index), request);
                  })
              .retry(addresses.size() - 1);
        });
  }
}
