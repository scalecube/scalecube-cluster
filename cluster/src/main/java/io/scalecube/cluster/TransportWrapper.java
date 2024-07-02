package io.scalecube.cluster;

import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.net.Address;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
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
    return invokeWithRetry(member, request, transport::requestResponse);
  }

  /**
   * Execute send call.
   *
   * @param member member
   * @param request request
   * @return mono result
   */
  public Mono<Void> send(Member member, Message request) {
    return invokeWithRetry(member, request, transport::send);
  }

  private <T> Mono<T> invokeWithRetry(
      Member member, Message request, BiFunction<Address, Message, Mono<T>> function) {
    return Mono.defer(
        () -> {
          final List<Address> addresses = member.addresses();
          final Integer index = addressIndexByMember.computeIfAbsent(member, m -> 0);
          final AtomicInteger currentIndex = new AtomicInteger(index);

          return Mono.defer(
                  () -> {
                    if (currentIndex.get() == addresses.size()) {
                      currentIndex.set(0);
                    }
                    final Address address = addresses.get(currentIndex.get());
                    return function.apply(address, request);
                  })
              .doOnSuccess(s -> addressIndexByMember.put(member, currentIndex.get()))
              .doOnError(ex -> currentIndex.incrementAndGet())
              .retry(addresses.size() - 1);
        });
  }
}
