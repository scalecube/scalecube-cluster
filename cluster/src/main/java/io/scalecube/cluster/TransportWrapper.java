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

  private final Map<Member, AtomicInteger> addressIndexByMember = new ConcurrentHashMap<>();

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
    final List<Address> addresses = member.addresses();
    final AtomicInteger currentIndex =
        addressIndexByMember.computeIfAbsent(member, m -> new AtomicInteger());
    return Mono.defer(
            () -> {
              synchronized (this) {
                if (currentIndex.get() == addresses.size()) {
                  currentIndex.set(0);
                }
                final Address address = addresses.get(currentIndex.getAndIncrement());
                return transport.requestResponse(address, request);
              }
            })
        .retry(addresses.size() - 1)
        .doOnError(throwable -> addressIndexByMember.remove(member, currentIndex));
  }

  /**
   * Execute send call.
   *
   * @param member member
   * @param request request
   * @return mono result
   */
  public Mono<Void> send(Member member, Message request) {
    final List<Address> addresses = member.addresses();
    final AtomicInteger currentIndex =
        addressIndexByMember.computeIfAbsent(member, m -> new AtomicInteger());
    return Mono.defer(
            () -> {
              synchronized (this) {
                if (currentIndex.get() == addresses.size()) {
                  currentIndex.set(0);
                }
                final Address address = addresses.get(currentIndex.getAndIncrement());
                return transport.send(address, request);
              }
            })
        .retry(addresses.size() - 1)
        .doOnError(throwable -> addressIndexByMember.remove(member, currentIndex));
  }
}
