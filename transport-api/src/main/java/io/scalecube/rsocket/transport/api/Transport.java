package io.scalecube.rsocket.transport.api;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface Transport {

  /**
   * Returns local {@link Address} on which current instance of transport listens for incoming messages.
   *
   * @return address
   */
  Address address();

  /**
   * Sends message to the given address. It will issue connect in case if no transport channel by given transport
   * {@code address} exists already. Send is an async operation.
   *
   * @param address address where message will be sent
   * @param message message to send
   * @throws IllegalArgumentException if {@code message} or {@code address} is null
   */
  Mono<Void> send(Address address, Message message);

  /**
   * Returns stream of received messages. For each observers subscribed to the returned observable:
   * <ul>
   * <li>{@code rx.Observer#onNext(Object)} will be invoked when some message arrived to current transport</li>
   * <li>{@code rx.Observer#onCompleted()} will be invoked when there is no possibility that server will receive new
   * message observable for already closed transport</li>
   * <li>{@code rx.Observer#onError(Throwable)} will not be invoked</li>
   * </ul>
   *
   * @return Observable which emit received messages or complete event when transport is closed
   */
  Flux<Message> listen();

  /**
   * Stop transport, disconnect all connections and release all resources which belong to this transport. After
   * transport is stopped it can't be opened again. Observable returned from method {@link #listen()} will immediately
   * emit onComplete event for all subscribers. Stop is async operation.
   */
  Mono<Void> stop();

  /**
   * @return true if transport was stopped; false otherwise.
   */
  boolean isStopped();

  /**
   * Returns network emulator associated with this instance of transport. It always returns non null instance even if
   * network emulator is disabled by transport config. In case when network emulator is disable all calls to network
   * emulator instance will result in no operation.
   *
   * @return network emulator
   */
  NetworkEmulator networkEmulator();
}
