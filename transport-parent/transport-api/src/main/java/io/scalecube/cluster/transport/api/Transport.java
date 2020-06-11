package io.scalecube.cluster.transport.api;

import io.scalecube.net.Address;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Transport is responsible for maintaining existing p2p connections to/from other transports. It
 * allows to send messages to other transports and listen for incoming messages.
 */
public interface Transport {

  /**
   * Returns local {@link Address} on which current instance of transport listens for incoming
   * messages.
   *
   * @return address
   */
  Address address();

  /**
   * Start transport. After this call method {@link #address()} shall be eligible for calling.
   *
   * @return started {@code Transport}
   */
  Mono<Transport> start();

  /**
   * Stop transport, disconnect all connections and release all resources which belong to this
   * transport. After transport is stopped it can't be used again. Observable returned from method
   * {@link #listen()} will immediately emit onComplete event for all subscribers.
   */
  Mono<Void> stop();

  /**
   * Return transport's stopped state.
   *
   * @return true if transport was stopped; false otherwise
   */
  boolean isStopped();

  /**
   * Sends message to the given address. It will issue connect in case if no transport channel by
   * given transport {@code address} exists already. Send is an async operation.
   *
   * @param address address where message will be sent
   * @param message message to send
   * @return promise which will be completed with result of sending (void or exception)
   * @throws IllegalArgumentException if {@code message} or {@code address} is null
   */
  Mono<Void> send(Address address, Message message);

  /**
   * Sends message to the given address. It will issue connect in case if no transport channel by
   * given transport {@code address} exists already. Send is an async operation and expecting a
   * response by a provided correlationId and sender address of the caller.
   *
   * @param address address where message will be sent
   * @param request to send message must contain correlctionId and sender to handle reply.
   * @return promise which will be completed with result of sending (message or exception)
   * @throws IllegalArgumentException if {@code message} or {@code address} is null
   */
  Mono<Message> requestResponse(Address address, Message request);

  /**
   * Returns stream of received messages. For each observers subscribed to the returned observable:
   *
   * <ul>
   *   <li>{@code rx.Observer#onNext(Object)} will be invoked when some message arrived to current
   *       transport
   *   <li>{@code rx.Observer#onCompleted()} will be invoked when there is no possibility that
   *       server will receive new message observable for already closed transport
   *   <li>{@code rx.Observer#onError(Throwable)} will not be invoked
   * </ul>
   *
   * @return Observable which emit received messages or complete event when transport is closed
   */
  Flux<Message> listen();
}
