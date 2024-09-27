package io.scalecube.cluster.transport.api;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Transport is responsible for maintaining existing p2p connections to/from other transports. It
 * allows to send messages to other transports and listen for incoming messages.
 */
public interface Transport {

  Pattern ADDRESS_FORMAT = Pattern.compile("(?<host>^.*):(?<port>\\d+$)");

  /**
   * Returns local address on which current instance of transport listens for incoming messages.
   *
   * @return address
   */
  String address();

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
  Mono<Void> send(String address, Message message);

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
  Mono<Message> requestResponse(String address, Message request);

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

  /**
   * Init transport with the default configuration synchronously. Starts to accept connections on
   * local address.
   *
   * @return transport
   */
  static Transport bindAwait() {
    return bindAwait(TransportConfig.defaultConfig());
  }

  /**
   * Init transport with the given configuration synchronously. Starts to accept connections on
   * local address.
   *
   * @return transport
   */
  static Transport bindAwait(TransportConfig config) {
    try {
      return bind(config).block();
    } catch (Exception e) {
      throw Exceptions.propagate(e.getCause() != null ? e.getCause() : e);
    }
  }

  /**
   * Init transport with the default configuration asynchronously. Starts to accept connections on
   * local address.
   *
   * @return promise for bind operation
   */
  static Mono<Transport> bind() {
    return bind(TransportConfig.defaultConfig());
  }

  /**
   * Init transport with the given configuration asynchronously. Starts to accept connections on
   * local address.
   *
   * @param config transport config
   * @return promise for bind operation
   */
  static Mono<Transport> bind(TransportConfig config) {
    Objects.requireNonNull(config.transportFactory(), "[bind] transportFactory");
    return config.transportFactory().createTransport(config).start();
  }

  /**
   * Parses string in format {@code host:port} and returns host part.
   *
   * @param address address, must be string in format {@code host:port}
   * @return address host, or throwing exception
   */
  static String parseHost(String address) {
    if (address == null || address.isEmpty()) {
      throw new IllegalArgumentException("Cannot parse address host from: " + address);
    }

    Matcher matcher = ADDRESS_FORMAT.matcher(address);
    if (!matcher.find()) {
      throw new IllegalArgumentException("Cannot parse address host from: " + address);
    }

    String host = matcher.group(1);
    if (host == null || host.isEmpty()) {
      throw new IllegalArgumentException("Cannot parse address host from: " + address);
    }

    return host;
  }

  /**
   * Parses string in format {@code host:port} and returns port part.
   *
   * @param address address, must be string in format {@code host:port}
   * @return address port, or throwing exception
   */
  static int parsePort(String address) {
    if (address == null || address.isEmpty()) {
      throw new IllegalArgumentException("Cannot parse address port from: " + address);
    }

    Matcher matcher = ADDRESS_FORMAT.matcher(address);
    if (!matcher.find()) {
      throw new IllegalArgumentException("Cannot parse address port from: " + address);
    }

    int port;
    try {
      port = Integer.parseInt(matcher.group(2));
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException("Cannot parse address port from: " + address, ex);
    }

    return port;
  }
}
