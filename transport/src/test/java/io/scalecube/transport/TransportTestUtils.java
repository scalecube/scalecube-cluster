package io.scalecube.transport;

import io.scalecube.rsocket.transport.api.Address;
import io.scalecube.rsocket.transport.api.Message;
import io.scalecube.rsocket.transport.api.Transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public final class TransportTestUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransportTestUtils.class);

  public static int CONNECT_TIMEOUT = 100;
  public static int DEFAULT_PORT = 5800;

  private TransportTestUtils() {
    // Do not instantiate
  }

  public static Transport createTransport() {
    TransportConfig config = TransportConfig.builder()
        .connectTimeout(CONNECT_TIMEOUT)
        .useNetworkEmulator(true)
        .port(DEFAULT_PORT)
        .build();
    return new TransportImpl(config).bind0().block();
  }

  public static void destroyTransport(Transport transport) {
    if (transport != null && !transport.isStopped()) {
      transport.stop()
          .doOnError(ignore -> LOGGER.warn("Failed to await transport termination"))
          .block(Duration.ofSeconds(1));
    }
  }

  public static void send(final Transport from, final Address to, final Message msg) {
    final CompletableFuture<Void> promise = new CompletableFuture<>();
    promise.thenAccept(aVoid -> {
      if (promise.isDone()) {
        try {
          promise.get();
        } catch (Exception e) {
          LOGGER.error("Failed to send {} to {} from transport: {}, cause: {}", msg, to, from, e.getCause());
        }
      }
    });
    from.send(to, msg)
        .doOnError(
            e -> LOGGER.error("Failed to send {} to {} from transport: {}, cause: {}", msg, to, from, e.getCause()))
        .subscribe();
  }
}
