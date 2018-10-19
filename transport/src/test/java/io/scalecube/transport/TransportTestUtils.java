package io.scalecube.transport;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Transport test utility class. */
public final class TransportTestUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransportTestUtils.class);

  public static int CONNECT_TIMEOUT = 100;

  private TransportTestUtils() {
    // Do not instantiate
  }

  /**
   * Factory method to create a transport.
   *
   * @return tramsprot
   */
  public static Transport createTransport() {
    TransportConfig config =
        TransportConfig.builder().connectTimeout(CONNECT_TIMEOUT).useNetworkEmulator(true).build();
    return Transport.bindAwait(config);
  }

  /**
   * Stopping transport.
   *
   * @param transport trnasport object
   */
  public static void destroyTransport(Transport transport) {
    if (transport != null && !transport.isStopped()) {
      CompletableFuture<Void> close = new CompletableFuture<>();
      transport.stop(close);
      try {
        close.get(1, TimeUnit.SECONDS);
      } catch (Exception ignore) {
        LOGGER.warn("Failed to await transport termination");
      }
    }
  }

  /**
   * Sending message from src to destination.
   *
   * @param from src
   * @param to destination
   * @param msg request
   */
  public static void send(final Transport from, final Address to, final Message msg) {
    final CompletableFuture<Void> promise = new CompletableFuture<>();
    promise.thenAccept(
        avoid -> {
          if (promise.isDone()) {
            try {
              promise.get();
            } catch (Exception e) {
              LOGGER.error(
                  "Failed to send {} to {} from transport: {}, cause: {}",
                  msg,
                  to,
                  from,
                  e.getCause());
            }
          }
        });
    from.send(to, msg, promise);
  }
}
