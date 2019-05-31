package io.scalecube.transport;

import static io.scalecube.transport.TransportConfig.DEFAULT_MAX_FRAME_LENGTH;

import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

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
        TransportConfig.builder()
            .connectTimeout(CONNECT_TIMEOUT)
            .useNetworkEmulator(true)
            .maxFrameLength(DEFAULT_MAX_FRAME_LENGTH)
            .build();

    Transport transport = Transport.bindAwait(config);

    return new SenderAwareTransport(transport);
  }

  /**
   * Stopping transport.
   *
   * @param transport trnasport object
   */
  public static void destroyTransport(Transport transport) {
    if (transport != null && !transport.isStopped()) {
      try {
        transport.stop().block(Duration.ofSeconds(1));
      } catch (Exception ignore) {
        LOGGER.warn("Failed to await transport termination");
      }
    }
  }

  /**
   * Sending message from src to destination.
   *
   * @param transport src
   * @param to destination
   * @param msg request
   */
  public static Mono<Void> send(Transport transport, Address to, Message msg) {
    return transport
        .send(to, msg)
        .doOnError(
            th ->
                LOGGER.error(
                    "Failed to send {} to {} from transport: {}, cause: {}",
                    msg,
                    to,
                    transport,
                    th.toString()));
  }
}
