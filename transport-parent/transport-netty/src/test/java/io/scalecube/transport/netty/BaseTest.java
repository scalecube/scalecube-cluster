package io.scalecube.transport.netty;

import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.cluster.utils.NetworkEmulatorTransport;
import io.scalecube.net.Address;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/** Base test class. */
public class BaseTest {

  protected static final Logger LOGGER = LoggerFactory.getLogger(BaseTest.class);

  protected static final int CONNECT_TIMEOUT = 10000;

  @BeforeEach
  public final void baseSetUp(TestInfo testInfo) {
    LOGGER.info("***** Test started  : " + testInfo.getDisplayName() + " *****");
  }

  @AfterEach
  public final void baseTearDown(TestInfo testInfo) {
    LOGGER.info("***** Test finished : " + testInfo.getDisplayName() + " *****");
  }

  /**
   * Sending message from src to destination.
   *
   * @param transport src
   * @param to destination
   * @param msg request
   */
  protected Mono<Void> send(Transport transport, Address to, Message msg) {
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

  /**
   * Stopping transport.
   *
   * @param transport trnasport object
   */
  protected void destroyTransport(Transport transport) {
    if (transport != null && !transport.isStopped()) {
      try {
        transport.stop().block(Duration.ofSeconds(1));
      } catch (Exception ex) {
        LOGGER.warn("Failed to await transport termination: " + ex);
      }
    }
  }

  /**
   * Factory method to create a transport.
   *
   * @return tramsprot
   */
  protected NetworkEmulatorTransport createTcpTransport() {
    return new NetworkEmulatorTransport(
        TransportImpl.bindAwait(
            TransportConfig.defaultConfig()
                .connectTimeout(CONNECT_TIMEOUT)
                .transportFactory(new TcpTransportFactory())));
  }

  /**
   * Factory method to create a transport.
   *
   * @return tramsprot
   */
  protected NetworkEmulatorTransport createWebsocketTransport() {
    return new NetworkEmulatorTransport(
        TransportImpl.bindAwait(
            TransportConfig.defaultConfig()
                .connectTimeout(CONNECT_TIMEOUT)
                .transportFactory(new WebsocketTransportFactory())));
  }
}
