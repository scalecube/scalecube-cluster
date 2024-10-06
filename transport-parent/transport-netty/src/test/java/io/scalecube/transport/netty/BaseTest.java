package io.scalecube.transport.netty;

import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.cluster.utils.NetworkEmulatorTransport;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import reactor.core.publisher.Mono;

/** Base test class. */
public class BaseTest {

  protected static final Logger LOGGER = System.getLogger(BaseTest.class.getName());

  @BeforeEach
  public final void baseSetUp(TestInfo testInfo) {
    LOGGER.log(Level.INFO, "***** Test started  : " + testInfo.getDisplayName() + " *****");
  }

  @AfterEach
  public final void baseTearDown(TestInfo testInfo) {
    LOGGER.log(Level.INFO, "***** Test finished : " + testInfo.getDisplayName() + " *****");
  }

  /**
   * Sending message from src to destination.
   *
   * @param transport src
   * @param to destination
   * @param msg request
   */
  protected Mono<Void> send(Transport transport, String to, Message msg) {
    return transport
        .send(to, msg)
        .doOnError(
            th ->
                LOGGER.log(
                    Level.ERROR,
                    "Failed to send {0} to {1} from transport: {2}, cause: {3}",
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
        LOGGER.log(Level.WARNING, "Failed to await transport termination: {0}", ex.toString());
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
        Transport.bindAwait(
            TransportConfig.defaultConfig().transportFactory(new TcpTransportFactory())));
  }

  /**
   * Factory method to create a transport.
   *
   * @return tramsprot
   */
  protected NetworkEmulatorTransport createWebsocketTransport() {
    return new NetworkEmulatorTransport(
        Transport.bindAwait(
            TransportConfig.defaultConfig().transportFactory(new WebsocketTransportFactory())));
  }
}
