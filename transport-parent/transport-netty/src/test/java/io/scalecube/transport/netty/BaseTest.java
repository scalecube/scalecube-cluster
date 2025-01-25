package io.scalecube.transport.netty;

import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.cluster.utils.NetworkEmulatorTransport;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class BaseTest {

  public static final Logger LOGGER = LoggerFactory.getLogger(BaseTest.class);

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
        LOGGER.warn("Failed to await transport termination: {}", ex.toString());
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
