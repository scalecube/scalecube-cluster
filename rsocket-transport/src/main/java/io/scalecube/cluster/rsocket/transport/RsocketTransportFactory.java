package io.scalecube.cluster.rsocket.transport;

import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.cluster.transport.api.TransportFactory;

public class RsocketTransportFactory implements TransportFactory {
  @Override
  public Transport create(TransportConfig transportConfig) {
    return new RSocketTransport(transportConfig);
  }
}
