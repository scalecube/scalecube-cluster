package io.scalecube.cluster.netty.transport;

import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.cluster.transport.api.TransportFactory;

public class NettyTransportFactory implements TransportFactory {
  @Override
  public Transport create(TransportConfig transportConfig) {
    return new NettyTransport(transportConfig);
  }
}
