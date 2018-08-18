package io.scalecube.cluster.transport.netty;

import io.scalecube.cluster.transport.Transport;
import io.scalecube.cluster.transport.TransportConfig;
import io.scalecube.cluster.transport.TransportFactory;

public class NettyTransportFactory implements TransportFactory {
  @Override
  public Transport create(TransportConfig transportConfig) {
    return new NettyTransport(transportConfig);
  }
}
