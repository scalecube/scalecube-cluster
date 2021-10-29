package io.scalecube.transport.netty.tcp;

import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.cluster.transport.api.TransportFactory;
import io.scalecube.transport.netty.TransportImpl;

public final class TcpTransportFactory implements TransportFactory {

  @Override
  public Transport createTransport(TransportConfig config) {
    return new TransportImpl(
        config.messageCodec(),
        new TcpReceiver(config),
        new TcpSender(config),
        config.addressMapper());
  }
}
