package io.scalecube.transport.netty.http;

import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.cluster.transport.api.TransportFactory;
import io.scalecube.transport.netty.TransportImpl;

public final class HttpTransportFactory implements TransportFactory {

  @Override
  public Transport createTransport(TransportConfig config) {
    return new HttpTransport(config);
  }
}
