package io.scalecube.transport.netty.websocket;

import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.cluster.transport.api.TransportFactory;
import io.scalecube.transport.netty.TransportImpl;

public final class WebsocketTransportFactory implements TransportFactory {

  @Override
  public Transport createTransport(TransportConfig config) {
    return new TransportImpl(config, new WebsocketReceiver(config), new WebsocketSender(config));
  }
}
