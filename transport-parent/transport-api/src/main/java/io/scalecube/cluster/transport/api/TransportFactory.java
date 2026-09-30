package io.scalecube.cluster.transport.api;

import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

public interface TransportFactory {

  // First ServiceLoader provider (transport-netty registers websocket), null when none
  TransportFactory INSTANCE =
      StreamSupport.stream(ServiceLoader.load(TransportFactory.class).spliterator(), false)
          .findFirst()
          .orElse(null);

  Transport createTransport(TransportConfig config);
}
