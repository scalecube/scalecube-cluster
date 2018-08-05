package io.scalecube.cluster.transport.api;

@FunctionalInterface
public interface TransportFactory {

  static Transport defaultTransport(TransportConfig transportConfig) {
    return ServiceLoaderUtil.findFirst(TransportFactory.class)
        .map(factory -> factory.create(transportConfig))
        .orElseThrow(() -> new IllegalStateException("TransportFactory implementation not found"));
  }

  Transport create(TransportConfig transportConfig);
}
