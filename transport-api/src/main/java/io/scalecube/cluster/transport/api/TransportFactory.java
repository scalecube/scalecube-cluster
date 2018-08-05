package io.scalecube.cluster.transport.api;

public interface TransportFactory {

  Transport create(TransportConfig transportConfig);
}
