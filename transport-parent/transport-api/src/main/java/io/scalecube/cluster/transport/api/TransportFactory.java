package io.scalecube.cluster.transport.api;

public interface TransportFactory {

  Transport createTransport(TransportConfig config);
}
