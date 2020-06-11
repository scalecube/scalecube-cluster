package io.scalecube.cluster.transport.api;

import io.scalecube.utils.ServiceLoaderUtil;

public interface TransportFactory {

  TransportFactory INSTANCE = ServiceLoaderUtil.findFirst(TransportFactory.class).orElse(null);

  Transport createTransport(TransportConfig config);
}
