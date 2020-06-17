package io.scalecube.transport.netty;

import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;

public interface Receiver {

  Mono<DisposableServer> bind();
}
