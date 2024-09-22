package io.scalecube.transport.netty;

import io.scalecube.cluster.transport.api.Message;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;

public interface Sender {

  Mono<Connection> connect(String address);

  Mono<Void> send(Message message);
}
