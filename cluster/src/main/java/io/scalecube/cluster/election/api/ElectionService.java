package io.scalecube.cluster.election.api;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ElectionService {

  String id();

  String name();

  State currentState();

  Flux<ElectionEvent> listen();
  
  Mono<Void> leave();
}
