package io.scalecube.cluster.election.api;

import reactor.core.publisher.Flux;

public interface ElectionService {

  String id();

  String name();

  State currentState();

  Flux<ElectionEvent> listen();
}
