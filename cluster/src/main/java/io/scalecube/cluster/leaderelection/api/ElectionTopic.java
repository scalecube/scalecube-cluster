package io.scalecube.cluster.leaderelection.api;

import io.scalecube.cluster.leaderelection.State;
import reactor.core.publisher.Flux;

public interface ElectionTopic {

  String id();

  String name();

  State currentState();

  Flux<ElectionEvent> listen();
}
