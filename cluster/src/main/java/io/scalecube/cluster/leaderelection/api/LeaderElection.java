package io.scalecube.cluster.leaderelection.api;

import io.scalecube.cluster.leaderelection.State;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface LeaderElection {

	Mono<Void> start();

	Flux<ElectionEvent> listen();

	State currentState();

	String id();

}
