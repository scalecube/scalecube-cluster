package io.scalecube.cluster;

import reactor.core.publisher.Mono;

public interface LeaderElection {

	Mono<Void> start();

}
