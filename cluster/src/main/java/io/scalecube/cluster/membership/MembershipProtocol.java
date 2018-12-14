package io.scalecube.cluster.membership;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Cluster Membership Protocol component responsible for managing information about existing members
 * of the cluster.
 */
public interface MembershipProtocol {

  /**
   * Starts running cluster membership protocol. After started it begins to receive and send cluster
   * membership messages
   */
  Mono<Void> start();

  /** Stops running cluster membership protocol and releases occupied resources. */
  void stop();

  /** Listen changes in cluster membership. */
  Flux<MembershipEvent> listen();
}
