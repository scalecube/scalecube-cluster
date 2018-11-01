package io.scalecube.cluster.membership;

import io.scalecube.cluster.Member;
import java.util.Map;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Cluster Membership Protocol component responsible for managing information about existing members
 * of the cluster.
 */
public interface MembershipProtocol {

  /** Returns local cluster member. */
  Member member();

  /** Updates local member metadata. */
  Mono<Void> updateMetadata(Map<String, String> metadata);

  /** Listen changes in cluster membership. */
  Flux<MembershipEvent> listen();
}
