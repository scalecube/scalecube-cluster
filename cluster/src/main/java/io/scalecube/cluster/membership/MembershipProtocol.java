package io.scalecube.cluster.membership;

import io.scalecube.cluster.Member;
import java.util.Map;
import reactor.core.publisher.Flux;

/**
 * Cluster Membership Protocol component responsible for managing information about existing members
 * of the cluster.
 */
public interface MembershipProtocol {

  /** Returns local cluster member. */
  Member member();

  /** Updates local member metadata. */
  void updateMetadata(Map<String, String> metadata);

  /** Updates local member metadata to set given key and value. */
  void updateMetadataProperty(String key, String value);

  /** Listen changes in cluster membership. */
  Flux<MembershipEvent> listen();
}
