package io.scalecube.cluster.membership;

import io.scalecube.cluster.Member;
import io.scalecube.transport.Address;
import java.util.Collection;
import java.util.Optional;
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

  /**
   * Returns list of all members of the joined cluster. This will include all cluster members
   * including local member.
   *
   * @return all members in the cluster (including local one)
   */
  Collection<Member> members();

  /**
   * Returns list of all cluster members of the joined cluster excluding local member.
   *
   * @return all members in the cluster (excluding local one)
   */
  Collection<Member> otherMembers();

  /**
   * Returns local cluster member which corresponds to this cluster instance.
   *
   * @return local member
   */
  Member member();

  /**
   * Returns cluster member with given id or null if no member with such id exists at joined
   * cluster.
   *
   * @return member by id
   */
  Optional<Member> member(String id);

  /**
   * Returns cluster member by given address or null if no member with such address exists at joined
   * cluster.
   *
   * @return member by address
   */
  Optional<Member> member(Address address);
}
