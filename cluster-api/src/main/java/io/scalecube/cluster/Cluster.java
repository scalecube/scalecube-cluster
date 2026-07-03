package io.scalecube.cluster;

import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import java.util.Collection;
import java.util.Optional;
import reactor.core.publisher.Mono;

/** Facade cluster interface which provides API to interact with cluster members. */
public interface Cluster {

  /**
   * Returns address of this cluster instance.
   *
   * @return cluster address
   */
  String address();

  /**
   * Returns the {@link Transport} this cluster instance is bound to — the same transport its
   * membership, failure-detector, gossip and metadata protocols use. Exposed so embedded layers
   * (libraries decorating the cluster) can reuse it for their own point-to-point messaging
   * ({@link Transport#send(String, Message) send} / {@link Transport#requestResponse(String,
   * Message) requestResponse} / {@link Transport#listen() listen}) instead of binding a second
   * transport and opening another port. Inbound messages are delivered to the registered {@link
   * ClusterMessageHandler#onMessage(Message)}.
   *
   * @return the cluster's transport
   */
  Transport transport();

  /**
   * Spreads given message between cluster members using gossiping protocol.
   *
   * @param message message to disseminate.
   * @return result future
   */
  Mono<String> spreadGossip(Message message);

  /**
   * Returns local cluster member metadata.
   *
   * @return local member metadata
   */
  <T> Optional<T> metadata();

  /**
   * Returns cluster member metadata by given member.
   *
   * @param member cluster member
   * @return cluster member metadata
   */
  <T> Optional<T> metadata(Member member);

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
  Optional<Member> memberById(String id);

  /**
   * Returns cluster member by given address or null if no member with such address exists at joined
   * cluster.
   *
   * @return member by address
   */
  Optional<Member> memberByAddress(String address);

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
   * Updates local member metadata with the given metadata map. Metadata is updated asynchronously
   * and results in a membership update event for local member once it is updated locally.
   * Information about new metadata is disseminated to other nodes of the cluster with a
   * weekly-consistent guarantees.
   *
   * @param metadata new metadata
   */
  <T> Mono<Void> updateMetadata(T metadata);

  /**
   * Member notifies other members of the cluster about leaving and gracefully shutdown and free
   * occupied resources.
   */
  void shutdown();

  /**
   * Returns promise which is completed when cluster instance has been shut down.
   *
   * @return promise which is completed once graceful shutdown is finished.
   */
  Mono<Void> onShutdown();
}
