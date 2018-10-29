package io.scalecube.cluster.gossip;

import io.scalecube.transport.Message;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Gossip Protocol component responsible for spreading information (gossips) over the cluster
 * members using infection-style dissemination algorithms. It provides reliable cross-cluster
 * broadcast.
 */
public interface GossipProtocol {

  /** Starts running gossip protocol. After started it begins to receive and send gossip messages */
  void start();

  /** Stops running gossip protocol and releases occupied resources. */
  void stop();

  /**
   * Spreads given message between cluster members.
   *
   * @return future result with gossip id once gossip fully spread.
   */
  Mono<String> spread(Message message);

  /** Listens for gossips from other cluster members. */
  Flux<Message> listen();
}
