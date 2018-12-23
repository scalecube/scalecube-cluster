package io.scalecube.cluster.election;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.election.api.ElectionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ElectionFactory {

  private final ConcurrentMap<String, ElectionService> topic = new ConcurrentHashMap<>();

  private final Cluster cluster;

  public ElectionFactory(Cluster cluster) {
    this.cluster = cluster;
  }

  /**
   * Create a leadership topic for a given name.
   *
   * @param quorumName of the topic for this leader election.
   * @return election topic.
   */
  public ElectionService create(String quorumName) {
    return topic.computeIfAbsent(
        quorumName,
        (key) -> {
          ElectionProtocol le = ElectionProtocol.builder(this.cluster, key).build();
          le.start().subscribe();
          return le;
        });
  }
}
