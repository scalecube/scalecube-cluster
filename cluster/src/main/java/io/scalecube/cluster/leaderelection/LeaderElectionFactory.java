package io.scalecube.cluster.leaderelection;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.leaderelection.api.ElectionTopic;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LeaderElectionFactory {

  private final ConcurrentMap<String, RaftLeaderElection> topic = new ConcurrentHashMap<>();

  private final Cluster cluster;

  public LeaderElectionFactory(Cluster cluster) {
    this.cluster = cluster;
  }

  /**
   * Create a leadership topic for a given name.
   *
   * @param name of the topic for this leader election.
   * @return election topic.
   */
  public ElectionTopic leadership(String name) {
    return topic.compute(
        name,
        (key, value) -> {
          RaftLeaderElection le = RaftLeaderElection.builder(this.cluster, key).build();
          le.start().subscribe();
          return le;
        });
  }
}
