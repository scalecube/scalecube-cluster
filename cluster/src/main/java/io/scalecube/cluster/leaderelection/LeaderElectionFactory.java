package io.scalecube.cluster.leaderelection;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.leaderelection.api.ElectionTopic;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import reactor.core.publisher.Mono;

public class LeaderElectionFactory {

  private final ConcurrentMap<String, RaftLeaderElection> topic = new ConcurrentHashMap<>();

  private final Cluster cluster;

  public LeaderElectionFactory(Cluster cluster) {
    this.cluster = cluster;
  }

  public ElectionTopic leadership(String name) {
    return topic.compute(name, (key, value) -> {
      RaftLeaderElection le = new RaftLeaderElection(this.cluster, key);
      le.start().subscribe();
      return le;
    });
  }
}
