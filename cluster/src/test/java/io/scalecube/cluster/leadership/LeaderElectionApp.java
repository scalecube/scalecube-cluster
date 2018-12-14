package io.scalecube.cluster.leadership;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.leaderelection.api.ElectionTopic;

public class LeaderElectionApp {

  public static void main(String[] args) {

    Cluster cluster  = Cluster.joinAwait();
    Cluster cluster1 = Cluster.joinAwait(cluster.address());
    Cluster cluster2 = Cluster.joinAwait(cluster.address());

    ElectionTopic le1 = cluster.leadership("some-topic");
    ElectionTopic le2 = cluster1.leadership("some-topic");
    ElectionTopic le3 = cluster2.leadership("some-topic");

    le1.listen().subscribe(e -> {
      System.out.println("Alice " + le1.currentState() + " -> " + e.state());
      print(le1, le2, le3);
    });

    le2.listen().subscribe(e -> {
      System.out.println("Joe " + le1.currentState() + " -> " + e.state());
      print(le1, le2, le3);

    });
    le3.listen().subscribe(e -> {
      System.out.println("David " + le1.currentState() + " -> " + e.state());
      print(le1, le2, le3);
    });
    System.out.println("done " + cluster1.metadata());

  }

  public static void print(ElectionTopic... elections) {
    for (ElectionTopic election : elections) {
      System.out.println(election.id() + " - " + election.currentState());
    }
  }
}
