package io.scalecube.cluster.leadership;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.leaderelection.api.ElectionTopic;

public class LeaderElectionApp {

  public static void main(String[] args) throws InterruptedException {

    
    Cluster cluster0  = Cluster.joinAwait();
    Cluster cluster1 = Cluster.joinAwait(cluster0.address());
    Cluster cluster2 = Cluster.joinAwait(cluster0.address());

    ElectionTopic le1 = cluster0.leadership("topic-1");
    ElectionTopic le2 = cluster1.leadership("topic-1");
    ElectionTopic le3 = cluster2.leadership("topic-1");

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
    
    Thread.currentThread().join();
  }

  public static void print(ElectionTopic... elections) {
    for (ElectionTopic election : elections) {
      System.out.println(election.id() + " - " + election.currentState());
    }
  }
}
