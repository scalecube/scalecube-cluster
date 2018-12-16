package io.scalecube.cluster.leadership;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.leaderelection.api.ElectionTopic;

public class LeaderElectionApp {

  public static void main(String[] args) throws InterruptedException {

    
    Cluster cluster0  = Cluster.joinAwait();
    Cluster cluster1 = Cluster.joinAwait(cluster0.address());
    Cluster cluster2 = Cluster.joinAwait(cluster0.address());
    Cluster cluster3 = Cluster.joinAwait(cluster0.address());
//    Cluster cluster4 = Cluster.joinAwait(cluster0.address());
    
    ElectionTopic le1 = cluster0.leadership("topic-1");
    ElectionTopic le2 = cluster1.leadership("topic-1");
    ElectionTopic le3 = cluster2.leadership("topic-1");
    ElectionTopic le4 = cluster3.leadership("topic-1");
//    ElectionTopic le5 = cluster4.leadership("topic-1");

    le1.listen().subscribe(e -> {
      System.out.println("Alice  -> " + e.state());
      print(le1, le2);
    });

    le2.listen().subscribe(e -> {
      System.out.println("Joe  -> " + e.state());
      print(le1, le2);
    });

    le3.listen().subscribe(e -> {
      System.out.println("David  -> " + e.state());
      print(le1, le2, le3);
    });

    le4.listen().subscribe(e -> {
      System.out.println("Ran  -> " + e.state());
      print(le1, le2, le3, le4);
    });
/*
    le5.listen().subscribe(e -> {
      System.out.println("David " + le1.currentState() + " -> " + e.state());
      print(le1, le2, le3, le4, le5);
    });*/
    Thread.currentThread().join();
  }

  public static void print(ElectionTopic... elections) {
    for (ElectionTopic election : elections) {
      System.out.println(election.id() + " - " + election.currentState());
    }
  }
}
