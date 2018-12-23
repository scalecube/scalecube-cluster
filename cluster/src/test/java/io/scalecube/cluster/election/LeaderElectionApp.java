package io.scalecube.cluster.election;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.election.api.ElectionService;

public class LeaderElectionApp {

  public static void main(String[] args) throws InterruptedException {

    
    Cluster cluster0  = Cluster.joinAwait();
    Cluster cluster1 = Cluster.joinAwait(cluster0.address());
    Cluster cluster2 = Cluster.joinAwait(cluster0.address());
    Cluster cluster3 = Cluster.joinAwait(cluster0.address());
//    Cluster cluster4 = Cluster.joinAwait(cluster0.address());
    
    ElectionService le1 = cluster0.election().create("topic-1");
    ElectionService le2 = cluster1.election().create("topic-1");
    ElectionService le3 = cluster2.election().create("topic-1");
    ElectionService le4 = cluster3.election().create("topic-1");
//    ElectionTopic le5 = cluster4.leadership("topic-1");

    le1.listen().subscribe(e -> {
      System.out.println("Alice  -> " + e.state());
      print(le1, le2, le3, le4);
    });

    le2.listen().subscribe(e -> {
      System.out.println("Joe  -> " + e.state());
      print(le1, le2, le3, le4);
    });

    le3.listen().subscribe(e -> {
      System.out.println("David  -> " + e.state());
      print(le1, le2, le3, le4);
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

  public static void print(ElectionService... elections) {
    for (ElectionService quorum : elections) {
      System.out.println(quorum.id() + " - " + quorum.currentState());
    }
  }
}
