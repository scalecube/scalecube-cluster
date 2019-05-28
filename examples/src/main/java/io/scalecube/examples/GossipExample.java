package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.transport.Message;

/**
 * Basic example for member gossiping between cluster members. to run the example Start ClusterNodeA
 * and cluster ClusterNodeB A listen on gossip B spread gossip
 *
 * @author ronen hamias, Anton Kharenko
 */
public class GossipExample {

  /** Main method. */
  public static void main(String[] args) throws Exception {
    // Start cluster nodes and subscribe on listening gossips
    Cluster alice =
        new Cluster()
            .handler(
                cluster -> {
                  return new ClusterMessageHandler() {
                    @Override
                    public void onGossip(Message gossip) {
                      System.out.println("Alice heard: " + gossip.data());
                    }
                  };
                })
            .startAwait();

    Cluster bob =
        new Cluster()
            .seedMembers(alice.address())
            .handler(
                cluster -> {
                  return new ClusterMessageHandler() {
                    @Override
                    public void onGossip(Message gossip) {
                      System.out.println("Bob heard: " + gossip.data());
                    }
                  };
                })
            .startAwait();

    Cluster carol =
        new Cluster()
            .seedMembers(alice.address())
            .handler(
                cluster -> {
                  return new ClusterMessageHandler() {
                    @Override
                    public void onGossip(Message gossip) {
                      System.out.println("Carol heard: " + gossip.data());
                    }
                  };
                })
            .startAwait();

    Cluster dan =
        new Cluster()
            .seedMembers(alice.address())
            .handler(
                cluster -> {
                  return new ClusterMessageHandler() {
                    @Override
                    public void onGossip(Message gossip) {
                      System.out.println("Dan heard: " + gossip.data());
                    }
                  };
                })
            .startAwait();

    // Start cluster node Eve that joins cluster and spreads gossip
    Cluster eve = new Cluster().seedMembers(alice.address());
    eve.spreadGossip(Message.fromData("Gossip from Eve"))
        .doOnError(System.err::println)
        .subscribe();

    // Avoid exit main thread immediately ]:->
    Thread.sleep(1000);
  }
}
