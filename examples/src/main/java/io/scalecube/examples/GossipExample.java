package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;

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
        new ClusterImpl()
            .transportFactory(TcpTransportFactory::new)
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

    //noinspection unused
    Cluster bob =
        new ClusterImpl()
            .membership(opts -> opts.seedMembers(alice.address()))
            .transportFactory(TcpTransportFactory::new)
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

    //noinspection unused
    Cluster carol =
        new ClusterImpl()
            .membership(opts -> opts.seedMembers(alice.address()))
            .transportFactory(TcpTransportFactory::new)
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

    //noinspection unused
    Cluster dan =
        new ClusterImpl()
            .membership(opts -> opts.seedMembers(alice.address()))
            .transportFactory(TcpTransportFactory::new)
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
    Cluster eve =
        new ClusterImpl()
            .membership(opts -> opts.seedMembers(alice.address()))
            .transportFactory(TcpTransportFactory::new)
            .startAwait();
    eve.spreadGossip(Message.fromData("Gossip from Eve"))
        .doOnError(System.err::println)
        .subscribe(null, Throwable::printStackTrace);

    // Avoid exit main thread immediately ]:->
    Thread.sleep(1000);
  }
}
