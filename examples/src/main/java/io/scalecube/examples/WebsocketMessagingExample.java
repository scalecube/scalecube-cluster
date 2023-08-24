package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import reactor.core.publisher.Flux;

/**
 * Exactly the same example as {@link MessagingExample} but cluster transport is websocket.
 *
 * @see io.scalecube.transport.netty.websocket.WebsocketTransportFactory
 * @author arvy
 */
public class WebsocketMessagingExample {

  /** Main method. */
  public static void main(String[] args) throws Exception {
    // Start cluster node Alice to listen and respond for incoming greeting messages
    Cluster alice =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .handler(
                cluster -> {
                  return new ClusterMessageHandler() {
                    @Override
                    public void onMessage(Message msg) {
                      System.out.println("Alice received: " + msg.data());
                      cluster
                          .send(msg.sender(), Message.fromData("Greetings from Alice"))
                          .subscribe(null, Throwable::printStackTrace);
                    }
                  };
                })
            .startAwait();

    // Join cluster node Bob to cluster with Alice, listen and respond for incoming greeting
    // messages
    Cluster bob =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.seedMembers(alice.addresses()))
            .handler(
                cluster -> {
                  return new ClusterMessageHandler() {
                    @Override
                    public void onMessage(Message msg) {
                      System.out.println("Bob received: " + msg.data());
                      cluster
                          .send(msg.sender(), Message.fromData("Greetings from Bob"))
                          .subscribe(null, Throwable::printStackTrace);
                    }
                  };
                })
            .startAwait();

    // Join cluster node Carol to cluster with Alice and Bob
    Cluster carol =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.seedMembers(alice.addresses().get(0), bob.addresses().get(0)))
            .handler(
                cluster -> new ClusterMessageHandler() {
                  @Override
                  public void onMessage(Message msg) {
                    System.out.println("Carol received: " + msg.data());
                  }
                })
            .startAwait();

    // Send from Carol greeting message to all other cluster members (which is Alice and Bob)
    Message greetingMsg = Message.fromData("Greetings from Carol");

    Flux.fromIterable(carol.otherMembers())
        .flatMap(member -> carol.send(member, greetingMsg))
        .subscribe(null, Throwable::printStackTrace);

    // Avoid exit main thread immediately ]:->
    Thread.sleep(1000);
  }
}
