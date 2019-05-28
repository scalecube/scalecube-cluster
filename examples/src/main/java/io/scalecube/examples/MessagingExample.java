package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.transport.Message;

/**
 * Basic example for member transport between cluster members to run the example Start ClusterNodeA
 * and cluster ClusterNodeB A listen on transport messages B send message to member A.
 *
 * @author ronen hamias, Anton Kharenko
 */
public class MessagingExample {

  /** Main method. */
  public static void main(String[] args) throws Exception {
    // Start cluster node Alice to listen and respond for incoming greeting messages
    Cluster alice =
        new Cluster()
            .handler(
                cluster ->
                    new ClusterMessageHandler() {
                      @Override
                      public void onMessage(Message msg) {
                        System.out.println("Alice received: " + msg.data());
                        cluster.send(msg.sender(), Message.fromData("Greetings from Alice"));
                      }
                    })
            .startAwait();

    // Join cluster node Bob to cluster with Alice, listen and respond for incoming greeting
    // messages
    Cluster bob =
        new Cluster()
            .seedMembers(alice.address())
            .handler(
                cluster ->
                    new ClusterMessageHandler() {
                      @Override
                      public void onMessage(Message msg) {
                        System.out.println("Bob received: " + msg.data());
                        cluster.send(msg.sender(), Message.fromData("Greetings from Bob"));
                      }
                    })
            .startAwait();

    // Join cluster node Carol to cluster with Alice and Bob
    Cluster carol =
        new Cluster()
            .seedMembers(alice.address(), bob.address())
            .handler(
                cluster ->
                    new ClusterMessageHandler() {
                      @Override
                      public void onMessage(Message msg) {
                        System.out.println("Carol received: " + msg.data());
                      }
                    })
            .startAwait();

    // Send from Carol greeting message to all other cluster members (which is Alice and Bob)
    Message greetingMsg = Message.fromData("Greetings from Carol");
    carol.otherMembers().forEach(member -> carol.send(member, greetingMsg));

    // Avoid exit main thread immediately ]:->
    Thread.sleep(1000);
  }
}
