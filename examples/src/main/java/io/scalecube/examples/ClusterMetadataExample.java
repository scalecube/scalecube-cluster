package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import java.util.Collections;
import java.util.Optional;

/**
 * Using Cluster metadata: metadata is set of custom parameters that may be used by application
 * developers to attach additional business information and identifications to cluster members.
 *
 * <p>in this example we see how to attach logical name to a cluster member we nick name Joe
 *
 * @author ronen_h, Anton Kharenko
 */
public class ClusterMetadataExample {

  /** Main method. */
  public static void main(String[] args) throws Exception {
    // Start seed cluster member Alice
    Cluster alice = new ClusterImpl().transportFactory(TcpTransportFactory::new).startAwait();

    // Join Joe to cluster with metadata and listen for incoming messages and print them to stdout
    //noinspection unused
    Cluster joe =
        new ClusterImpl()
            .config(opts -> opts.metadata(Collections.singletonMap("name", "Joe")))
            .membership(opts -> opts.seedMembers(alice.address()))
            .transportFactory(TcpTransportFactory::new)
            .handler(
                cluster -> {
                  //noinspection CodeBlock2Expr
                  return new ClusterMessageHandler() {
                    @Override
                    public void onMessage(Message message) {
                      System.out.println("joe.listen(): " + message.data());
                    }
                  };
                })
            .startAwait();

    // Scan the list of members in the cluster and find Joe there
    Optional<Member> joeMemberOptional = alice.otherMembers().stream().findAny();

    System.err.println("### joeMemberOptional: " + joeMemberOptional);
    System.err.println("### joeMetadata: " + alice.metadata(joeMemberOptional.get()));
  }
}
