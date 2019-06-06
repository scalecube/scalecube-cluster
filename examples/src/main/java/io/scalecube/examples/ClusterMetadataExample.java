package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.transport.api.Message;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

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
    Cluster alice = new ClusterImpl().startAwait();

    // Join Joe to cluster with metadata and listen for incoming messages and print them to stdout
    //noinspection unused
    Cluster joe =
        new ClusterImpl()
            .config(
                options ->
                    options
                        .seedMembers(alice.address())
                        .metadata(Collections.singletonMap("name", "Joe")))
            .handler(
                cluster -> {
                  return new ClusterMessageHandler() {
                    @Override
                    public void onMessage(Message message) {
                      System.out.println("joe.listen(): " + message.data());
                    }
                  };
                })
            .startAwait();

    // Scan the list of members in the cluster and find Joe there
    Optional<Member> joeMemberOptional =
        alice.otherMembers().stream()
            .filter(
                member -> {
                  //noinspection unchecked
                  Map<String, String> metadata = (Map<String, String>) alice.metadata(member).get();
                  return "Joe".equals(metadata.get("name"));
                })
            .findAny();

    System.err.println("### joeMemberOptional: " + joeMemberOptional);

    // Send hello to Joe
    joeMemberOptional.ifPresent(
        member ->
            alice
                .send(member, Message.withData("Hello Joe").build())
                .subscribe(
                    null,
                    ex -> {
                      // no-op
                    }));

    TimeUnit.SECONDS.sleep(3);
  }
}
