package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.Member;
import io.scalecube.transport.Message;
import java.util.Collections;
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
    Cluster alice = Cluster.joinAwait();

    // Join Joe to cluster with metadata
    Cluster joe = Cluster.joinAwait(Collections.singletonMap("name", "Joe"), alice.address());

    // Subscribe Joe to listen for incoming messages and print them to system out
    joe.listen()
        .map(Message::data)
        .subscribe(
            o -> System.err.println("joe.listen(): " + o),
            ex -> System.err.println("joe.listen(): " + ex));

    // Scan the list of members in the cluster and find Joe there
    Optional<Member> joeMemberOptional =
        alice
            .otherMembers()
            .stream()
            .filter(member -> "Joe".equals(alice.metadata(member).get("name")))
            .findAny();

    System.err.println("### joeMemberOptional: " + joeMemberOptional);

    // Send hello to Joe
    joeMemberOptional.ifPresent(
        member ->
            alice
                .send(member, Message.withData("Hello Joe").sender(alice.address()).build())
                .subscribe(
                    null,
                    ex -> {
                      // no-op
                    }));

    TimeUnit.SECONDS.sleep(3);
  }
}
