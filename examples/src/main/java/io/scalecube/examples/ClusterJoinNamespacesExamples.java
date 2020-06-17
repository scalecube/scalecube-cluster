package io.scalecube.examples;

import static java.util.stream.Collectors.joining;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.Member;

public class ClusterJoinNamespacesExamples {

  /** Main method. */
  public static void main(String[] args) {
    // Start seed member Alice
    Cluster alice =
        new ClusterImpl()
            .config(opts -> opts.memberAlias("Alice"))
            .membership(opts -> opts.namespace("alice"))
            .startAwait();

    // Join Bob to cluster (seed: Alice)
    Cluster bob =
        new ClusterImpl()
            .config(opts -> opts.memberAlias("Bob"))
            .membership(opts -> opts.namespace("alice/bob-and-carol"))
            .membership(opts -> opts.seedMembers(alice.address()))
            .startAwait();

    // Join Carol to cluster (seed: Alice and Bob)
    Cluster carol =
        new ClusterImpl()
            .config(opts -> opts.memberAlias("Carol"))
            .membership(opts -> opts.namespace("alice/bob-and-carol"))
            .membership(opts -> opts.seedMembers(alice.address()))
            .startAwait();

    Cluster bobAndCarolChild1 =
        new ClusterImpl()
            .config(opts -> opts.memberAlias("Bob-and-Carol-Child-1"))
            .membership(opts -> opts.namespace("alice/bob-and-carol/child-1"))
            .membership(opts -> opts.seedMembers(alice.address()))
            .startAwait();

    Cluster carolChild2 =
        new ClusterImpl()
            .config(opts -> opts.memberAlias("Bob-and-Carol-Child-2"))
            .membership(opts -> opts.namespace("alice/bob-and-carol/child-2"))
            .membership(opts -> opts.seedMembers(alice.address()))
            .startAwait();

    // Join Dan to cluster
    Cluster dan =
        new ClusterImpl()
            .config(opts -> opts.memberAlias("Dan"))
            .membership(opts -> opts.namespace("alice/dan-and-eve"))
            .membership(opts -> opts.seedMembers(alice.address()))
            .startAwait();

    // Join Eve to cluster
    Cluster eve =
        new ClusterImpl()
            .config(opts -> opts.memberAlias("Eve"))
            .membership(opts -> opts.namespace("alice/dan-and-eve"))
            .membership(opts -> opts.seedMembers(alice.address()))
            .startAwait();

    // Print cluster members of each node

    System.out.println(
        "Alice ("
            + alice.address()
            + ") cluster: "
            + alice.members().stream().map(Member::toString).collect(joining("\n", "\n", "\n")));

    System.out.println(
        "Bob ("
            + bob.address()
            + ") cluster: "
            + bob.members().stream().map(Member::toString).collect(joining("\n", "\n", "\n")));

    System.out.println(
        "Carol ("
            + carol.address()
            + ") cluster: "
            + carol.members().stream().map(Member::toString).collect(joining("\n", "\n", "\n")));

    System.out.println(
        "Dan ("
            + dan.address()
            + ") cluster: "
            + dan.members().stream().map(Member::toString).collect(joining("\n", "\n", "\n")));

    System.out.println(
        "Eve ("
            + eve.address()
            + ") cluster: " // alone in cluster
            + eve.members().stream().map(Member::toString).collect(joining("\n", "\n", "\n")));

    System.out.println(
        "Bob-And-Carol-Child-1 ("
            + bobAndCarolChild1.address()
            + ") cluster: " // alone in cluster
            + bobAndCarolChild1.members().stream()
                .map(Member::toString)
                .collect(joining("\n", "\n", "\n")));

    System.out.println(
        "Bob-And-Carol-Child-2 ("
            + carolChild2.address()
            + ") cluster: " // alone in cluster
            + carolChild2.members().stream()
                .map(Member::toString)
                .collect(joining("\n", "\n", "\n")));
  }
}
