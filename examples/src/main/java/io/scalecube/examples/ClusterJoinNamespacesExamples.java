package io.scalecube.examples;

import static java.util.stream.Collectors.joining;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.Member;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;

public class ClusterJoinNamespacesExamples {

  /** Main method. */
  public static void main(String[] args) {
    // Start seed member Alice
    Cluster alice =
        new ClusterImpl()
            .config(opts -> opts.memberAlias("Alice"))
            .membership(opts -> opts.namespace("alice"))
            .transportFactory(TcpTransportFactory::new)
            .startAwait();

    // Join Bob to cluster (seed: Alice)
    Cluster bob =
        new ClusterImpl()
            .config(opts -> opts.memberAlias("Bob"))
            .membership(opts -> opts.namespace("alice/bob-and-carol"))
            .membership(opts -> opts.seedMembers(alice.addresses()))
            .transportFactory(TcpTransportFactory::new)
            .startAwait();

    // Join Carol to cluster (seed: Alice and Bob)
    Cluster carol =
        new ClusterImpl()
            .config(opts -> opts.memberAlias("Carol"))
            .membership(opts -> opts.namespace("alice/bob-and-carol"))
            .membership(opts -> opts.seedMembers(alice.addresses()))
            .transportFactory(TcpTransportFactory::new)
            .startAwait();

    Cluster bobAndCarolChild1 =
        new ClusterImpl()
            .config(opts -> opts.memberAlias("Bob-and-Carol-Child-1"))
            .membership(opts -> opts.namespace("alice/bob-and-carol/child-1"))
            .membership(opts -> opts.seedMembers(alice.addresses()))
            .transportFactory(TcpTransportFactory::new)
            .startAwait();

    Cluster carolChild2 =
        new ClusterImpl()
            .config(opts -> opts.memberAlias("Bob-and-Carol-Child-2"))
            .membership(opts -> opts.namespace("alice/bob-and-carol/child-2"))
            .membership(opts -> opts.seedMembers(alice.addresses()))
            .transportFactory(TcpTransportFactory::new)
            .startAwait();

    // Join Dan to cluster
    Cluster dan =
        new ClusterImpl()
            .config(opts -> opts.memberAlias("Dan"))
            .membership(opts -> opts.namespace("alice/dan-and-eve"))
            .membership(opts -> opts.seedMembers(alice.addresses()))
            .transportFactory(TcpTransportFactory::new)
            .startAwait();

    // Join Eve to cluster
    Cluster eve =
        new ClusterImpl()
            .config(opts -> opts.memberAlias("Eve"))
            .membership(opts -> opts.namespace("alice/dan-and-eve"))
            .membership(opts -> opts.seedMembers(alice.addresses()))
            .transportFactory(TcpTransportFactory::new)
            .startAwait();

    // Print cluster members of each node

    System.out.println(
        "Alice ("
            + alice.addresses()
            + ") cluster: "
            + alice.members().stream().map(Member::toString).collect(joining("\n", "\n", "\n")));

    System.out.println(
        "Bob ("
            + bob.addresses()
            + ") cluster: "
            + bob.members().stream().map(Member::toString).collect(joining("\n", "\n", "\n")));

    System.out.println(
        "Carol ("
            + carol.addresses()
            + ") cluster: "
            + carol.members().stream().map(Member::toString).collect(joining("\n", "\n", "\n")));

    System.out.println(
        "Dan ("
            + dan.addresses()
            + ") cluster: "
            + dan.members().stream().map(Member::toString).collect(joining("\n", "\n", "\n")));

    System.out.println(
        "Eve ("
            + eve.addresses()
            + ") cluster: " // alone in cluster
            + eve.members().stream().map(Member::toString).collect(joining("\n", "\n", "\n")));

    System.out.println(
        "Bob-And-Carol-Child-1 ("
            + bobAndCarolChild1.addresses()
            + ") cluster: " // alone in cluster
            + bobAndCarolChild1.members().stream()
                .map(Member::toString)
                .collect(joining("\n", "\n", "\n")));

    System.out.println(
        "Bob-And-Carol-Child-2 ("
            + carolChild2.addresses()
            + ") cluster: " // alone in cluster
            + carolChild2.members().stream()
                .map(Member::toString)
                .collect(joining("\n", "\n", "\n")));
  }
}
