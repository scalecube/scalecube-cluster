package io.scalecube.examples;

import static java.util.stream.Collectors.joining;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.Member;
import java.util.UUID;

public class MemberIdExample {

  /** Main method. */
  public static void main(String[] args) {
    Cluster alice =
        new ClusterImpl()
            .config(opts -> opts.memberIdGenerator(() -> UUID.randomUUID().toString()))
            .startAwait();

    // Join Bob to cluster with Alice
    Cluster bob =
        new ClusterImpl()
            .config(opts -> opts.memberIdGenerator(() -> UUID.randomUUID().toString()))
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
  }
}
