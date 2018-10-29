package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterMath;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;

/**
 * Example of subscribing and listening for cluster membership events which is emmited when new
 * member joins or leave cluster.
 *
 * @author Anton Kharenko
 */
public class MembershipEventsExample {

  private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");

  /** Main method. */
  public static void main(String[] args) throws Exception {
    // Alice init cluster
    Cluster alice = Cluster.joinAwait(Collections.singletonMap("name", "Alice"));
    System.out.println(now() + " Alice join members: " + alice.members());
    alice
        .listenMembership()
        .subscribe(event -> System.out.println(now() + " Alice received: " + event));

    // Bob join cluster
    Cluster bob = Cluster.joinAwait(Collections.singletonMap("name", "Bob"), alice.address());
    System.out.println(now() + " Bob join members: " + bob.members());
    bob.listenMembership()
        .subscribe(event -> System.out.println(now() + " Bob received: " + event));

    // Carol join cluster
    Cluster carol =
        Cluster.joinAwait(
            Collections.singletonMap("name", "Carol"), alice.address(), bob.address());
    System.out.println(now() + " Carol join members: " + carol.members());
    carol
        .listenMembership()
        .subscribe(event -> System.out.println(now() + " Carol received: " + event));

    // Bob leave cluster
    bob.shutdown().block();

    // Avoid exit main thread immediately ]:->
    long pingInterval = ClusterConfig.DEFAULT_PING_INTERVAL;
    long suspicionTimeout =
        ClusterMath.suspicionTimeout(ClusterConfig.DEFAULT_SUSPICION_MULT, 4, pingInterval);
    long maxRemoveTimeout = suspicionTimeout + 3 * pingInterval;
    Thread.sleep(maxRemoveTimeout);
  }

  private static String now() {
    return sdf.format(new Date());
  }
}
