package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMath;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.fdetector.FailureDetectorConfig;
import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.cluster.membership.MembershipEvent;
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
    Cluster alice =
        new ClusterImpl()
            .config(opts -> opts.memberAlias("Alice"))
            .config(opts -> opts.metadata(Collections.singletonMap("name", "Alice")))
            .handler(
                cluster -> {
                  return new ClusterMessageHandler() {
                    @Override
                    public void onMembershipEvent(MembershipEvent event) {
                      System.out.println(now() + " Alice received: " + event);
                    }
                  };
                })
            .startAwait();
    System.out.println(now() + " Alice join members: " + alice.members());

    // Bob join cluster
    Cluster bob =
        new ClusterImpl()
            .config(opts -> opts.memberAlias("Bob"))
            .config(opts -> opts.metadata(Collections.singletonMap("name", "Bob")))
            .membership(opts -> opts.seedMembers(alice.address()))
            .handler(
                cluster -> {
                  return new ClusterMessageHandler() {
                    @Override
                    public void onMembershipEvent(MembershipEvent event) {
                      System.out.println(now() + " Bob received: " + event);
                    }
                  };
                })
            .startAwait();
    System.out.println(now() + " Bob join members: " + bob.members());

    // Carol join cluster
    Cluster carol =
        new ClusterImpl()
            .config(opts -> opts.memberAlias("Carol"))
            .config(opts -> opts.metadata(Collections.singletonMap("name", "Carol")))
            .membership(opts -> opts.seedMembers(bob.address()))
            .handler(
                cluster -> {
                  return new ClusterMessageHandler() {
                    @Override
                    public void onMembershipEvent(MembershipEvent event) {
                      System.out.println(now() + " Carol received: " + event);
                    }
                  };
                })
            .startAwait();
    System.out.println(now() + " Carol join members: " + carol.members());

    // Bob leave cluster
    bob.shutdown();
    bob.onShutdown().block();

    // Avoid exit main thread immediately ]:->
    long pingInterval = FailureDetectorConfig.DEFAULT_PING_INTERVAL;
    long suspicionTimeout =
        ClusterMath.suspicionTimeout(MembershipConfig.DEFAULT_SUSPICION_MULT, 4, pingInterval);
    long maxRemoveTimeout = suspicionTimeout + 3 * pingInterval;
    Thread.sleep(maxRemoveTimeout);
  }

  private static String now() {
    return sdf.format(new Date());
  }
}
