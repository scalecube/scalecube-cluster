package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterMath;
import io.scalecube.cluster.ClusterMessageHandler;
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
        new Cluster()
            .metadata(Collections.singletonMap("name", "Alice"))
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
        new Cluster()
            .seedMembers(alice.address())
            .metadata(Collections.singletonMap("name", "Bob"))
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
        new Cluster()
            .seedMembers(alice.address(), bob.address())
            .metadata(Collections.singletonMap("name", "Carol"))
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
