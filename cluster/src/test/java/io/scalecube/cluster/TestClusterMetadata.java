package io.scalecube.cluster;

import io.scalecube.cluster.membership.MembershipEvent;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestClusterMetadata {

  @Test
  void testMetadata() throws InterruptedException {
    Cluster cluster0 = Cluster.joinAwait();
    Cluster cluster1 = Cluster.joinAwait(cluster0.address());

    cluster1
        .listenMembership()
        .filter(MembershipEvent::isUpdated)
        .subscribe(
            event -> {
              Map<String, String> metadata = cluster1.metadata(event.member());
              System.out.println(
                  "### metadata: "
                      + metadata
                      + " | "
                      + System.currentTimeMillis()
                      + " | "
                      + event.member().id()
                      + " | "
                      + Thread.currentThread().getName());
            });


    for (int i = 0; i < 5; i++) {
      cluster0.updateMetadataProperty("key", String.valueOf(i)).subscribe(); // 2
    }

    Thread.currentThread().join();
  }
}
