package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import java.time.Duration;
import reactor.core.publisher.Mono;

public class ClusterMonitorTest {

  public static void main(String[] args) throws InterruptedException {
    Cluster seedNode = Cluster.joinAwait();

    Cluster otherNode = Cluster.joinAwait(seedNode.address());

    Mono.delay(Duration.ofSeconds(3))
      .doOnSuccess(aLong -> {
        seedNode.shutdown().subscribe();
      })
      .subscribe();

    Thread.sleep(600_000);
  }
}
