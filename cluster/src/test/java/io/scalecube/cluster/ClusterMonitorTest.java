package io.scalecube.cluster;

import org.junit.jupiter.api.Test;


//TODO: remove
public class ClusterMonitorTest {
  @Test

  public void testMonitor()throws Exception{
    ClusterImpl seedNode  = (ClusterImpl) Cluster.joinAwait();
    Cluster otherNode = Cluster.joinAwait(seedNode.address());

    Thread.sleep(60_000);

  }

}
