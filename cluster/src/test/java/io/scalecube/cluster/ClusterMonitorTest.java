package io.scalecube.cluster;

import org.junit.jupiter.api.Test;

public class ClusterMonitorTest {
  @Test

  public void testMonitor()throws Exception{
    ClusterImpl seedNode  = (ClusterImpl) Cluster.joinAwait();
    Cluster otherNode = Cluster.joinAwait(seedNode.address());


    //ClusterImpl.ClusterMonitor clusterMonitor = seedNode.new ClusterMonitor();
    //System.out.println(clusterMonitor.getMember());
    //System.out.println(clusterMonitor.getAliveMembers());
    //System.out.println(clusterMonitor.getIncarnation());

    Thread.sleep(100_000);

  }

}
