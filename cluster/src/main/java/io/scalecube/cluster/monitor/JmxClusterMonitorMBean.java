package io.scalecube.cluster.monitor;

import io.scalecube.cluster.Member;
import io.scalecube.net.Address;
import java.util.Optional;
import java.util.stream.Collectors;

public class JmxClusterMonitorMBean implements ClusterMonitorMBean {

  private final ClusterMonitorModel monitorModel;

  public JmxClusterMonitorMBean(ClusterMonitorModel monitorModel) {
    this.monitorModel = monitorModel;
  }

  @Override
  public String getClusterConfig() {
    return String.valueOf(monitorModel.config());
  }

  @Override
  public int getClusterSize() {
    return monitorModel.clusterSize();
  }

  @Override
  public int getMemberIncarnation() {
    return monitorModel.incarnation();
  }

  @Override
  public String getMember() {
    return String.valueOf(monitorModel.member());
  }

  @Override
  public String getMetadata() {
    return String.valueOf(
        Optional.ofNullable(monitorModel.metadata()).map(Object::toString).orElse(null));
  }

  @Override
  public String getSeedMembers() {
    return monitorModel.seedMembers().stream()
        .map(Address::toString)
        .collect(Collectors.joining(",", "[", "]"));
  }

  @Override
  public String getAliveMembers() {
    return monitorModel.aliveMembers().stream()
        .map(Member::toString)
        .collect(Collectors.joining(",", "[", "]"));
  }

  @Override
  public String getSuspectedMembers() {
    return monitorModel.suspectedMembers().stream()
        .map(Member::toString)
        .collect(Collectors.joining(",", "[", "]"));
  }

  @Override
  public String getRemovedMembers() {
    return monitorModel.removedMembers().stream()
        .map(Member::toString)
        .collect(Collectors.joining(",", "[", "]"));
  }
}
