package io.scalecube.cluster.monitor;

public interface ClusterMonitorMBean {

  String getClusterConfig();

  int getClusterSize();

  int getMemberIncarnation();

  String getMember();

  String getMetadata();

  String getSeedMembers();

  String getAliveMembers();

  String getSuspectedMembers();

  String getRemovedMembers();
}
