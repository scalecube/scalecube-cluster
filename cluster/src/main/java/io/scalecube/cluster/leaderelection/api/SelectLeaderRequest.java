package io.scalecube.cluster.leaderelection.api;

public class SelectLeaderRequest {

  private String serviceName;

  public String serviceName() {
    return serviceName;
  }
}
