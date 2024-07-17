package io.scalecube.cluster2.fdetector;

import java.util.StringJoiner;
import reactor.core.Exceptions;

public class FailureDetectorConfig implements Cloneable {

  // Default settings for LAN cluster
  public static final int DEFAULT_PING_INTERVAL = 1_000;
  public static final int DEFAULT_PING_REQ_MEMBERS = 3;

  // Default settings for WAN cluster (overrides default/LAN settings)
  public static final int DEFAULT_WAN_PING_INTERVAL = 5_000;

  // Default settings for local cluster working via loopback interface (overrides default/LAN
  // settings)
  public static final int DEFAULT_LOCAL_PING_INTERVAL = 1_000;
  public static final int DEFAULT_LOCAL_PING_REQ_MEMBERS = 1;

  private int pingInterval = DEFAULT_PING_INTERVAL;
  private int pingReqMembers = DEFAULT_PING_REQ_MEMBERS;

  public FailureDetectorConfig() {}

  public static FailureDetectorConfig defaultLanConfig() {
    return new FailureDetectorConfig();
  }

  public static FailureDetectorConfig defaultWanConfig() {
    return new FailureDetectorConfig().pingInterval(DEFAULT_WAN_PING_INTERVAL);
  }

  public static FailureDetectorConfig defaultLocalConfig() {
    return new FailureDetectorConfig()
        .pingInterval(DEFAULT_LOCAL_PING_INTERVAL)
        .pingReqMembers(DEFAULT_LOCAL_PING_REQ_MEMBERS);
  }

  public int pingInterval() {
    return pingInterval;
  }

  public FailureDetectorConfig pingInterval(int pingInterval) {
    this.pingInterval = pingInterval;
    return this;
  }

  public int pingReqMembers() {
    return pingReqMembers;
  }

  public FailureDetectorConfig pingReqMembers(int pingReqMembers) {
    this.pingReqMembers = pingReqMembers;
    return this;
  }

  @Override
  public FailureDetectorConfig clone() {
    try {
      return (FailureDetectorConfig) super.clone();
    } catch (CloneNotSupportedException e) {
      throw Exceptions.propagate(e);
    }
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", FailureDetectorConfig.class.getSimpleName() + "[", "]")
        .add("pingInterval=" + pingInterval)
        .add("pingReqMembers=" + pingReqMembers)
        .toString();
  }
}
