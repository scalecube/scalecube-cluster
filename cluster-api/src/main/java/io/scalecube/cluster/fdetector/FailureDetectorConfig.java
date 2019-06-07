package io.scalecube.cluster.fdetector;

import reactor.core.Exceptions;

public final class FailureDetectorConfig implements Cloneable {

  // Default settings for LAN cluster
  public static final int DEFAULT_PING_INTERVAL = 1_000;
  public static final int DEFAULT_PING_TIMEOUT = 500;
  public static final int DEFAULT_PING_REQ_MEMBERS = 3;

  // Default settings for WAN cluster (overrides default/LAN settings)
  public static final int DEFAULT_WAN_SYNC_INTERVAL = 60_000;
  public static final int DEFAULT_WAN_PING_TIMEOUT = 3_000;
  public static final int DEFAULT_WAN_PING_INTERVAL = 5_000;

  // Default settings for local cluster working via loopback interface (overrides default/LAN
  // settings)
  public static final int DEFAULT_LOCAL_PING_TIMEOUT = 200;
  public static final int DEFAULT_LOCAL_PING_INTERVAL = 1_000;
  public static final int DEFAULT_LOCAL_PING_REQ_MEMBERS = 1;

  private int pingInterval = DEFAULT_PING_INTERVAL;
  private int pingTimeout = DEFAULT_PING_TIMEOUT;
  private int pingReqMembers = DEFAULT_PING_REQ_MEMBERS;

  public FailureDetectorConfig() {
  }

  public static FailureDetectorConfig

  public int pingInterval() {
    return pingInterval;
  }

  public int pingTimeout() {
    return pingTimeout;
  }

  public int pingReqMembers() {
    return pingReqMembers;
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
    return "FailureDetectorConfig{"
        + "pingInterval="
        + pingInterval
        + ", pingTimeout="
        + pingTimeout
        + ", pingReqMembers="
        + pingReqMembers
        + '}';
  }
}
