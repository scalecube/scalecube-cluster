package io.scalecube.cluster.fdetector;

import reactor.core.Exceptions;

public final class FailureDetectorConfig implements Cloneable {

  // Default settings for LAN cluster
  public static final int DEFAULT_PING_INTERVAL = 1_000;
  public static final int DEFAULT_PING_TIMEOUT = 500;
  public static final int DEFAULT_PING_REQ_MEMBERS = 3;

  // Default settings for WAN cluster (overrides default/LAN settings)
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

  public FailureDetectorConfig() {}

  public static FailureDetectorConfig defaultConfig() {
    return new FailureDetectorConfig();
  }

  /**
   * Creates {@code FailureDetectorConfig} with default settings for cluster on LAN network.
   *
   * @return new {@code FailureDetectorConfig}
   */
  public static FailureDetectorConfig defaultLanConfig() {
    return defaultConfig();
  }

  /**
   * Creates {@code FailureDetectorConfig} with default settings for cluster on WAN network.
   *
   * @return new {@code FailureDetectorConfig}
   */
  public static FailureDetectorConfig defaultWanConfig() {
    return defaultConfig()
        .pingTimeout(DEFAULT_WAN_PING_TIMEOUT)
        .pingInterval(DEFAULT_WAN_PING_INTERVAL);
  }

  /**
   * Creates {@code FailureDetectorConfig} with default settings for cluster on local loopback
   * interface.
   *
   * @return new {@code FailureDetectorConfig}
   */
  public static FailureDetectorConfig defaultLocalConfig() {
    return defaultConfig()
        .pingTimeout(DEFAULT_LOCAL_PING_TIMEOUT)
        .pingInterval(DEFAULT_LOCAL_PING_INTERVAL)
        .pingReqMembers(DEFAULT_LOCAL_PING_REQ_MEMBERS);
  }

  /**
   * Sets pingInterval.
   *
   * @param pingInterval ping interval
   * @return new {@code FailureDetectorConfig}
   */
  public FailureDetectorConfig pingInterval(int pingInterval) {
    FailureDetectorConfig f = clone();
    f.pingInterval = pingInterval;
    return f;
  }

  public int pingInterval() {
    return pingInterval;
  }

  /**
   * Sets ping timeout.
   *
   * @param pingTimeout ping timeout
   * @return new {@code FailureDetectorConfig}
   */
  public FailureDetectorConfig pingTimeout(int pingTimeout) {
    FailureDetectorConfig f = clone();
    f.pingTimeout = pingTimeout;
    return f;
  }

  public int pingTimeout() {
    return pingTimeout;
  }

  /**
   * Sets number of members for requesting a ping.
   *
   * @param pingReqMembers number of members for requesting a ping
   * @return new {@code FailureDetectorConfig}
   */
  public FailureDetectorConfig pingReqMembers(int pingReqMembers) {
    FailureDetectorConfig f = clone();
    this.pingReqMembers = pingReqMembers;
    return f;
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
