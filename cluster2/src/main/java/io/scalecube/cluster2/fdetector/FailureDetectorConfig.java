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
    return defaultConfig().pingInterval(DEFAULT_WAN_PING_INTERVAL);
  }

  /**
   * Creates {@code FailureDetectorConfig} with default settings for cluster on local loopback
   * interface.
   *
   * @return new {@code FailureDetectorConfig}
   */
  public static FailureDetectorConfig defaultLocalConfig() {
    return defaultConfig()
        .pingInterval(DEFAULT_LOCAL_PING_INTERVAL)
        .pingReqMembers(DEFAULT_LOCAL_PING_REQ_MEMBERS);
  }

  /**
   * Setter for {@code pingInterval}.
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
   * Setter for number of members for requesting a ping.
   *
   * @param pingReqMembers number of members for requesting a ping
   * @return new {@code FailureDetectorConfig}
   */
  public FailureDetectorConfig pingReqMembers(int pingReqMembers) {
    FailureDetectorConfig f = clone();
    f.pingReqMembers = pingReqMembers;
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
    return new StringJoiner(", ", FailureDetectorConfig.class.getSimpleName() + "[", "]")
        .add("pingInterval=" + pingInterval)
        .add("pingReqMembers=" + pingReqMembers)
        .toString();
  }
}
