package io.scalecube.cluster.fdetector;

import java.util.Properties;
import java.util.StringJoiner;

public final class FailureDetectorConfig {

  public static final int DEFAULT_PING_INTERVAL = 1_000;
  public static final int DEFAULT_PING_TIMEOUT = 500;
  public static final int DEFAULT_PING_REQ_MEMBERS = 3;

  public static final String PING_INTERVAL_PROP_NAME =
      "scalecube.cluster.failureDetector.pingInterval";
  public static final String PING_TIMEOUT_PROP_NAME =
      "scalecube.cluster.failureDetector.pingTimeout";
  public static final String PING_REQ_MEMBERS_PROP_NAME =
      "scalecube.cluster.failureDetector.pingReqMembers";

  private int pingInterval;
  private int pingTimeout;
  private int pingReqMembers;

  public FailureDetectorConfig() {
    this(System.getProperties());
  }

  public FailureDetectorConfig(Properties properties) {
    pingInterval(properties);
    pingTimeout(properties);
    pingReqMembers(properties);
  }

  private static String getProperty(Properties properties, String name) {
    final var value = properties.getProperty(name);
    return "@null".equals(value) ? null : value;
  }

  private static int getProperty(Properties properties, String name, int defaultValue) {
    final var value = getProperty(properties, name);
    return value != null ? Integer.parseInt(value) : defaultValue;
  }

  public int pingInterval() {
    return pingInterval;
  }

  public FailureDetectorConfig pingInterval(int pingInterval) {
    this.pingInterval = pingInterval;
    return this;
  }

  public FailureDetectorConfig pingInterval(Properties properties) {
    return pingInterval(getProperty(properties, PING_INTERVAL_PROP_NAME, DEFAULT_PING_INTERVAL));
  }

  public int pingTimeout() {
    return pingTimeout;
  }

  public FailureDetectorConfig pingTimeout(int pingTimeout) {
    this.pingTimeout = pingTimeout;
    return this;
  }

  public FailureDetectorConfig pingTimeout(Properties properties) {
    return pingTimeout(getProperty(properties, PING_TIMEOUT_PROP_NAME, DEFAULT_PING_TIMEOUT));
  }

  public int pingReqMembers() {
    return pingReqMembers;
  }

  public FailureDetectorConfig pingReqMembers(int pingReqMembers) {
    this.pingReqMembers = pingReqMembers;
    return this;
  }

  public FailureDetectorConfig pingReqMembers(Properties properties) {
    return pingReqMembers(
        getProperty(properties, PING_REQ_MEMBERS_PROP_NAME, DEFAULT_PING_REQ_MEMBERS));
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", FailureDetectorConfig.class.getSimpleName() + "[", "]")
        .add("pingInterval=" + pingInterval)
        .add("pingTimeout=" + pingTimeout)
        .add("pingReqMembers=" + pingReqMembers)
        .toString();
  }
}
