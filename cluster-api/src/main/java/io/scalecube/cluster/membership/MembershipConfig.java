package io.scalecube.cluster.membership;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.StringJoiner;

public final class MembershipConfig {

  public static final int DEFAULT_SYNC_INTERVAL = 3_000;
  public static final int DEFAULT_SYNC_TIMEOUT = 3_000;
  public static final int DEFAULT_SUSPICION_MULT = 5;
  public static final String DEFAULT_NAMESPACE = "default";

  public static final String SEED_MEMBERS_PROP_NAME = "scalecube.cluster.membership.seedMembers";
  public static final String SYNC_INTERVAL_PROP_NAME = "scalecube.cluster.membership.syncInterval";
  public static final String SYNC_TIMEOUT_PROP_NAME = "scalecube.cluster.membership.syncTimeout";
  public static final String SUSPICION_MULT_PROP_NAME =
      "scalecube.cluster.membership.suspicionMult";
  public static final String NAMESPACE_PROP_NAME = "scalecube.cluster.membership.namespace";

  private List<String> seedMembers;
  private int syncInterval;
  private int syncTimeout;
  private int suspicionMult;
  private String namespace;

  public MembershipConfig() {
    this(System.getProperties());
  }

  public MembershipConfig(Properties properties) {
    seedMembers(properties);
    syncInterval(properties);
    syncTimeout(properties);
    suspicionMult(properties);
    namespace(properties);
  }

  private static String getProperty(Properties properties, String name) {
    final var value = properties.getProperty(name);
    return "@null".equals(value) ? null : value;
  }

  private static String getProperty(Properties properties, String name, String defaultValue) {
    final var value = getProperty(properties, name);
    return value != null ? value : defaultValue;
  }

  private static int getProperty(Properties properties, String name, int defaultValue) {
    final var value = getProperty(properties, name);
    return value != null ? Integer.parseInt(value) : defaultValue;
  }

  public List<String> seedMembers() {
    return seedMembers;
  }

  public MembershipConfig seedMembers(String... seedMembers) {
    return seedMembers(Arrays.asList(seedMembers));
  }

  public MembershipConfig seedMembers(List<String> seedMembers) {
    this.seedMembers = List.copyOf(seedMembers);
    return this;
  }

  /**
   * Reads comma-separated seed members, e.g. {@code host1:4801,host2:4801}.
   *
   * @param properties properties
   * @return this
   */
  public MembershipConfig seedMembers(Properties properties) {
    final var value = getProperty(properties, SEED_MEMBERS_PROP_NAME);
    if (value == null || value.isBlank()) {
      return seedMembers(Collections.emptyList());
    }
    return seedMembers(
        Arrays.stream(value.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList());
  }

  public int syncInterval() {
    return syncInterval;
  }

  public MembershipConfig syncInterval(int syncInterval) {
    this.syncInterval = syncInterval;
    return this;
  }

  public MembershipConfig syncInterval(Properties properties) {
    return syncInterval(getProperty(properties, SYNC_INTERVAL_PROP_NAME, DEFAULT_SYNC_INTERVAL));
  }

  public int syncTimeout() {
    return syncTimeout;
  }

  public MembershipConfig syncTimeout(int syncTimeout) {
    this.syncTimeout = syncTimeout;
    return this;
  }

  public MembershipConfig syncTimeout(Properties properties) {
    return syncTimeout(getProperty(properties, SYNC_TIMEOUT_PROP_NAME, DEFAULT_SYNC_TIMEOUT));
  }

  public int suspicionMult() {
    return suspicionMult;
  }

  public MembershipConfig suspicionMult(int suspicionMult) {
    this.suspicionMult = suspicionMult;
    return this;
  }

  public MembershipConfig suspicionMult(Properties properties) {
    return suspicionMult(getProperty(properties, SUSPICION_MULT_PROP_NAME, DEFAULT_SUSPICION_MULT));
  }

  public String namespace() {
    return namespace;
  }

  public MembershipConfig namespace(String namespace) {
    this.namespace = namespace;
    return this;
  }

  public MembershipConfig namespace(Properties properties) {
    return namespace(getProperty(properties, NAMESPACE_PROP_NAME, DEFAULT_NAMESPACE));
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", MembershipConfig.class.getSimpleName() + "[", "]")
        .add("seedMembers=" + seedMembers)
        .add("syncInterval=" + syncInterval)
        .add("syncTimeout=" + syncTimeout)
        .add("suspicionMult=" + suspicionMult)
        .add("namespace='" + namespace + "'")
        .toString();
  }
}
