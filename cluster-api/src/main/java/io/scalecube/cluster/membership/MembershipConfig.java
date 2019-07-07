package io.scalecube.cluster.membership;

import io.scalecube.net.Address;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import reactor.core.Exceptions;

public final class MembershipConfig implements Cloneable {

  // Default settings for LAN cluster
  public static final int DEFAULT_SYNC_INTERVAL = 30_000;
  public static final int DEFAULT_SYNC_TIMEOUT = 3_000;
  public static final int DEFAULT_SUSPICION_MULT = 5;

  // Default settings for WAN cluster (overrides default/LAN settings)
  public static final int DEFAULT_WAN_SUSPICION_MULT = 6;
  public static final int DEFAULT_WAN_SYNC_INTERVAL = 60_000;

  // Default settings for local cluster working via loopback interface (overrides default/LAN
  // settings)
  public static final int DEFAULT_LOCAL_SUSPICION_MULT = 3;
  public static final int DEFAULT_LOCAL_SYNC_INTERVAL = 15_000;

  private List<Address> seedMembers = Collections.emptyList();
  private int syncInterval = DEFAULT_SYNC_INTERVAL;
  private int syncTimeout = DEFAULT_SYNC_TIMEOUT;
  private int suspicionMult = DEFAULT_SUSPICION_MULT;
  private String syncGroup = "default";

  public MembershipConfig() {}

  public static MembershipConfig defaultConfig() {
    return new MembershipConfig();
  }

  /**
   * Creates {@code MembershipConfig} with default settings for cluster on LAN network.
   *
   * @return new {@code MembershipConfig}
   */
  public static MembershipConfig defaultLanConfig() {
    return defaultConfig();
  }

  /**
   * Creates {@code MembershipConfig} with default settings for cluster on WAN network.
   *
   * @return new {@code MembershipConfig}
   */
  public static MembershipConfig defaultWanConfig() {
    return defaultConfig()
        .suspicionMult(DEFAULT_WAN_SUSPICION_MULT)
        .syncInterval(DEFAULT_WAN_SYNC_INTERVAL);
  }

  /**
   * Creates {@code MembershipConfig} with default settings for cluster on local loopback interface.
   *
   * @return new {@code MembershipConfig}
   */
  public static MembershipConfig defaultLocalConfig() {
    return defaultConfig()
        .suspicionMult(DEFAULT_LOCAL_SUSPICION_MULT)
        .syncInterval(DEFAULT_LOCAL_SYNC_INTERVAL);
  }

  public List<Address> seedMembers() {
    return seedMembers;
  }

  /**
   * Sets a seedMembers.
   *
   * @param seedMembers seed members
   * @return new {@code MembershipConfig} instance
   */
  public MembershipConfig seedMembers(Address... seedMembers) {
    return seedMembers(Arrays.asList(seedMembers));
  }

  /**
   * Sets a seedMembers.
   *
   * @param seedMembers seed members
   * @return new {@code MembershipConfig} instance
   */
  public MembershipConfig seedMembers(List<Address> seedMembers) {
    MembershipConfig m = clone();
    m.seedMembers = Collections.unmodifiableList(new ArrayList<>(seedMembers));
    return m;
  }

  public int syncInterval() {
    return syncInterval;
  }

  /**
   * Sets a syncInterval.
   *
   * @param syncInterval sync interval
   * @return new {@code MembershipConfig} instance
   */
  public MembershipConfig syncInterval(int syncInterval) {
    MembershipConfig m = clone();
    m.syncInterval = syncInterval;
    return m;
  }

  public int syncTimeout() {
    return syncTimeout;
  }

  /**
   * Sets a syncTimeout.
   *
   * @param syncTimeout sync timeout
   * @return new {@code MembershipConfig} instance
   */
  public MembershipConfig syncTimeout(int syncTimeout) {
    MembershipConfig m = clone();
    m.syncTimeout = syncTimeout;
    return m;
  }

  public int suspicionMult() {
    return suspicionMult;
  }

  /**
   * Sets a suspicionMult.
   *
   * @param suspicionMult suspicion multiplier
   * @return new {@code MembershipConfig} instance
   */
  public MembershipConfig suspicionMult(int suspicionMult) {
    MembershipConfig m = clone();
    m.suspicionMult = suspicionMult;
    return m;
  }

  public String syncGroup() {
    return syncGroup;
  }

  /**
   * Sets a syncGroup.
   *
   * @param syncGroup sync group
   * @return new {@code MembershipConfig} instance
   */
  public MembershipConfig syncGroup(String syncGroup) {
    MembershipConfig m = clone();
    m.syncGroup = syncGroup;
    return m;
  }

  @Override
  public MembershipConfig clone() {
    try {
      return (MembershipConfig) super.clone();
    } catch (CloneNotSupportedException e) {
      throw Exceptions.propagate(e);
    }
  }

  @Override
  public String toString() {
    return "MembershipConfig{"
        + "seedMembers="
        + seedMembers
        + ", syncInterval="
        + syncInterval
        + ", syncTimeout="
        + syncTimeout
        + ", suspicionMult="
        + suspicionMult
        + ", syncGroup='"
        + syncGroup
        + '\''
        + '}';
  }
}
