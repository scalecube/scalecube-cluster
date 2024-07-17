package io.scalecube.cluster2.membership;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import reactor.core.Exceptions;

public class MembershipConfig implements Cloneable {

  // Default settings for LAN cluster
  public static final int DEFAULT_SUSPICION_MULT = 5;
  public static final int DEFAULT_SYNC_INTERVAL = 30_000;

  // Default settings for WAN cluster (overrides default/LAN settings)
  public static final int DEFAULT_WAN_SUSPICION_MULT = 6;
  public static final int DEFAULT_WAN_SYNC_INTERVAL = 60_000;

  // Default settings for local cluster working via loopback interface (overrides default/LAN
  // settings)
  public static final int DEFAULT_LOCAL_SUSPICION_MULT = 3;
  public static final int DEFAULT_LOCAL_SYNC_INTERVAL = 3_000;

  private List<String> seedMembers = Collections.emptyList();
  private int syncInterval = DEFAULT_SYNC_INTERVAL;
  private int suspicionMult = DEFAULT_SUSPICION_MULT;
  private String namespace = "default";

  public MembershipConfig() {}

  public static MembershipConfig defaultLanConfig() {
    return new MembershipConfig();
  }

  public static MembershipConfig defaultWanConfig() {
    return new MembershipConfig()
        .suspicionMult(DEFAULT_WAN_SUSPICION_MULT)
        .syncInterval(DEFAULT_WAN_SYNC_INTERVAL);
  }

  public static MembershipConfig defaultLocalConfig() {
    return new MembershipConfig()
        .suspicionMult(DEFAULT_LOCAL_SUSPICION_MULT)
        .syncInterval(DEFAULT_LOCAL_SYNC_INTERVAL);
  }

  public List<String> seedMembers() {
    return seedMembers;
  }

  public MembershipConfig seedMembers(String... seedMembers) {
    return seedMembers(Arrays.asList(seedMembers));
  }

  public MembershipConfig seedMembers(List<String> seedMembers) {
    this.seedMembers = Collections.unmodifiableList(new ArrayList<>(seedMembers));
    return this;
  }

  public int syncInterval() {
    return syncInterval;
  }

  public MembershipConfig syncInterval(int syncInterval) {
    this.syncInterval = syncInterval;
    return this;
  }

  public int suspicionMult() {
    return suspicionMult;
  }

  public MembershipConfig suspicionMult(int suspicionMult) {
    this.suspicionMult = suspicionMult;
    return this;
  }

  public String namespace() {
    return namespace;
  }

  public MembershipConfig namespace(String namespace) {
    this.namespace = namespace;
    return this;
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
    return new StringJoiner(", ", MembershipConfig.class.getSimpleName() + "[", "]")
        .add("seedMembers=" + seedMembers)
        .add("syncInterval=" + syncInterval)
        .add("suspicionMult=" + suspicionMult)
        .add("namespace='" + namespace + "'")
        .toString();
  }
}
