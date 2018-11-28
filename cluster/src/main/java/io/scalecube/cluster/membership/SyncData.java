package io.scalecube.cluster.membership;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A class containing full membership table from specific member and used full synchronization
 * between cluster members.
 */
final class SyncData {

  /** Full cluster membership table. */
  private List<MembershipRecord> membership;

  /**
   * Sort of cluster identifier. Only members in the same sync group allowed to join into cluster.
   */
  private String syncGroup;

  /** Instantiates empty sync data for deserialization purpose. */
  SyncData() {}

  public SyncData(Collection<MembershipRecord> membership, String syncGroup) {
    this.membership = new ArrayList<>(membership);
    this.syncGroup = syncGroup;
  }

  public Collection<MembershipRecord> getMembership() {
    return new ArrayList<>(membership);
  }

  public String getSyncGroup() {
    return syncGroup;
  }

  @Override
  public String toString() {
    return "SyncData{membership=" + membership + ", syncGroup=" + syncGroup + '}';
  }
}
