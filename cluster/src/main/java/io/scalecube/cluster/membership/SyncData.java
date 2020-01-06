package io.scalecube.cluster.membership;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * A class containing full membership table from specific member and used full synchronization
 * between cluster members.
 */
final class SyncData implements Externalizable {

  private static final long serialVersionUID = 1L;

  /** Full cluster membership table. */
  private List<MembershipRecord> membership;

  /**
   * Sort of cluster identifier. Only members in the same sync group allowed to join into cluster.
   */
  private String syncGroup;

  public SyncData() {}

  public SyncData(Collection<MembershipRecord> membership, String syncGroup) {
    Objects.requireNonNull(membership);
    Objects.requireNonNull(syncGroup);
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
  public void writeExternal(ObjectOutput out) throws IOException {
    // membership
    out.writeInt(membership.size());
    for (MembershipRecord record : membership) {
      out.writeObject(record);
    }
    // syncGroup
    out.writeUTF(syncGroup);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // membership
    int size = in.readInt();
    membership = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      membership.add((MembershipRecord) in.readObject());
    }
    // syncGroup
    syncGroup = in.readUTF();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", SyncData.class.getSimpleName() + "[", "]")
        .add("membership=" + membership)
        .add("syncGroup='" + syncGroup + "'")
        .toString();
  }
}
