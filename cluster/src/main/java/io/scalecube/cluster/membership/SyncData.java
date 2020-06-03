package io.scalecube.cluster.membership;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

  public SyncData() {}

  public SyncData(Collection<MembershipRecord> membership) {
    Objects.requireNonNull(membership);
    this.membership = Collections.unmodifiableList(new ArrayList<>(membership));
  }

  public Collection<MembershipRecord> getMembership() {
    return membership;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // membership
    out.writeInt(membership.size());
    for (MembershipRecord record : membership) {
      out.writeObject(record);
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // membership
    int membershipSize = in.readInt();
    List<MembershipRecord> membership = new ArrayList<>(membershipSize);
    for (int i = 0; i < membershipSize; i++) {
      membership.add((MembershipRecord) in.readObject());
    }
    this.membership = Collections.unmodifiableList(membership);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", SyncData.class.getSimpleName() + "[", "]")
        .add("membership=" + membership)
        .toString();
  }
}
