package io.scalecube.cluster.membership;

import static io.scalecube.cluster.membership.MemberStatus.ALIVE;
import static io.scalecube.cluster.membership.MemberStatus.DEAD;
import static io.scalecube.cluster.membership.MemberStatus.LEAVING;
import static io.scalecube.cluster.membership.MemberStatus.SUSPECT;

import io.scalecube.cluster.Member;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

/** Cluster membership record which represents member, status, and incarnation. */
final class MembershipRecord implements Externalizable {

  private static final long serialVersionUID = 1L;

  private Member member;
  private MemberStatus status;
  private int incarnation;

  public MembershipRecord() {}

  /** Instantiates new instance of membership record with given member, status and incarnation. */
  public MembershipRecord(Member member, MemberStatus status, int incarnation) {
    this.member = Objects.requireNonNull(member);
    this.status = Objects.requireNonNull(status);
    this.incarnation = incarnation;
  }

  public Member member() {
    return member;
  }

  public MemberStatus status() {
    return status;
  }

  public boolean isAlive() {
    return status == ALIVE;
  }

  public boolean isSuspect() {
    return status == SUSPECT;
  }

  public boolean isLeaving() {
    return status == LEAVING;
  }

  public boolean isDead() {
    return status == DEAD;
  }

  public int incarnation() {
    return incarnation;
  }

  /**
   * Checks either this record overrides given record.
   *
   * @param r0 existing record in membership table
   * @return true if this record overrides exiting; false otherwise
   */
  public boolean isOverrides(MembershipRecord r0) {
    if (r0 == null) {
      return isAlive() || isLeaving();
    }
    if (!Objects.equals(member.id(), r0.member.id())) {
      throw new IllegalArgumentException("Can't compare records for different members");
    }
    if (this.equals(r0)) {
      return false;
    }
    if (r0.isDead()) {
      return false;
    }
    if (isDead()) {
      return true;
    }
    if (incarnation == r0.incarnation) {
      return isSuspect() && (r0.isAlive() || r0.isLeaving());
    } else {
      return incarnation > r0.incarnation;
    }
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that == null || getClass() != that.getClass()) {
      return false;
    }
    MembershipRecord record = (MembershipRecord) that;
    return incarnation == record.incarnation
        && Objects.equals(member, record.member)
        && status == record.status;
  }

  @Override
  public int hashCode() {
    return Objects.hash(member, status, incarnation);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // member
    out.writeObject(member);
    // status
    out.writeObject(status);
    // incarnation
    out.writeInt(incarnation);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // member
    member = (Member) in.readObject();
    // status
    status = (MemberStatus) in.readObject();
    // incarnation
    incarnation = in.readInt();
  }

  @Override
  public String toString() {
    return "{m: " + member + ", s: " + status + ", inc: " + incarnation + '}';
  }
}
