package io.scalecube.cluster.membership;

import static io.scalecube.cluster.membership.MemberStatus.ALIVE;
import static io.scalecube.cluster.membership.MemberStatus.DEAD;
import static io.scalecube.cluster.membership.MemberStatus.SUSPECT;

import io.scalecube.cluster.Member;
import io.scalecube.transport.Address;
import java.util.Objects;

/** Cluster membership record which represents member, status, and incarnation. */
final class MembershipRecord {

  private Member member;
  private MemberStatus status;
  private int incarnation;

  /** Instantiates empty membership record for deserialization purpose. */
  MembershipRecord() {}

  /** Instantiates new instance of membership record with given member, status and incarnation. */
  public MembershipRecord(Member member, MemberStatus status, int incarnation) {
    this.member = Objects.requireNonNull(member);
    this.status = Objects.requireNonNull(status);
    this.incarnation = incarnation;
  }

  public Member member() {
    return member;
  }

  public String id() {
    return member.id();
  }

  public Address address() {
    return member.address();
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
      return isAlive();
    }
    if (!Objects.equals(this.member.id(), r0.member.id())) {
      throw new IllegalArgumentException("Can't compare records for different members");
    }
    if (r0.status == DEAD) {
      return false;
    }
    if (status == DEAD) {
      return true;
    }
    if (incarnation == r0.incarnation) {
      return (status != r0.status) && (status == SUSPECT);
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
  public String toString() {
    return "{m: " + member + ", s: " + status + ", inc: " + incarnation + '}';
  }
}
