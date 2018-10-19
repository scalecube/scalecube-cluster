package io.scalecube.cluster.membership;

import io.scalecube.cluster.Member;
import java.util.Objects;

/**
 * Event which is emitted on cluster membership changes when new member added or removed from
 * cluster.
 */
public final class MembershipEvent {

  public enum Type {
    ADDED,
    REMOVED,
    UPDATED
  }

  private final Type type;
  private final Member newMember;
  private final Member oldMember;

  private MembershipEvent(Type type, Member oldMember, Member newMember) {
    this.type = Objects.requireNonNull(type);
    this.oldMember = oldMember;
    this.newMember = newMember;
  }

  static MembershipEvent createRemoved(Member member) {
    return new MembershipEvent(Type.REMOVED, member, null);
  }

  static MembershipEvent createAdded(Member member) {
    return new MembershipEvent(Type.ADDED, null, member);
  }

  static MembershipEvent createUpdated(Member oldMember, Member newMember) {
    return new MembershipEvent(Type.UPDATED, oldMember, newMember);
  }

  public Type type() {
    return type;
  }

  public boolean isAdded() {
    return type == Type.ADDED;
  }

  public boolean isRemoved() {
    return type == Type.REMOVED;
  }

  public boolean isUpdated() {
    return type == Type.UPDATED;
  }

  public Member member() {
    return type == Type.REMOVED ? oldMember : newMember;
  }

  public Member oldMember() {
    return oldMember;
  }

  public Member newMember() {
    return newMember;
  }

  @Override
  public String toString() {
    return "MembershipEvent{type="
        + type
        + ", newMember="
        + newMember
        + ", oldMember="
        + oldMember
        + '}';
  }
}
