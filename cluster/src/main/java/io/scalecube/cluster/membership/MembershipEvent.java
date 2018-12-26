package io.scalecube.cluster.membership;

import io.scalecube.cluster.Member;
import java.util.Map;
import java.util.Objects;

/**
 * Event which is emitted on cluster membership changes when new member added, updated in the
 * cluster or removed from the cluster.
 */
public final class MembershipEvent {

  public enum Type {
    ADDED,
    REMOVED,
    UPDATED
  }

  private final Type type;
  private final Member member;
  private final Map<String, String> oldMetadata;
  private final Map<String, String> newMetadata;

  private MembershipEvent(
      Type type, Member member, Map<String, String> oldMetadata, Map<String, String> newMetadata) {
    this.type = type;
    this.member = member;
    this.oldMetadata = oldMetadata;
    this.newMetadata = newMetadata;
  }

  /**
   * Creates REMOVED membership event with cluster member and its metadata (optional).
   *
   * @param member cluster member; not null
   * @param metadata member metadata; optional
   * @return membership event
   */
  public static MembershipEvent createRemoved(Member member, Map<String, String> metadata) {
    Objects.requireNonNull(member, "member must be not null");
    return new MembershipEvent(Type.REMOVED, member, metadata, null);
  }

  /**
   * Creates ADDED membership event with cluster member and its metadata.
   *
   * @param member cluster memeber; not null
   * @param metadata member metadata; not null
   * @return membership event
   */
  public static MembershipEvent createAdded(Member member, Map<String, String> metadata) {
    Objects.requireNonNull(member, "member must be not null");
    Objects.requireNonNull(metadata, "metadata must be not null for ADDED event");
    return new MembershipEvent(Type.ADDED, member, null, metadata);
  }

  /**
   * Creates UPDATED membership event.
   *
   * @param member cluster member; not null
   * @param oldMetadata previous metadata; not null
   * @param newMetadata new metadata; not null
   * @return membership event
   */
  public static MembershipEvent createUpdated(
      Member member, Map<String, String> oldMetadata, Map<String, String> newMetadata) {
    Objects.requireNonNull(member, "member must be not null");
    Objects.requireNonNull(newMetadata, "old metadata must be not null for UPDATED event");
    Objects.requireNonNull(newMetadata, "new metadata must be not null for UPDATED event");
    return new MembershipEvent(Type.UPDATED, member, oldMetadata, newMetadata);
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
    return member;
  }

  public Map<String, String> oldMetadata() {
    return oldMetadata;
  }

  public Map<String, String> newMetadata() {
    return newMetadata;
  }

  @Override
  public String toString() {
    return "MembershipEvent{type="
        + type
        + ", member="
        + member
        + ", newMetadata="
        + metadataAsString(newMetadata)
        + ", oldMetadata="
        + metadataAsString(oldMetadata)
        + '}';
  }

  private String metadataAsString(Map<String, String> metadata) {
    if (metadata == null) {
      return null;
    }
    if (metadata.isEmpty()) {
      return "[]";
    }
    return Integer.toHexString(metadata.hashCode() & Integer.MAX_VALUE) + "-" + metadata.size();
  }
}
