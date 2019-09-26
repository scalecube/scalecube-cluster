package io.scalecube.cluster.membership;

import io.scalecube.cluster.Member;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Event which is emitted on cluster membership changes when new member added, updated in the
 * cluster or removed from the cluster.
 */
public final class MembershipEvent {

  public enum Type {
    ADDED,
    REMOVED,
    LEAVING,
    UPDATED
  }

  private final Type type;
  private final Member member;
  private final ByteBuffer oldMetadata;
  private final ByteBuffer newMetadata;
  private final long timestamp;

  private MembershipEvent(
      Type type, Member member, ByteBuffer oldMetadata, ByteBuffer newMetadata, long timestamp) {
    this.type = type;
    this.member = member;
    this.oldMetadata = oldMetadata;
    this.newMetadata = newMetadata;
    this.timestamp = timestamp;
  }

  /**
   * Creates REMOVED membership event with cluster member and its metadata (optional).
   *
   * @param member cluster member; not null
   * @param metadata member metadata; optional
   * @param timestamp event timestamp
   * @return membership event
   */
  public static MembershipEvent createRemoved(Member member, ByteBuffer metadata, long timestamp) {
    Objects.requireNonNull(member, "member must be not null");
    return new MembershipEvent(Type.REMOVED, member, metadata, null, timestamp);
  }

  /**
   * Creates ADDED membership event with cluster member and its metadata.
   *
   * @param member cluster memeber; not null
   * @param metadata member metadata; not null
   * @param timestamp event timestamp
   * @return membership event
   */
  public static MembershipEvent createAdded(Member member, ByteBuffer metadata, long timestamp) {
    Objects.requireNonNull(member, "member must be not null");
    return new MembershipEvent(Type.ADDED, member, null, metadata, timestamp);
  }

  /**
   * Creates LEAVING membership event.
   *
   * @param member cluster member; not null
   * @param metadata member metadata; not null
   * @param timestamp event timestamp
   * @return membership event
   */
  public static MembershipEvent createLeaving(Member member, ByteBuffer metadata, long timestamp) {
    Objects.requireNonNull(member, "member must be not null");
    return new MembershipEvent(Type.LEAVING, member, null, metadata, timestamp);
  }

  /**
   * Creates UPDATED membership event.
   *
   * @param member cluster member; not null
   * @param oldMetadata previous metadata; not null
   * @param newMetadata new metadata; not null
   * @param timestamp event timestamp
   * @return membership event
   */
  public static MembershipEvent createUpdated(
      Member member, ByteBuffer oldMetadata, ByteBuffer newMetadata, long timestamp) {
    Objects.requireNonNull(member, "member must be not null");
    return new MembershipEvent(Type.UPDATED, member, oldMetadata, newMetadata, timestamp);
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

  public boolean isLeaving() {
    return type == Type.LEAVING;
  }

  public boolean isUpdated() {
    return type == Type.UPDATED;
  }

  public Member member() {
    return member;
  }

  public ByteBuffer oldMetadata() {
    return oldMetadata;
  }

  public ByteBuffer newMetadata() {
    return newMetadata;
  }

  public long timestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", MembershipEvent.class.getSimpleName() + "[", "]")
        .add("type=" + type)
        .add("member=" + member)
        .add("oldMetadata=" + metadataAsString(oldMetadata))
        .add("newMetadata=" + metadataAsString(newMetadata))
        .add("timestamp=" + timestampAsString(timestamp))
        .toString();
  }

  private String timestampAsString(long timestamp) {
    return Instant.ofEpochMilli(timestamp).toString();
  }

  private String metadataAsString(ByteBuffer metadata) {
    if (metadata == null) {
      return null;
    }
    return Integer.toHexString(metadata.hashCode()) + "-" + metadata.remaining();
  }
}
