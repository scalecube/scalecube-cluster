package io.scalecube.cluster.fdetector;

import io.scalecube.cluster.Member;

/** DTO class. Supports FailureDetector messages (Ping, Ack, PingReq). */
final class PingData {

  enum AckType {

    /**
     * Ping completed successfully.
     *
     * <p>Destination member responded normally.
     */
    DEST_OK,

    /**
     * Ping didn't complete normally.
     *
     * <p>Member referenced by field {@link #to} is gone.
     */
    DEST_GONE
  }

  /** Message's source address. */
  private Member from;
  /** Message's destination address. */
  private Member to;
  /** Address of member, who originally initiated ping sequence. */
  private Member originalIssuer;
  /** Ping response type. */
  private AckType ackType;

  /** Instantiates empty ping data for deserialization purpose. */
  PingData() {}

  private PingData(PingData other) {
    this.from = other.from;
    this.to = other.to;
    this.originalIssuer = other.originalIssuer;
    this.ackType = other.ackType;
  }

  public PingData(Member from, Member to) {
    this.from = from;
    this.to = to;
    this.originalIssuer = null;
    this.ackType = null;
  }

  public PingData(Member from, Member to, Member originalIssuer) {
    this.from = from;
    this.to = to;
    this.originalIssuer = originalIssuer;
    this.ackType = null;
  }

  public Member getFrom() {
    return from;
  }

  public Member getTo() {
    return to;
  }

  public Member getOriginalIssuer() {
    return originalIssuer;
  }

  public AckType getAckType() {
    return ackType;
  }

  public PingData withAckType(AckType ackType) {
    PingData p = new PingData(this);
    p.ackType = ackType;
    return p;
  }

  @Override
  public String toString() {
    return "PingData{"
        + "from="
        + from
        + ", to="
        + to
        + ", originalIssuer="
        + originalIssuer
        + ", ackType="
        + ackType
        + '}';
  }
}
