package io.scalecube.cluster.fdetector;

import io.scalecube.cluster.Member;

/** DTO class. Supports FailureDetector messages (Ping, Ack, PingReq). */
final class PingData {
  /** Message's source address. */
  private Member from;
  /** Message's destination address. */
  private Member to;
  /** Address of member, who originally initiated ping sequence. */
  private Member originalIssuer;

  /** Instantiates empty ping data for deserialization purpose. */
  PingData() {}

  public PingData(Member from, Member to) {
    this.from = from;
    this.to = to;
    this.originalIssuer = null;
  }

  public PingData(Member from, Member to, Member originalIssuer) {
    this.from = from;
    this.to = to;
    this.originalIssuer = originalIssuer;
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

  @Override
  public String toString() {
    return "PingData{from="
        + from
        + ", to="
        + to
        + (originalIssuer != null ? ", originalIssuer=" + originalIssuer : "")
        + '}';
  }
}
