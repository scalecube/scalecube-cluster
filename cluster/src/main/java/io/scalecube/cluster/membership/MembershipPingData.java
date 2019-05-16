package io.scalecube.cluster.membership;

import io.scalecube.cluster.Member;

final class MembershipPingData {
  /** Message's destination address. */
  private Member target;

  /** Instantiates empty ping data for deserialization purpose. */
  MembershipPingData() {}

  public MembershipPingData(Member target) {
    this.target = target;
  }

  public Member getTarget() {
    return target;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("MembershipPingData{");
    sb.append("target=").append(target);
    sb.append('}');
    return sb.toString();
  }
}
