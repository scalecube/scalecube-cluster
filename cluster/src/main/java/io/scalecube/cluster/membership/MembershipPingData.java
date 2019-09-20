package io.scalecube.cluster.membership;

import io.scalecube.cluster.Member;
import java.util.StringJoiner;

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
    return new StringJoiner(", ", MembershipPingData.class.getSimpleName() + "[", "]")
        .add("target=" + target)
        .toString();
  }
}
