package io.scalecube.cluster2.membership;

import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.sbe.MemberStatus;
import java.util.StringJoiner;

public class MembershipRecord {

  private int incarnation;
  private MemberStatus status;
  private String alias;
  private String namespace;
  private Member member;

  public MembershipRecord() {}

  public int incarnation() {
    return incarnation;
  }

  public MembershipRecord incarnation(int incarnation) {
    this.incarnation = incarnation;
    return this;
  }

  public MemberStatus status() {
    return status;
  }

  public MembershipRecord status(MemberStatus status) {
    this.status = status;
    return this;
  }

  public String alias() {
    return alias;
  }

  public MembershipRecord alias(String alias) {
    this.alias = alias;
    return this;
  }

  public String namespace() {
    return namespace;
  }

  public MembershipRecord namespace(String namespace) {
    this.namespace = namespace;
    return this;
  }

  public Member member() {
    return member;
  }

  public MembershipRecord member(Member member) {
    this.member = member;
    return this;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", MembershipRecord.class.getSimpleName() + "[", "]")
        .add("incarnation=" + incarnation)
        .add("status=" + status)
        .add("alias='" + alias + "'")
        .add("namespace='" + namespace + "'")
        .add("member=" + member)
        .toString();
  }
}
