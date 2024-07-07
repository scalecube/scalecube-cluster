package io.scalecube.cluster2;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.UUID;

public class Member {

  private UUID id;
  private String address;

  public Member() {}

  public Member(UUID id, String address) {
    this.id = id;
    this.address = address;
  }

  public UUID id() {
    return id;
  }

  public Member id(UUID id) {
    this.id = id;
    return this;
  }

  public String address() {
    return address;
  }

  public Member address(String address) {
    this.address = address;
    return this;
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that == null || getClass() != that.getClass()) {
      return false;
    }
    Member member = (Member) that;
    return Objects.equals(id, member.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Member.class.getSimpleName() + "[", "]")
        .add("id=" + id)
        .add("address='" + address + "'")
        .toString();
  }
}
