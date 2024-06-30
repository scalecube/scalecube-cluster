package io.scalecube.cluster2;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.UUID;

public class Member {

  private final UUID id;
  private final String address;

  public Member(UUID id, String address) {
    this.id = id;
    this.address = address;
  }

  public UUID id() {
    return id;
  }

  public String address() {
    return address;
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
