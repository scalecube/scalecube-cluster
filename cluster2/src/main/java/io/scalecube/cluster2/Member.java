package io.scalecube.cluster2;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.UUID;

public class Member {

  private final UUID id;
  private final String alias;
  private final String address;
  private final String namespace;

  public Member(UUID id, String alias, String address, String namespace) {
    this.id = Objects.requireNonNull(id, "member id");
    this.alias = alias; // optional
    this.address = Objects.requireNonNull(address, "member address");
    this.namespace = Objects.requireNonNull(namespace, "member namespace");
  }

  public UUID id() {
    return id;
  }

  public String alias() {
    return alias;
  }

  public String address() {
    return address;
  }

  public String namespace() {
    return namespace;
  }

  private static String stringifyId(UUID id) {
    return Long.toHexString(id.getMostSignificantBits() & Long.MAX_VALUE);
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
    return Objects.equals(id, member.id)
        && Objects.equals(address, member.address)
        && Objects.equals(namespace, member.namespace);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, address, namespace);
  }

  @Override
  public String toString() {
    StringJoiner stringJoiner = new StringJoiner(":");
    if (alias == null) {
      return stringJoiner.add(namespace).add(stringifyId(id) + "@" + address).toString();
    } else {
      return stringJoiner.add(namespace).add(alias).add(stringifyId(id) + "@" + address).toString();
    }
  }
}
