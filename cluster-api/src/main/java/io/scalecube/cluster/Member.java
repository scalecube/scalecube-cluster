package io.scalecube.cluster;

import io.scalecube.net.Address;
import java.util.Objects;
import java.util.UUID;

/**
 * Cluster member which represents node in the cluster and contains its id and address. This class
 * is essentially immutable.
 */
public final class Member {

  private String id;
  private String alias;
  private Address address;

  /** Instantiates empty member for deserialization purpose. */
  Member() {}

  /**
   * Constructor.
   *
   * @param id member id
   * @param alias member alias (optional)
   * @param address member address
   */
  public Member(String id, String alias, Address address) {
    this.id = Objects.requireNonNull(id, "member id");
    this.alias = alias; // optional
    this.address = Objects.requireNonNull(address, "member address");
  }

  public String id() {
    return id;
  }

  public String alias() {
    return alias;
  }

  public Address address() {
    return address;
  }

  public static String generateId() {
    return Long.toHexString(UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE);
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
    return Objects.equals(id, member.id) && Objects.equals(address, member.address);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, address);
  }

  @Override
  public String toString() {
    if (alias == null) {
      return id + "@" + address;
    } else {
      return alias + "/" + id + "@" + address;
    }
  }
}
