package io.scalecube.cluster;

import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.net.Address;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.UUID;

/**
 * Cluster member which represents node in the cluster and contains its id and address. This class
 * is essentially immutable.
 */
public final class Member implements Externalizable {

  private static final long serialVersionUID = 1L;

  private String id;
  private String alias;
  private List<Address> addresses;
  private String namespace;

  public Member() {}

  /**
   * Constructor.
   *
   * @param id member id
   * @param alias member alias (optional)
   * @param addresses member addresses
   * @param namespace namespace
   */
  public Member(String id, String alias, List<Address> addresses, String namespace) {
    this.id = Objects.requireNonNull(id, "id");
    this.alias = alias;
    this.addresses = Objects.requireNonNull(addresses, "addresses");
    this.namespace = Objects.requireNonNull(namespace, "namespace");
  }

  /**
   * Returns cluster member local id.
   *
   * @return member id
   */
  public String id() {
    return id;
  }

  /**
   * Returns cluster member alias if exists, otherwise {@code null}.
   *
   * @see ClusterConfig#memberAlias(String)
   * @return alias if exists or {@code null}
   */
  public String alias() {
    return alias;
  }

  /**
   * Returns cluster member namespace.
   *
   * @see MembershipConfig#namespace(String)
   * @return namespace
   */
  public String namespace() {
    return namespace;
  }

  /**
   * Returns cluster member addresses, those are addresses on which this cluster member listens
   * connections from other cluster members.
   *
   * @see io.scalecube.cluster.transport.api.TransportConfig#port(int)
   * @return member address
   */
  public List<Address> addresses() {
    return addresses;
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
        && Objects.equals(addresses, member.addresses)
        && Objects.equals(namespace, member.namespace);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, addresses, namespace);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // id
    out.writeUTF(id);
    // alias
    boolean aliasNotNull = alias != null;
    out.writeBoolean(aliasNotNull);
    if (aliasNotNull) {
      out.writeUTF(alias);
    }
    // address
    out.writeInt(addresses.size());
    for (Address address : addresses) {
      out.writeUTF(address.toString());
    }
    // namespace
    out.writeUTF(namespace);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    // id
    id = in.readUTF();
    // alias
    boolean aliasNotNull = in.readBoolean();
    if (aliasNotNull) {
      alias = in.readUTF();
    }
    // addresses
    final int addressesSize = in.readInt();
    addresses = new ArrayList<>(addressesSize);
    for (int i = 0; i < addressesSize; i++) {
      addresses.add(Address.from(in.readUTF()));
    }
    // namespace
    this.namespace = in.readUTF();
  }

  private static String stringifyId(String id) {
    try {
      final UUID uuid = UUID.fromString(id);
      return Long.toHexString(uuid.getMostSignificantBits() & Long.MAX_VALUE);
    } catch (Exception ex) {
      return id;
    }
  }

  @Override
  public String toString() {
    StringJoiner stringJoiner = new StringJoiner(":");
    if (alias == null) {
      return stringJoiner.add(namespace).add(stringifyId(id)).toString();
    } else {
      return stringJoiner.add(namespace).add(alias).add(stringifyId(id)).toString();
    }
  }
}
