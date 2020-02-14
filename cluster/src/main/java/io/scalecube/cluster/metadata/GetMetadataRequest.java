package io.scalecube.cluster.metadata;

import io.scalecube.cluster.Member;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.StringJoiner;

/** DTO class. Stands for remote request on getting metadata in remote MetadataStore. */
final class GetMetadataRequest implements Externalizable {

  private static final long serialVersionUID = 1L;

  /** Target member. */
  private Member member;

  public GetMetadataRequest() {}

  GetMetadataRequest(Member member) {
    this.member = Objects.requireNonNull(member);
  }

  public Member getMember() {
    return member;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // member
    out.writeObject(member);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // member
    member = (Member) in.readObject();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", GetMetadataRequest.class.getSimpleName() + "[", "]")
        .add("member=" + member)
        .toString();
  }
}
