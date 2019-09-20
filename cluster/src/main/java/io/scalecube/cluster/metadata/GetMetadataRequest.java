package io.scalecube.cluster.metadata;

import io.scalecube.cluster.Member;
import java.util.Objects;
import java.util.StringJoiner;

/** DTO class. Stands for remote request on getting metadata in remote MetadataStore. */
final class GetMetadataRequest {

  /** Target member. */
  private Member member;

  /** Instantiates empty GetMetadataRequest for deserialization purpose. */
  GetMetadataRequest() {}

  GetMetadataRequest(Member member) {
    this.member = Objects.requireNonNull(member);
  }

  public Member getMember() {
    return member;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", GetMetadataRequest.class.getSimpleName() + "[", "]")
        .add("member=" + member)
        .toString();
  }
}
