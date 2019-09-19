package io.scalecube.cluster.metadata;

import io.scalecube.cluster.Member;
import java.nio.ByteBuffer;
import java.util.StringJoiner;

/**
 * DTO class. Stands for response for preceding remote request on getting metadata in remote
 * MetadataStore.
 */
final class GetMetadataResponse {

  /** Target member with metadata. */
  private Member member;

  /** Cluster member metadata. */
  private ByteBuffer metadata;

  /** Instantiates empty GetMetadataResponse for deserialization purpose. */
  GetMetadataResponse() {}

  GetMetadataResponse(Member member, ByteBuffer metadata) {
    this.member = member;
    this.metadata = metadata;
  }

  Member getMember() {
    return member;
  }

  ByteBuffer getMetadata() {
    return metadata;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", GetMetadataResponse.class.getSimpleName() + "[", "]")
        .add("member=" + member)
        .add("metadata=" + metadata.remaining())
        .toString();
  }
}
