package io.scalecube.cluster.metadata;

import io.scalecube.cluster.Member;
import java.util.Map;

/**
 * DTO class. Stands for response for preceding remote request on getting metadata in remote
 * MetadataStore.
 */
final class GetMetadataResponse {

  /** Target member with metadata. */
  private Member member;

  /** Cluster member metadata. */
  private Map<String, String> metadata;

  /** Instantiates empty GetMetadataResponse for deserialization purpose. */
  GetMetadataResponse() {}

  GetMetadataResponse(Member member, Map<String, String> metadata) {
    this.member = member;
    this.metadata = metadata;
  }

  Member getMember() {
    return member;
  }

  Map<String, String> getMetadata() {
    return metadata;
  }

  @Override
  public String toString() {
    return "GetMetadataResponse{" + "member=" + member + ", metadata=" + metadata + '}';
  }
}
