package io.scalecube.cluster.metadata;

import io.scalecube.cluster.Member;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.StringJoiner;

/**
 * DTO class. Stands for response for preceding remote request on getting metadata in remote
 * MetadataStore.
 */
final class GetMetadataResponse implements Externalizable {

  private static final long serialVersionUID = 1L;

  /** Target member with metadata. */
  private Member member;

  /** Cluster member metadata. */
  private ByteBuffer metadata;

  public GetMetadataResponse() {}

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
  public void writeExternal(ObjectOutput out) throws IOException {
    // member
    out.writeObject(member);
    // metadata
    byte[] bytes = metadata.array();
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // member
    member = (Member) in.readObject();
    // metadata
    int size = in.readInt();
    byte[] bytes = new byte[size];
    in.read(bytes);
    metadata = ByteBuffer.wrap(bytes);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", GetMetadataResponse.class.getSimpleName() + "[", "]")
        .add("member=" + member)
        .add("metadata=" + metadata.remaining())
        .toString();
  }
}
