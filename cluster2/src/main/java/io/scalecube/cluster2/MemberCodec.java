package io.scalecube.cluster2;

import io.scalecube.cluster2.sbe.MemberEncoder;
import io.scalecube.cluster2.sbe.MessageHeaderEncoder;
import java.util.function.Consumer;
import org.agrona.ExpandableArrayBuffer;

public class MemberCodec {

  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final MemberEncoder memberEncoder = new MemberEncoder();
  private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

  public ExpandableArrayBuffer encode(Member member) {
    return encode(
        encoder -> {
          UUIDCodec.encode(member.id(), memberEncoder.id());
          memberEncoder
              .alias(member.alias())
              .address(member.address())
              .namespace(member.namespace());
        });
  }

  public ExpandableArrayBuffer encodeNull() {
    return encode(
        encoder -> {
          UUIDCodec.encode(null, memberEncoder.id());
          memberEncoder.alias(null).address(null).namespace(null);
        });
  }

  public ExpandableArrayBuffer encode(Consumer<MemberEncoder> consumer) {
    memberEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    consumer.accept(memberEncoder);
    return buffer;
  }

  public int encodedLength() {
    return headerEncoder.encodedLength() + memberEncoder.encodedLength();
  }

  public ExpandableArrayBuffer buffer() {
    return buffer;
  }
}
