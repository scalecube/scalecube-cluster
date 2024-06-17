package io.scalecube.cluster2;

import io.scalecube.cluster2.sbe.MemberEncoder;
import io.scalecube.cluster2.sbe.MessageHeaderEncoder;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;

public class MemberCodec {

  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final MemberEncoder memberEncoder = new MemberEncoder();
  private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
  private int encodedLength;

  public MemberCodec() {}

  public DirectBuffer encode(Member member) {
    return encode(
        encoder -> {
          if (member != null) {
            UUIDCodec.encode(member.id(), memberEncoder.id());
            memberEncoder
                .alias(member.alias())
                .address(member.address())
                .namespace(member.namespace());
          } else {
            UUIDCodec.encode(null, memberEncoder.id());
            memberEncoder.alias(null).address(null).namespace(null);
          }
        });
  }

  private DirectBuffer encode(Consumer<MemberEncoder> consumer) {
    encodedLength = 0;
    memberEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    consumer.accept(memberEncoder);
    encodedLength = headerEncoder.encodedLength() + memberEncoder.encodedLength();
    return buffer;
  }

  public int encodedLength() {
    return encodedLength;
  }

  public DirectBuffer buffer() {
    return buffer;
  }
}
