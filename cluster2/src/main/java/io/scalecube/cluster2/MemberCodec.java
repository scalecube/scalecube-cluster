package io.scalecube.cluster2;

import static io.scalecube.cluster2.UUIDCodec.uuid;

import io.scalecube.cluster2.sbe.MemberDecoder;
import io.scalecube.cluster2.sbe.MemberEncoder;
import java.util.UUID;
import java.util.function.Consumer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class MemberCodec extends AbstractCodec {

  private final MemberDecoder memberDecoder = new MemberDecoder();
  private final MemberEncoder memberEncoder = new MemberEncoder();

  public MemberCodec() {}

  // Encode

  public MutableDirectBuffer encode(Member member) {
    return encode(
        encoder -> {
          if (member != null) {
            UUIDCodec.encode(member.id(), memberEncoder.id());
            memberEncoder.address(member.address());
          } else {
            UUIDCodec.encode(null, memberEncoder.id());
            memberEncoder.address(null);
          }
        });
  }

  private MutableDirectBuffer encode(Consumer<MemberEncoder> consumer) {
    encodedLength = 0;
    memberEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    consumer.accept(memberEncoder);
    encodedLength = headerEncoder.encodedLength() + memberEncoder.encodedLength();
    return encodedBuffer;
  }

  // Decode

  public Member member(Consumer<UnsafeBuffer> consumer) {
    consumer.accept(unsafeBuffer);

    memberDecoder.wrapAndApplyHeader(unsafeBuffer, 0, headerDecoder);

    final UUID id = uuid(memberDecoder.id());
    if (id == null) {
      return null;
    }

    final String address = memberDecoder.address();

    return new Member(id, address);
  }
}
