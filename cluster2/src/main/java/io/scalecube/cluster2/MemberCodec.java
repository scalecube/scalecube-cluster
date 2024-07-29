package io.scalecube.cluster2;

import static io.scalecube.cluster2.UUIDCodec.encodeUUID;
import static io.scalecube.cluster2.UUIDCodec.uuid;

import io.scalecube.cluster2.sbe.MemberDecoder;
import io.scalecube.cluster2.sbe.MemberEncoder;
import java.util.UUID;
import java.util.function.Consumer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class MemberCodec extends AbstractCodec {

  private final MemberEncoder memberEncoder = new MemberEncoder();
  private final MemberDecoder memberDecoder = new MemberDecoder();

  public MemberCodec() {}

  // Encode

  public MutableDirectBuffer encode(Member member) {
    encodedLength = 0;

    memberEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);

    if (member != null) {
      encodeUUID(member.id(), memberEncoder.id());
      memberEncoder.address(member.address());
    } else {
      encodeUUID(null, memberEncoder.id());
      memberEncoder.address(null);
    }

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
