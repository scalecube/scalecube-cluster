package io.scalecube.cluster2;

import io.scalecube.cluster2.sbe.MemberDecoder;
import io.scalecube.cluster2.sbe.MemberEncoder;
import java.util.UUID;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class MemberCodec extends AbstractCodec {

  private final MemberDecoder memberDecoder = new MemberDecoder();
  private final MemberEncoder memberEncoder = new MemberEncoder();

  public MemberCodec() {}

  // Encode

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
    memberEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    consumer.accept(memberEncoder);
    encodedLength = headerEncoder.encodedLength() + memberEncoder.encodedLength();
    return encodedBuffer;
  }

  // Decode

  public Member member(Consumer<UnsafeBuffer> consumer) {
    consumer.accept(unsafeBuffer);
    memberDecoder.wrapAndApplyHeader(unsafeBuffer, 0, headerDecoder);

    final UUID id = UUIDCodec.uuid(memberDecoder.id());
    if (id == null) {
      return null;
    }

    final String alias = memberDecoder.alias();
    final String address = memberDecoder.address();
    final String namespace = memberDecoder.namespace();

    return new Member(id, alias, address, namespace);
  }
}
