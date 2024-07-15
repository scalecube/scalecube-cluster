package io.scalecube.cluster2.membership;

import io.scalecube.cluster2.AbstractCodec;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.sbe.MemberStatus;
import io.scalecube.cluster2.sbe.MembershipRecordDecoder;
import io.scalecube.cluster2.sbe.MembershipRecordEncoder;
import java.util.function.Consumer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class MembershipRecordCodec extends AbstractCodec {

  private final MemberCodec memberCodec = new MemberCodec();
  private final MembershipRecordEncoder membershipRecordEncoder = new MembershipRecordEncoder();
  private final MembershipRecordDecoder membershipRecordDecoder = new MembershipRecordDecoder();

  public MembershipRecordCodec() {}

  // Encode

  public MutableDirectBuffer encode(MembershipRecord record) {
    encodedLength = 0;

    membershipRecordEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    membershipRecordEncoder.incarnation(record.incarnation());
    membershipRecordEncoder.status(record.status());
    membershipRecordEncoder.alias(record.alias());
    membershipRecordEncoder.namespace(record.namespace());
    membershipRecordEncoder.putMember(
        memberCodec.encode(record.member()), 0, memberCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + membershipRecordEncoder.encodedLength();
    return encodedBuffer;
  }

  // Decode

  public MembershipRecord membershipRecord(Consumer<UnsafeBuffer> consumer) {
    consumer.accept(unsafeBuffer);

    membershipRecordDecoder.wrapAndApplyHeader(unsafeBuffer, 0, headerDecoder);
    final int incarnation = membershipRecordDecoder.incarnation();
    final MemberStatus status = membershipRecordDecoder.status();
    final String alias = membershipRecordDecoder.alias();
    final String ns = membershipRecordDecoder.namespace();
    final Member member = memberCodec.member(membershipRecordDecoder::wrapMember);

    return new MembershipRecord()
        .incarnation(incarnation)
        .status(status)
        .alias(alias)
        .namespace(ns)
        .member(member);
  }
}
