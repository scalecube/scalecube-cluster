package io.scalecube.cluster2.membership;

import io.scalecube.cluster2.AbstractCodec;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.sbe.MemberStatus;
import io.scalecube.cluster2.sbe.MembershipRecordEncoder;
import org.agrona.MutableDirectBuffer;

public class MembershipRecordCodec extends AbstractCodec {

  private final MemberCodec memberCodec = new MemberCodec();
  private final MembershipRecordEncoder membershipRecordEncoder = new MembershipRecordEncoder();

  public MembershipRecordCodec() {}

  public MutableDirectBuffer encode(MembershipRecord membershipRecord) {
    encodedLength = 0;

    membershipRecordEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    membershipRecordEncoder.incarnation(membershipRecord.incarnation());
    membershipRecordEncoder.status(membershipRecord.status());
    membershipRecordEncoder.alias(membershipRecord.alias());
    membershipRecordEncoder.namespace(membershipRecord.namespace());
    membershipRecordEncoder.putMember(
        memberCodec.encode(membershipRecord.member()), 0, memberCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + membershipRecordEncoder.encodedLength();
    return encodedBuffer;
  }
}
