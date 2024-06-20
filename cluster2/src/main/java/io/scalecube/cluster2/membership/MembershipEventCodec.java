package io.scalecube.cluster2.membership;

import io.scalecube.cluster2.AbstractCodec;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.sbe.MembershipEventEncoder;
import io.scalecube.cluster2.sbe.MembershipEventType;
import org.agrona.MutableDirectBuffer;

public class MembershipEventCodec extends AbstractCodec {

  private final MemberCodec memberCodec = new MemberCodec();
  private final MembershipEventEncoder membershipEventEncoder = new MembershipEventEncoder();

  public MembershipEventCodec() {}

  public MutableDirectBuffer encodeMembershipEvent(
      MembershipEventType type, long timestamp, Member member) {
    encodedLength = 0;

    membershipEventEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    membershipEventEncoder.type(type);
    membershipEventEncoder.timestamp(timestamp);
    membershipEventEncoder.putMember(memberCodec.encode(member), 0, memberCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + membershipEventEncoder.encodedLength();
    return encodedBuffer;
  }
}
