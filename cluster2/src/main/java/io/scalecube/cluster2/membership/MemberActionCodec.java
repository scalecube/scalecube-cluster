package io.scalecube.cluster2.membership;

import io.scalecube.cluster2.AbstractCodec;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.sbe.AddMemberEncoder;
import io.scalecube.cluster2.sbe.RemoveMemberEncoder;
import org.agrona.MutableDirectBuffer;

public class MemberActionCodec extends AbstractCodec {

  private final MemberCodec memberCodec = new MemberCodec();
  private final AddMemberEncoder addMemberEncoder = new AddMemberEncoder();
  private final RemoveMemberEncoder removeMemberEncoder = new RemoveMemberEncoder();

  public MemberActionCodec() {}

  public MutableDirectBuffer encodeAddMember(Member member) {
    encodedLength = 0;

    addMemberEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    addMemberEncoder.putMember(memberCodec.encode(member), 0, memberCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + addMemberEncoder.encodedLength();
    return encodedBuffer;
  }

  public MutableDirectBuffer encodeRemoveMember(Member member) {
    encodedLength = 0;

    removeMemberEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    removeMemberEncoder.putMember(memberCodec.encode(member), 0, memberCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + removeMemberEncoder.encodedLength();
    return encodedBuffer;
  }
}
