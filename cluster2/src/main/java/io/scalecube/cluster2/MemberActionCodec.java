package io.scalecube.cluster2;

import io.scalecube.cluster2.sbe.MemberActionEncoder;
import io.scalecube.cluster2.sbe.MemberActionType;
import org.agrona.MutableDirectBuffer;

public class MemberActionCodec extends AbstractCodec {

  private final MemberCodec memberCodec = new MemberCodec();
  private final MemberActionEncoder memberActionEncoder = new MemberActionEncoder();

  public MemberActionCodec() {}

  public MutableDirectBuffer encode(MemberActionType actionType, Member member) {
    encodedLength = 0;

    memberActionEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    memberActionEncoder.actionType(actionType);
    memberActionEncoder.putMember(memberCodec.encode(member), 0, memberCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + memberActionEncoder.encodedLength();
    return encodedBuffer;
  }
}
