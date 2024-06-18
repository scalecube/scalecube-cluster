package io.scalecube.cluster2.fdetector;

import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.sbe.FailureDetectorEventEncoder;
import io.scalecube.cluster2.sbe.MemberStatus;
import io.scalecube.cluster2.sbe.MessageHeaderEncoder;
import io.scalecube.cluster2.sbe.PingAckEncoder;
import io.scalecube.cluster2.sbe.PingEncoder;
import io.scalecube.cluster2.sbe.PingRequestEncoder;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;

public class FailureDetectorCodec {

  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final PingEncoder pingEncoder = new PingEncoder();
  private final PingRequestEncoder pingRequestEncoder = new PingRequestEncoder();
  private final PingAckEncoder pingAckEncoder = new PingAckEncoder();
  private final FailureDetectorEventEncoder failureDetectorEventEncoder =
      new FailureDetectorEventEncoder();
  private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
  private final MemberCodec memberCodec = new MemberCodec();
  private int encodedLength;

  public FailureDetectorCodec() {}

  public DirectBuffer encodePing(long cid, Member localMember, Member pingMember) {
    encodedLength = 0;

    pingEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    pingEncoder.cid(cid);
    pingEncoder.putFrom(memberCodec.encode(localMember), 0, memberCodec.encodedLength());
    pingEncoder.putTo(memberCodec.encode(pingMember), 0, memberCodec.encodedLength());
    pingEncoder.putOriginalIssuer(memberCodec.encode(null), 0, memberCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + pingEncoder.encodedLength();
    return buffer;
  }

  public DirectBuffer encodePingRequest(long cid, Member localMember, Member pingMember) {
    encodedLength = 0;

    pingRequestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    pingRequestEncoder.cid(cid);
    pingRequestEncoder.putFrom(memberCodec.encode(localMember), 0, memberCodec.encodedLength());
    pingRequestEncoder.putTo(memberCodec.encode(pingMember), 0, memberCodec.encodedLength());
    pingRequestEncoder.putOriginalIssuer(memberCodec.encode(null), 0, memberCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + pingRequestEncoder.encodedLength();
    return buffer;
  }

  public DirectBuffer encodeTransitPing(
      long cid, Member localMember, Member target, Member originalIssuer) {
    encodedLength = 0;

    pingEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    pingEncoder.cid(cid);
    pingEncoder.putFrom(memberCodec.encode(localMember), 0, memberCodec.encodedLength());
    pingEncoder.putTo(memberCodec.encode(target), 0, memberCodec.encodedLength());
    pingEncoder.putOriginalIssuer(
        memberCodec.encode(originalIssuer), 0, memberCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + pingEncoder.encodedLength();
    return buffer;
  }

  public DirectBuffer encodePingAck(long cid, Member from, Member to, Member originalIssuer) {
    encodedLength = 0;

    pingAckEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    pingAckEncoder.cid(cid);
    pingAckEncoder.putFrom(memberCodec.encode(from), 0, memberCodec.encodedLength());
    pingAckEncoder.putTo(memberCodec.encode(to), 0, memberCodec.encodedLength());
    pingAckEncoder.putOriginalIssuer(
        memberCodec.encode(originalIssuer), 0, memberCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + pingAckEncoder.encodedLength();
    return buffer;
  }

  public DirectBuffer encodeFailureDetectorEvent(Member member, MemberStatus status) {
    encodedLength = 0;

    failureDetectorEventEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    failureDetectorEventEncoder.status(status);
    failureDetectorEventEncoder.putMember(
        memberCodec.encode(member), 0, memberCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + failureDetectorEventEncoder.encodedLength();
    return buffer;
  }

  public int encodedLength() {
    return encodedLength;
  }

  public DirectBuffer buffer() {
    return buffer;
  }
}
