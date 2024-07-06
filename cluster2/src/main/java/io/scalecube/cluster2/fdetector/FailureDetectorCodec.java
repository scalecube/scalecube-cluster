package io.scalecube.cluster2.fdetector;

import io.scalecube.cluster2.AbstractCodec;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.sbe.FailureDetectorEventEncoder;
import io.scalecube.cluster2.sbe.MemberStatus;
import io.scalecube.cluster2.sbe.PingAckEncoder;
import io.scalecube.cluster2.sbe.PingEncoder;
import io.scalecube.cluster2.sbe.PingRequestEncoder;
import org.agrona.MutableDirectBuffer;

public class FailureDetectorCodec extends AbstractCodec {

  private final PingEncoder pingEncoder = new PingEncoder();
  private final PingRequestEncoder pingRequestEncoder = new PingRequestEncoder();
  private final PingAckEncoder pingAckEncoder = new PingAckEncoder();
  private final FailureDetectorEventEncoder failureDetectorEventEncoder =
      new FailureDetectorEventEncoder();
  private final MemberCodec memberCodec = new MemberCodec();

  public FailureDetectorCodec() {}

  public MutableDirectBuffer encodePing(
      long cid, long period, Member from, Member target, Member issuer) {
    encodedLength = 0;

    pingEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    pingEncoder.cid(cid);
    pingEncoder.period(period);
    pingEncoder.putFrom(memberCodec.encode(from), 0, memberCodec.encodedLength());
    pingEncoder.putTarget(memberCodec.encode(target), 0, memberCodec.encodedLength());
    pingEncoder.putIssuer(memberCodec.encode(issuer), 0, memberCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + pingEncoder.encodedLength();
    return encodedBuffer;
  }

  public MutableDirectBuffer encodePingRequest(long cid, long period, Member from, Member target) {
    encodedLength = 0;

    pingRequestEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    pingRequestEncoder.cid(cid);
    pingRequestEncoder.period(period);
    pingRequestEncoder.putFrom(memberCodec.encode(from), 0, memberCodec.encodedLength());
    pingRequestEncoder.putTarget(memberCodec.encode(target), 0, memberCodec.encodedLength());
    pingRequestEncoder.putIssuer(memberCodec.encode(null), 0, memberCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + pingRequestEncoder.encodedLength();
    return encodedBuffer;
  }

  public MutableDirectBuffer encodePingAck(
      long cid, long period, Member from, Member target, Member issuer) {
    encodedLength = 0;

    pingAckEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    pingAckEncoder.cid(cid);
    pingAckEncoder.period(period);
    pingAckEncoder.putFrom(memberCodec.encode(from), 0, memberCodec.encodedLength());
    pingAckEncoder.putTarget(memberCodec.encode(target), 0, memberCodec.encodedLength());
    pingAckEncoder.putIssuer(memberCodec.encode(issuer), 0, memberCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + pingAckEncoder.encodedLength();
    return encodedBuffer;
  }

  public MutableDirectBuffer encodeFailureDetectorEvent(Member member, MemberStatus status) {
    encodedLength = 0;

    failureDetectorEventEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    failureDetectorEventEncoder.status(status);
    failureDetectorEventEncoder.putMember(
        memberCodec.encode(member), 0, memberCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + failureDetectorEventEncoder.encodedLength();
    return encodedBuffer;
  }
}
