package io.scalecube.cluster2.fdetector;

import static io.scalecube.cluster2.UUIDCodec.encode;

import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.sbe.AckType;
import io.scalecube.cluster2.sbe.MessageHeaderEncoder;
import io.scalecube.cluster2.sbe.PingAckEncoder;
import io.scalecube.cluster2.sbe.PingEncoder;
import io.scalecube.cluster2.sbe.PingRequestEncoder;
import java.util.UUID;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;

public class FailureDetectorCodec {

  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final PingEncoder pingEncoder = new PingEncoder();
  private final PingRequestEncoder pingRequestEncoder = new PingRequestEncoder();
  private final PingAckEncoder pingAckEncoder = new PingAckEncoder();
  private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
  private final MemberCodec memberCodec = new MemberCodec();
  private int encodedLength;

  public FailureDetectorCodec() {}

  public DirectBuffer encodePing(UUID cid, Member localMember, Member pingMember) {
    encodedLength = 0;

    pingEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    encode(cid, pingEncoder.cid());

    pingEncoder.putFrom(memberCodec.encode(localMember), 0, memberCodec.encodedLength());
    pingEncoder.putTo(memberCodec.encode(pingMember), 0, memberCodec.encodedLength());
    pingEncoder.putOriginalIssuer(memberCodec.encodeNull(), 0, memberCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + pingEncoder.encodedLength();
    return buffer;
  }

  public DirectBuffer encodePingRequest(UUID cid, Member localMember, Member pingMember) {
    encodedLength = 0;

    pingRequestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    encode(cid, pingRequestEncoder.cid());

    pingRequestEncoder.putFrom(memberCodec.encode(localMember), 0, memberCodec.encodedLength());
    pingRequestEncoder.putTo(memberCodec.encode(pingMember), 0, memberCodec.encodedLength());
    pingRequestEncoder.putOriginalIssuer(memberCodec.encodeNull(), 0, memberCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + pingRequestEncoder.encodedLength();
    return buffer;
  }

  public DirectBuffer encodeTransitPing(
      UUID cid, Member localMember, Member target, Member originalIssuer) {
    encodedLength = 0;

    pingEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    encode(cid, pingEncoder.cid());

    pingEncoder.putFrom(memberCodec.encode(localMember), 0, memberCodec.encodedLength());
    pingEncoder.putTo(memberCodec.encode(target), 0, memberCodec.encodedLength());
    pingEncoder.putOriginalIssuer(
        memberCodec.encode(originalIssuer), 0, memberCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + pingEncoder.encodedLength();
    return buffer;
  }

  public DirectBuffer encodePingAck(
      UUID cid, AckType ackType, Member from, Member to, Member originalIssuer) {
    encodedLength = 0;

    pingAckEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    encode(cid, pingAckEncoder.cid());
    pingAckEncoder.ackType(ackType);

    pingAckEncoder.putFrom(memberCodec.encode(from), 0, memberCodec.encodedLength());
    pingAckEncoder.putTo(memberCodec.encode(to), 0, memberCodec.encodedLength());
    pingAckEncoder.putOriginalIssuer(
        memberCodec.encode(originalIssuer), 0, memberCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + pingAckEncoder.encodedLength();
    return buffer;
  }

  public int encodedLength() {
    return encodedLength;
  }

  public DirectBuffer buffer() {
    return buffer;
  }
}
