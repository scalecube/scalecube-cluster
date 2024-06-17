package io.scalecube.cluster2.fdetector;

import static io.scalecube.cluster2.UUIDCodec.encode;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster2.AbstractAgent;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.sbe.AckType;
import io.scalecube.cluster2.sbe.MessageHeaderEncoder;
import io.scalecube.cluster2.sbe.PingEncoder;
import io.scalecube.cluster2.sbe.PingRequestEncoder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.EpochClock;

public class FailureDetector extends AbstractAgent {

  private final Member localMember;

  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final PingEncoder pingEncoder = new PingEncoder();
  private final PingRequestEncoder pingRequestEncoder = new PingRequestEncoder();
  private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
  private final MemberCodec memberCodec = new MemberCodec();
  private final List<Member> pingMembers = new ArrayList<>();

  public FailureDetector(
      Member localMember,
      EpochClock epochClock,
      Transport transport,
      AtomicBuffer messageBuffer,
      Duration tickInterval) {
    super(transport, messageBuffer, epochClock, tickInterval);
    this.localMember = localMember;
  }

  @Override
  public String roleName() {
    return null;
  }

  @Override
  protected void onTick() {
    Member pingMember = nextPingMember();
    if (pingMember == null) {
      return;
    }
  }

  private UUID encodePing(Member pingMember) {
    final UUID cid = UUID.randomUUID();

    pingEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    encode(cid, pingEncoder.cid());
    pingEncoder.sender(localMember.address());
    pingEncoder.ackType(AckType.NULL_VAL);

    pingEncoder.putFrom(memberCodec.encode(localMember), 0, memberCodec.encodedLength());
    pingEncoder.putTo(memberCodec.encode(pingMember), 0, memberCodec.encodedLength());
    pingEncoder.putOriginalIssuer(memberCodec.encodeNull(), 0, memberCodec.encodedLength());

    return cid;
  }

  private UUID encodePingRequest(Member pingMember) {
    final UUID cid = UUID.randomUUID();

    pingRequestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    encode(cid, pingRequestEncoder.cid());
    pingRequestEncoder.sender(localMember.address());
    pingRequestEncoder.ackType(AckType.NULL_VAL);

    pingRequestEncoder.putFrom(memberCodec.encode(localMember), 0, memberCodec.encodedLength());
    pingRequestEncoder.putTo(memberCodec.encode(pingMember), 0, memberCodec.encodedLength());
    pingRequestEncoder.putOriginalIssuer(memberCodec.encodeNull(), 0, memberCodec.encodedLength());

    return cid;
  }

  private Member nextPingMember() {
    return pingMembers.size() > 0 ? pingMembers.get(random.nextInt(pingMembers.size())) : null;
  }

  @Override
  public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length) {}
}
