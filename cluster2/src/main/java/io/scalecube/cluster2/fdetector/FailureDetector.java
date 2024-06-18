package io.scalecube.cluster2.fdetector;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster2.AbstractAgent;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.sbe.MessageHeaderDecoder;
import io.scalecube.cluster2.sbe.PingAckDecoder;
import io.scalecube.cluster2.sbe.PingDecoder;
import io.scalecube.cluster2.sbe.PingRequestDecoder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;

public class FailureDetector extends AbstractAgent {

  private final Member localMember;

  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final PingDecoder pingDecoder = new PingDecoder();
  private final PingRequestDecoder pingRequestDecoder = new PingRequestDecoder();
  private final PingAckDecoder pingAckDecoder = new PingAckDecoder();
  private final FailureDetectorCodec codec = new FailureDetectorCodec();
  private final MemberCodec memberCodec = new MemberCodec();
  private final List<Member> pingMembers = new ArrayList<>();

  public FailureDetector(
      Transport transport,
      BroadcastTransmitter messageTx,
      Supplier<CopyBroadcastReceiver> messageRxSupplier,
      EpochClock epochClock,
      Duration tickInterval,
      Member localMember) {
    super(transport, messageTx, messageRxSupplier, epochClock, tickInterval);
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

  private Member nextPingMember() {
    return pingMembers.size() > 0 ? pingMembers.get(random.nextInt(pingMembers.size())) : null;
  }

  @Override
  public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length) {
    headerDecoder.wrap(buffer, index);
    final int templateId = headerDecoder.templateId();
    switch (templateId) {
      case PingDecoder.TEMPLATE_ID:
        onPing(pingDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      case PingRequestDecoder.TEMPLATE_ID:
        onPingRequest(pingRequestDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      case PingAckDecoder.TEMPLATE_ID:
        onPingAck(pingAckDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      default:
        // no-op
    }
  }

  private void onPing(PingDecoder decoder) {
    final long cid = decoder.cid();
    final Member from = memberCodec.member(decoder::wrapFrom);
    final Member target = memberCodec.member(decoder::wrapTarget);
    final Member issuer = memberCodec.member(decoder::wrapIssuer);

    if (!localMember.id().equals(target.id())) {
      return;
    }

    transport.send(
        from.address(), codec.encodePingAck(cid, from, target, issuer), 0, codec.encodedLength());
  }

  private void onPingRequest(PingRequestDecoder decoder) {
    final long cid = decoder.cid();
    final Member from = memberCodec.member(decoder::wrapFrom);
    final Member target = memberCodec.member(decoder::wrapTarget);
    decoder.skipIssuer();

    transport.send(
        target.address(),
        codec.encodePing(cid, localMember, target, from),
        0,
        codec.encodedLength());
  }

  private void onPingAck(PingAckDecoder decoder) {
    final long cid = decoder.cid();
    final Member from = memberCodec.member(decoder::wrapFrom);
    final Member target = memberCodec.member(decoder::wrapTarget);
    final Member issuer = memberCodec.member(decoder::wrapIssuer);

    if (issuer != null) {
      transport.send(
          issuer.address(),
          codec.encodePingAck(cid, issuer, target, null),
          0,
          codec.encodedLength());
      return;
    }

    if (!localMember.id().equals(from.id())) {
      return;
    }

    invokeCallback(cid, target);
  }
}
