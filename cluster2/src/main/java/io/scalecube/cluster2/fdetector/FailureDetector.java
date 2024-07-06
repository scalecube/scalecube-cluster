package io.scalecube.cluster2.fdetector;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster2.AbstractAgent;
import io.scalecube.cluster2.CallbackInvoker;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.sbe.MemberStatus;
import io.scalecube.cluster2.sbe.MembershipEventDecoder;
import io.scalecube.cluster2.sbe.MembershipEventType;
import io.scalecube.cluster2.sbe.MessageHeaderDecoder;
import io.scalecube.cluster2.sbe.PingAckDecoder;
import io.scalecube.cluster2.sbe.PingDecoder;
import io.scalecube.cluster2.sbe.PingRequestDecoder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.function.Supplier;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ArrayListUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;

public class FailureDetector extends AbstractAgent {

  private final FailureDetectorConfig config;
  private final Member localMember;

  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final PingDecoder pingDecoder = new PingDecoder();
  private final PingRequestDecoder pingRequestDecoder = new PingRequestDecoder();
  private final PingAckDecoder pingAckDecoder = new PingAckDecoder();
  private final MembershipEventDecoder membershipEventDecoder = new MembershipEventDecoder();
  private final FailureDetectorCodec codec = new FailureDetectorCodec();
  private final MemberCodec memberCodec = new MemberCodec();
  private final String roleName;
  private long period;
  private final ArrayList<Member> pingMembers = new ArrayList<>();
  private final ArrayList<Member> pingReqMembers = new ArrayList<>();

  public FailureDetector(
      Transport transport,
      BroadcastTransmitter messageTx,
      Supplier<CopyBroadcastReceiver> messageRxSupplier,
      EpochClock epochClock,
      CallbackInvoker callbackInvoker,
      FailureDetectorConfig config,
      Member localMember) {
    super(
        transport,
        messageTx,
        messageRxSupplier,
        epochClock,
        callbackInvoker,
        Duration.ofMillis(config.pingInterval()));
    this.localMember = localMember;
    this.config = config;
    roleName = "fdetector@" + localMember.address();
  }

  @Override
  public String roleName() {
    return roleName;
  }

  public long period() {
    return period;
  }

  @Override
  protected void onTick() {
    final Member pingMember = nextPingMember();
    if (pingMember == null) {
      return;
    }

    final long period = ++this.period;

    transport.send(
        pingMember.address(),
        codec.encodePing(period, localMember, pingMember, null),
        0,
        codec.encodedLength());

    callbackInvoker.addCallback(
        period,
        config.pingTimeout(),
        (Member target) -> {
          if (target != null) {
            emitMemberStatus(target, MemberStatus.ALIVE);
          } else {
            nextPingReqMembers(pingMember);
            if (pingReqMembers.isEmpty()) {
              emitMemberStatus(pingMember, MemberStatus.SUSPECT);
            } else {
              doPingRequest(pingMember);
            }
          }
        });
  }

  private void emitMemberStatus(Member target, final MemberStatus memberStatus) {
    messageTx.transmit(
        1, codec.encodeFailureDetectorEvent(target, memberStatus), 0, codec.encodedLength());
  }

  private Member nextPingMember() {
    return pingMembers.size() > 0 ? pingMembers.get(random.nextInt(pingMembers.size())) : null;
  }

  private void nextPingReqMembers(Member pingMember) {
    pingReqMembers.clear();

    final int demand = config.pingReqMembers();
    final int size = pingMembers.size();
    if (demand == 0 || size <= 1) {
      return;
    }

    for (int i = 0, limit = demand < size ? demand : size - 1; i < limit; ) {
      final Member member = nextPingMember();
      if (!pingMember.equals(member) && !pingReqMembers.contains(member)) {
        pingReqMembers.add(member);
        i++;
      }
    }
  }

  private void doPingRequest(Member pingMember) {
    for (int n = pingReqMembers.size(), i = n - 1; i >= 0; i--) {
      final Member member = pingReqMembers.get(i);
      ArrayListUtil.fastUnorderedRemove(pingReqMembers, i);

      transport.send(
          member.address(),
          codec.encodePingRequest(period, localMember, pingMember),
          0,
          codec.encodedLength());

      callbackInvoker.addCallback(
          period,
          config.pingTimeout(),
          (Member target) -> {
            if (target != null) {
              emitMemberStatus(target, MemberStatus.ALIVE);
            } else {
              emitMemberStatus(pingMember, MemberStatus.SUSPECT);
            }
          });
    }
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
      case MembershipEventDecoder.TEMPLATE_ID:
        onMembershipEvent(membershipEventDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      default:
        // no-op
    }
  }

  private void onPing(PingDecoder decoder) {
    final long period = decoder.period();
    final Member from = memberCodec.member(decoder::wrapFrom);
    final Member target = memberCodec.member(decoder::wrapTarget);
    final Member issuer = memberCodec.member(decoder::wrapIssuer);

    if (!localMember.equals(target)) {
      return;
    }

    transport.send(
        from.address(),
        codec.encodePingAck(period, from, target, issuer),
        0,
        codec.encodedLength());
  }

  private void onPingRequest(PingRequestDecoder decoder) {
    final long period = decoder.period();
    final Member from = memberCodec.member(decoder::wrapFrom);
    final Member target = memberCodec.member(decoder::wrapTarget);
    decoder.skipIssuer();

    transport.send(
        target.address(),
        codec.encodePing(period, localMember, target, from),
        0,
        codec.encodedLength());
  }

  private void onPingAck(PingAckDecoder decoder) {
    final long period = decoder.period();
    final Member from = memberCodec.member(decoder::wrapFrom);
    final Member target = memberCodec.member(decoder::wrapTarget);
    final Member issuer = memberCodec.member(decoder::wrapIssuer);

    if (issuer != null) {
      transport.send(
          issuer.address(),
          codec.encodePingAck(period, issuer, target, null),
          0,
          codec.encodedLength());
      return;
    }

    if (localMember.equals(from)) {
      callbackInvoker.invokeCallback(period, target);
    }
  }

  private void onMembershipEvent(MembershipEventDecoder decoder) {
    final MembershipEventType type = decoder.type();
    final Member member = memberCodec.member(decoder::wrapMember);
    decoder.sbeSkip();

    if (localMember.equals(member)) {
      return;
    }

    switch (type) {
      case REMOVED:
      case LEAVING:
        pingMembers.remove(member);
        break;
      case ADDED:
        if (!pingMembers.contains(member)) {
          pingMembers.add(member);
        }
        break;
      default:
        // no-op
    }
  }
}
