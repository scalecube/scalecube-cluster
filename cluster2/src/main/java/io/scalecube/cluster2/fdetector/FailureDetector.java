package io.scalecube.cluster2.fdetector;

import static io.scalecube.cluster2.ShuffleUtil.shuffle;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster2.AbstractAgent;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.sbe.MemberActionDecoder;
import io.scalecube.cluster2.sbe.MemberActionType;
import io.scalecube.cluster2.sbe.MemberStatus;
import io.scalecube.cluster2.sbe.MessageHeaderDecoder;
import io.scalecube.cluster2.sbe.PingAckDecoder;
import io.scalecube.cluster2.sbe.PingDecoder;
import io.scalecube.cluster2.sbe.PingRequestDecoder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;
import java.util.function.Supplier;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ArrayListUtil;
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
  private final MemberActionDecoder memberActionDecoder = new MemberActionDecoder();
  private final MemberCodec memberCodec = new MemberCodec();
  private final String roleName;
  private final MemberSelector memberSelector;
  private final ArrayList<Member> pingMembers = new ArrayList<>();
  private final ArrayList<Member> pingReqMembers = new ArrayList<>();
  private long period = 0;
  private Member pingMember;
  private MemberStatus memberStatus;

  public FailureDetector(
      Transport transport,
      BroadcastTransmitter messageTx,
      Supplier<CopyBroadcastReceiver> messageRxSupplier,
      EpochClock epochClock,
      FailureDetectorConfig config,
      Member localMember) {
    super(
        transport,
        messageTx,
        messageRxSupplier,
        epochClock,
        Duration.ofMillis(config.pingInterval()));
    this.localMember = localMember;
    roleName = "fdetector@" + localMember.address();
    memberSelector = new MemberSelector(config.pingReqMembers(), pingMembers, pingReqMembers);
  }

  public long period() {
    return period;
  }

  @Override
  public String roleName() {
    return roleName;
  }

  @Override
  protected void onTick() {
    period++;

    // Conclude prev. period

    if (pingMember != null && memberStatus == null) {
      emitMemberStatus(pingMember, MemberStatus.SUSPECTED);
    }

    // Init current period

    memberStatus = null;
    pingMember = memberSelector.nextPingMember();
    if (pingMember == null) {
      return;
    }

    // Do ping

    transport.send(
        pingMember.address(),
        codec.encodePing(period, localMember, pingMember, null),
        0,
        codec.encodedLength());

    // Do ping request

    memberSelector.nextPingReqMembers(pingMember);

    doPingRequest(pingMember);
  }

  private void emitMemberStatus(Member target, final MemberStatus memberStatus) {
    messageTx.transmit(
        1, codec.encodeFailureDetectorEvent(target, memberStatus), 0, codec.encodedLength());
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
      case MemberActionDecoder.TEMPLATE_ID:
        onMemberAction(memberActionDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
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

    // Transit PingAck

    if (issuer != null) {
      transport.send(
          issuer.address(),
          codec.encodePingAck(period, issuer, target, null),
          0,
          codec.encodedLength());
      return;
    }

    // Normal PingAck

    if (this.period != period) {
      return;
    }

    if (!localMember.equals(from)) {
      return;
    }

    if (memberStatus == null) {
      memberStatus = MemberStatus.ALIVE;
      emitMemberStatus(target, memberStatus);
    }
  }

  private void onMemberAction(MemberActionDecoder decoder) {
    final MemberActionType actionType = decoder.actionType();
    final Member member = memberCodec.member(decoder::wrapMember);
    decoder.sbeSkip();

    if (localMember.equals(member)) {
      return;
    }

    switch (actionType) {
      case REMOVE_MEMBER:
        pingMembers.remove(member);
        break;
      case ADD_MEMBER:
        if (!pingMembers.contains(member)) {
          pingMembers.add(member);
        }
        break;
      default:
        // no-op
    }
  }

  static class MemberSelector {

    private final int pingReqMembersNum;
    private final ArrayList<Member> pingMembers;
    private final ArrayList<Member> pingReqMembers;

    private final Random random = new Random();
    private int index;

    MemberSelector(
        int pingReqMembersNum, ArrayList<Member> pingMembers, ArrayList<Member> pingReqMembers) {
      this.pingReqMembersNum = pingReqMembersNum;
      this.pingMembers = pingMembers;
      this.pingReqMembers = pingReqMembers;
    }

    Member nextPingMember() {
      final int size = pingMembers.size();
      if (size == 0) {
        return null;
      }

      final int i;
      if (index >= size) {
        i = index = 0;
        shuffle(pingMembers, random);
      } else {
        i = index++;
      }

      return pingMembers.get(i);
    }

    void nextPingReqMembers(Member pingMember) {
      pingReqMembers.clear();

      final int demand = pingReqMembersNum;
      final int size = pingMembers.size();
      if (size <= 1) {
        return;
      }

      for (int i = 0, limit = demand < size ? demand : size - 1; i < limit; ) {
        final Member member = pingMembers.get(random.nextInt(size));
        if (!pingMember.equals(member) && !pingReqMembers.contains(member)) {
          pingReqMembers.add(member);
          i++;
        }
      }
    }
  }
}
